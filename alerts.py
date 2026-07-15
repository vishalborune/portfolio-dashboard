"""
alerts.py — the headless alert engine (Sprint 2).

Runs on GitHub Actions. Four modes:

  python alerts.py states     # flowchart state-change alerts -> Telegram
                              # (every 30 min during market hours)
  python alerts.py filings    # NSE/BSE corporate announcements -> Telegram
                              # (every few hours; best-effort, exchanges are
                              #  bot-hostile — degrades gracefully)
  python alerts.py calendar   # this week's results dates -> Telegram (Mondays)
  python alerts.py digest     # Sunday summary -> email

Shares signals.py with the dashboard: ONE flowchart engine, two consumers.

Env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY, TELEGRAM_BOT_TOKEN,
TELEGRAM_CHAT_ID, and for digest: RESEND_API_KEY, DIGEST_EMAILS.
"""

import os
import re
import sys
import hashlib
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from supabase import create_client

import signals
from notify import send_telegram, send_email

# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------

def sb():
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


def get_holdings(client) -> pd.DataFrame:
    res = client.table("holdings").select("*").execute()
    df = pd.DataFrame(res.data or [])
    if not df.empty and "portfolio_id" not in df.columns:
        df["portfolio_id"] = 1
    return df


# Portfolio -> owner group -> Telegram chat routing
PF_GROUP = {1: "vishal", 2: "lakshmi", 3: "lakshmi"}

# Which owner groups receive Telegram alerts (state changes + filings).
# Vishal opted out — Lakshmi is the TA lead and acts on alerts; Vishal's
# dashboard still shows all states, and his Sunday email digest continues.
# To re-enable Vishal's pings: add "vishal" back to this set.
TELEGRAM_ALERT_GROUPS = {"lakshmi"}
PF_NAME = {1: "Vishal", 2: "Lakshmi", 3: "Abinaya"}


def chat_id_for_group(group: str):
    # Only ONE Telegram group exists — the one already set up. It now carries
    # Lakshmi + Abinaya's alerts (see TELEGRAM_ALERT_GROUPS below for which
    # portfolios' data actually gets sent to it).
    import os as _os
    return _os.environ.get("TELEGRAM_CHAT_ID")


def extract_yf_ticker(name: str):
    m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(name))
    if not m:
        return None
    exch, sym = m.group(1), m.group(2).strip()
    return f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"


def short_name(name: str) -> str:
    n = re.sub(r"\s*\([^)]*\)\s*$", "", str(name))
    n = re.sub(r"\s+(LIMITED|LTD\.?|LTD)\s*$", "", n, flags=re.IGNORECASE)
    return n.strip().title()


# ---------------------------------------------------------------------------
# MODE: states — flowchart state-change detection
# ---------------------------------------------------------------------------

STATE_ICON = {
    "EXIT": "🔴", "BULLISH SIGNAL": "🟢", "WAIT/WATCH": "🔵",
    "BE CAUTIOUS": "🟠", "MOMENTUM FADING": "🟣", "MAINTAIN/ADD": "🟢",
    "INSUFFICIENT DATA": "⚪", "NO DATA": "⚪",
}
# Only these transitions are worth waking people up for
ALERT_WORTHY = {"EXIT", "BE CAUTIOUS", "MOMENTUM FADING", "BULLISH SIGNAL", "MAINTAIN/ADD"}


def vol_context(vol_ratio) -> str:
    if vol_ratio is None or pd.isna(vol_ratio):
        return ""
    if vol_ratio >= 2.0:
        return f"\nVolume: <b>{vol_ratio:.1f}x</b> 10-wk avg — heavy, institutions likely active"
    if vol_ratio >= 1.5:
        return f"\nVolume: <b>{vol_ratio:.1f}x</b> 10-wk avg — elevated"
    if vol_ratio <= 0.6:
        return f"\nVolume: {vol_ratio:.1f}x 10-wk avg — quiet (weak-hands move?)"
    return f"\nVolume: {vol_ratio:.1f}x 10-wk avg"


def run_states():
    client = sb()
    holdings = get_holdings(client)
    if holdings.empty:
        print("No holdings.")
        return

    # Last known states, keyed (ticker, portfolio)
    prev_rows = client.table("alert_state").select("*").execute().data or []
    prev = {(r["ticker"], r.get("portfolio_id", 1)): r["state"] for r in prev_rows}

    # Compute each ticker ONCE, alert per portfolio that holds it
    state_cache = {}
    pending, changes_by_group, errors = {}, {}, 0
    vol_spikes = {}   # (group, ticker) -> spike info  (volume alerts, 15-Jul-2026)
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        if not ticker:
            continue
        pf = int(h.get("portfolio_id", 1))
        group = PF_GROUP.get(pf, "vishal")
        if ticker not in state_cache:
            try:
                state_cache[ticker] = signals.current_state(ticker)
            except Exception as e:
                errors += 1
                print(f"⚠️ {ticker}: {e}")
                continue
        d = state_cache.get(ticker)
        if not d:
            continue

        state = d["state"]
        old = prev.get((ticker, pf))
        if old != state and state in ALERT_WORTHY:
            # Aggregate per (group, ticker): one alert per stock per group,
            # even when multiple household portfolios hold it.
            key = (group, ticker)
            entry = pending.setdefault(key, {
                "name": short_name(h["stock_name"]), "d": d,
                "state": state, "pfs": [], "olds": [],
            })
            entry["pfs"].append(pf)
            entry["olds"].append(old)

        # --- Volume spike detection (requested by Lakshmi, 15-Jul-2026) ---
        # "Unusually high trading activity" alerts, ScoutQuest-style, from
        # data we already compute. vol_ratio = current week's volume vs the
        # 10-week average -- but early in the week the current bar only has
        # 1-2 days of volume, so we PACE-ADJUST: scale by 5/elapsed trading
        # days. A stock that's already traded 0.8x a full week's average by
        # Tuesday morning is pacing at 2x -- that's the signal. Threshold 2.0.
        vr = d.get("vol_ratio")
        if vr is not None and not pd.isna(vr) and d["state"] not in ("NO DATA", "INSUFFICIENT DATA"):
            elapsed = min(datetime.utcnow().weekday() + 1, 5)  # Mon=1 .. Fri=5
            pace = float(vr) * 5.0 / elapsed
            if pace >= 2.0:
                key = (group, ticker)
                if key not in vol_spikes:
                    vol_spikes[key] = {
                        "name": short_name(h["stock_name"]),
                        "pace": pace, "raw": float(vr),
                        "close": d.get("close"), "pfs": [],
                    }
                vol_spikes[key]["pfs"].append(pf)

        client.table("alert_state").upsert({
            "ticker": ticker, "portfolio_id": pf, "state": state,
            "reason": d.get("reason", ""),
            "updated_at": datetime.utcnow().isoformat(),
        }).execute()

    # Format one message per (group, ticker)
    for (group, ticker), e in pending.items():
        d, state = e["d"], e["state"]
        icon = STATE_ICON.get(state, "•")
        if group == "lakshmi":
            uniq_pfs = sorted(set(e["pfs"]))
            tag = "[Both] " if len(uniq_pfs) > 1 else f"[{PF_NAME.get(uniq_pfs[0], uniq_pfs[0])}] "
        else:
            tag = ""
        olds = {o for o in e["olds"] if o}
        was = f"\n(was {olds.pop()})" if len(olds) == 1 else ""
        msg = (f"{icon} {tag}<b>{e['name']}</b> → <b>{state}</b>"
               + was
               + f"\n{d.get('reason','')}"
               + vol_context(d.get("vol_ratio")))
        if state == "BULLISH SIGNAL":
            vr = d.get("vol_ratio")
            if vr is not None and not pd.isna(vr):
                if vr >= 1.5:
                    msg += "\n✅ Breakout volume-CONFIRMED — full size per rules"
                else:
                    msg += "\n⚠️ Breakout on weak volume — half size per rules"
        changes_by_group.setdefault(group, []).append(msg)

    sent = 0
    for group, changes in changes_by_group.items():
        if group not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(changes)} state change(s) for '{group}' — Telegram off for this group)")
            continue
        chat = chat_id_for_group(group)
        if not chat:
            print(f"⚠️ No Telegram chat configured for group '{group}' "
                  f"({len(changes)} alert(s) dropped)")
            continue
        header = f"📊 <b>State changes</b> · {date.today().strftime('%d %b %Y')}\n\n"
        send_telegram(header + "\n\n".join(changes), chat_id=chat)
        sent += len(changes)
    if sent:
        print(f"Sent {sent} state-change alert(s) across {len(changes_by_group)} group(s).")
    else:
        print(f"No state changes. ({len(holdings)} holdings checked, {errors} fetch errors)")

    # --- Volume spike dispatch (one alert per stock per group per day) ---
    if vol_spikes:
        today_iso = date.today().isoformat()
        try:
            logged = client.table("volume_alert_log").select("ticker, grp") \
                .eq("alert_date", today_iso).execute().data or []
            already = {(r["ticker"], r["grp"]) for r in logged}
        except Exception:
            already = set()

        spikes_by_group = {}
        for (group, ticker), s in vol_spikes.items():
            if (ticker, group) in already:
                continue
            if group == "lakshmi":
                uniq = sorted(set(s["pfs"]))
                tag = "[Both] " if len(uniq) > 1 else f"[{PF_NAME.get(uniq[0], uniq[0])}] "
            else:
                tag = ""
            price = f" · last ₹{s['close']:,.2f}" if s.get("close") else ""
            msg = (f"🔥 {tag}<b>{s['name']}</b> — unusually high trading activity\n"
                   f"Pacing at <b>{s['pace']:.1f}x</b> its 10-week average volume "
                   f"(this week already {s['raw']:.1f}x a full week's average){price}")
            spikes_by_group.setdefault(group, []).append((ticker, msg, s["pace"]))

        v_sent = 0
        for group, items in spikes_by_group.items():
            if group not in TELEGRAM_ALERT_GROUPS:
                print(f"({len(items)} volume spike(s) for '{group}' — Telegram off for this group)")
                continue
            chat = chat_id_for_group(group)
            if not chat:
                continue
            header = f"🔥 <b>Volume alerts</b> · {date.today().strftime('%d %b %Y')}\n\n"
            send_telegram(header + "\n\n".join(m for _, m, _ in items), chat_id=chat)
            for ticker, _, pace in items:
                try:
                    client.table("volume_alert_log").upsert({
                        "ticker": ticker, "grp": group,
                        "alert_date": today_iso, "pace_ratio": round(pace, 2),
                    }).execute()
                except Exception as e:
                    print(f"⚠️ volume_alert_log write failed for {ticker}: {e}")
            v_sent += len(items)
        if v_sent:
            print(f"Sent {v_sent} volume-spike alert(s).")


# ---------------------------------------------------------------------------
# MODE: filings — NSE/BSE corporate announcements (best-effort)
# ---------------------------------------------------------------------------

MATERIAL_KEYWORDS = [
    "order", "contract", "dividend", "bonus", "split", "buyback", "results",
    "financial result", "acquisition", "pledge", "resignation", "appointment",
    "rating", "fund raise", "preferential", "rights issue", "expansion",
]


def _fingerprint(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:32]


def fetch_bse_announcements(scrip_code: str) -> list:
    """BSE announcements for one scrip. Returns list of dicts; [] on any failure."""
    try:
        url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
               f"?pageno=1&strCat=-1&strPrevDate=&strScrip={scrip_code}"
               "&strSearch=P&strToDate=&strType=C")
        r = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bseindia.com/",
        })
        if r.status_code != 200:
            return []
        data = r.json().get("Table", []) or []
        out = []
        for a in data[:10]:
            out.append({
                "headline": a.get("NEWSSUB") or a.get("HEADLINE") or "",
                "date": (a.get("NEWS_DT") or "")[:10],
                "url": f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{a.get('ATTACHMENTNAME')}"
                        if a.get("ATTACHMENTNAME") else "https://www.bseindia.com/corporates/ann.html",
            })
        return out
    except Exception as e:
        print(f"  (BSE fetch failed for {scrip_code}: {e})")
        return []


def fetch_nse_announcements(symbol: str) -> list:
    """NSE announcements for one symbol. Session-warmup approach; [] on failure."""
    try:
        s = requests.Session()
        s.headers.update({
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"),
            "Accept-Language": "en-US,en;q=0.9",
        })
        s.get("https://www.nseindia.com", timeout=15)  # cookie warmup
        r = s.get(f"https://www.nseindia.com/api/corporate-announcements"
                  f"?index=equities&symbol={symbol}", timeout=15)
        if r.status_code != 200:
            return []
        data = r.json() or []
        out = []
        for a in (data if isinstance(data, list) else [])[:10]:
            out.append({
                "headline": a.get("desc") or a.get("attchmntText") or "",
                "date": (a.get("an_dt") or a.get("sort_date") or "")[:10],
                "url": a.get("attchmntFile") or "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            })
        return out
    except Exception as e:
        print(f"  (NSE fetch failed for {symbol}: {e})")
        return []


def run_filings():
    client = sb()
    holdings = get_holdings(client)
    if holdings.empty:
        return

    seen = {r["fingerprint"]
            for r in (client.table("filings_seen").select("fingerprint").execute().data or [])}

    alerts_by_group = {}
    seen_syms = set()
    for _, h in holdings.iterrows():
        name = h["stock_name"]
        m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(name))
        if not m:
            continue
        exch, sym = m.group(1), m.group(2).strip()
        holder_groups = {PF_GROUP.get(int(hh.get("portfolio_id", 1)), "vishal")
                          for _, hh in holdings.iterrows()
                          if str(hh["stock_name"]) == str(name)}
        if sym in seen_syms:
            continue
        seen_syms.add(sym)
        anns = (fetch_nse_announcements(sym) if exch == "XNSE"
                else fetch_bse_announcements(sym))

        cutoff = (date.today() - timedelta(days=3)).isoformat()
        for a in anns:
            if not a["headline"] or (a["date"] and a["date"] < cutoff):
                continue
            if not any(k in a["headline"].lower() for k in MATERIAL_KEYWORDS):
                continue
            fp = _fingerprint(sym, a["headline"], a["date"])
            if fp in seen:
                continue
            for g in holder_groups:
                alerts_by_group.setdefault(g, []).append(
                    f"📢 <b>{short_name(name)}</b>: {a['headline'][:200]}"
                    f"\n{a['date']} · <a href=\"{a['url']}\">filing</a>"
                )
            client.table("filings_seen").insert({
                "fingerprint": fp, "ticker": sym,
                "headline": a["headline"][:300], "filing_date": a["date"] or None,
            }).execute()
            seen.add(fp)

    total = 0
    for g, alerts in alerts_by_group.items():
        if g not in TELEGRAM_ALERT_GROUPS:
            continue
        chat = chat_id_for_group(g)
        if not chat:
            continue
        send_telegram("🗞 <b>Exchange filings</b>\n\n" + "\n\n".join(alerts[:15]),
                      chat_id=chat)
        total += len(alerts)
    if total:
        print(f"Sent {total} filing alert(s).")
    else:
        print("No new material filings.")


# ---------------------------------------------------------------------------
# MODE: calendar — this week's results dates (best-effort via NSE)
# ---------------------------------------------------------------------------

def run_calendar():
    client = sb()
    holdings = get_holdings(client)
    symbols = {}
    for _, h in holdings.iterrows():
        m = re.search(r"\(XNSE:([^)]+)\)", str(h["stock_name"]))
        if m:
            symbols[m.group(1).strip()] = short_name(h["stock_name"])

    events = []
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        s.get("https://www.nseindia.com", timeout=15)
        r = s.get("https://www.nseindia.com/api/event-calendar", timeout=15)
        if r.status_code == 200:
            week_end = (date.today() + timedelta(days=7)).isoformat()
            for e in (r.json() or []):
                sym = e.get("symbol", "")
                edate = (e.get("date") or "")[:10]
                if sym in symbols and date.today().isoformat() <= edate <= week_end:
                    events.append(f"• <b>{symbols[sym]}</b> — {e.get('purpose','event')} on {edate}")
    except Exception as e:
        print(f"(calendar fetch failed: {e})")

    if events:
        send_telegram("🗓 <b>This week — corporate events on our holdings</b>\n\n"
                      + "\n".join(events))
        print(f"Sent calendar with {len(events)} event(s).")
    else:
        print("No events found for this week (or calendar fetch unavailable).")


# ---------------------------------------------------------------------------
# MODE: digest — Sunday email summary
# ---------------------------------------------------------------------------

def run_digest():
    """One weekly digest, sent to the existing DIGEST_EMAILS recipients,
    reporting Lakshmi + Abinaya's holdings (matches the Telegram alert scope:
    Vishal's own portfolio is tracked on the dashboard but not pushed here)."""
    client = sb()
    all_holdings = get_holdings(client)
    if all_holdings.empty:
        return
    pf_ids = [p for p, g in PF_GROUP.items() if g == "lakshmi"]
    holdings = all_holdings[all_holdings["portfolio_id"].isin(pf_ids)]
    if holdings.empty:
        print("(digest: no Lakshmi/Abinaya holdings yet)")
        return
    _digest_for(client, holdings)


def _digest_for(client, holdings):
    import os as _os
    rows, exits, cautions, adds = [], [], [], []

    # Compute once per ticker, then note WHICH portfolios hold it —
    # same duplicate-collapse logic as the Telegram alerts: [Both] when
    # Lakshmi and Abinaya both hold it, otherwise [Lakshmi]/[Abinaya].
    by_ticker = {}
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        if not ticker:
            continue
        entry = by_ticker.setdefault(ticker, {
            "name": short_name(h["stock_name"]), "pfs": set()})
        entry["pfs"].add(int(h.get("portfolio_id", 2)))

    for ticker, e in by_ticker.items():
        try:
            d = signals.current_state(ticker)
        except Exception:
            continue
        owners = sorted(e["pfs"])
        tag = "[Both] " if len(owners) > 1 else f"[{PF_NAME.get(owners[0], owners[0])}] "
        name = f"{tag}{e['name']}"
        st_ = d["state"]
        rows.append((name, st_, d.get("reason", "")))
        if st_ == "EXIT":
            exits.append(name)
        elif st_ in ("BE CAUTIOUS", "MOMENTUM FADING"):
            cautions.append(name)
        elif st_ == "MAINTAIN/ADD":
            adds.append(name)

    color = {"EXIT": "#dc2626", "BE CAUTIOUS": "#d97706", "MOMENTUM FADING": "#7c3aed",
             "MAINTAIN/ADD": "#16a34a", "BULLISH SIGNAL": "#16a34a",
             "WAIT/WATCH": "#0891b2"}
    trs = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid #eee'>{n}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee;"
        f"color:{color.get(s,'#333')};font-weight:600'>{s}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee;color:#555;"
        f"font-size:13px'>{r}</td></tr>"
        for n, s, r in sorted(rows, key=lambda x: x[1]))

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:720px">
      <h2 style="color:#1e3a8a">Weekly Portfolio Digest — {date.today().strftime('%d %b %Y')}</h2>
      <p><b>{len(rows)}</b> holdings scanned ·
         <span style="color:#dc2626"><b>{len(exits)}</b> EXIT</span> ·
         <span style="color:#d97706"><b>{len(cautions)}</b> caution</span> ·
         <span style="color:#16a34a"><b>{len(adds)}</b> healthy</span></p>
      {"<p style='color:#dc2626'><b>Action needed:</b> " + ", ".join(exits) + "</p>" if exits else ""}
      <table style="border-collapse:collapse;width:100%">
        <tr style="background:#1e3a8a;color:#fff">
          <th style="padding:8px 10px;text-align:left">Stock</th>
          <th style="padding:8px 10px;text-align:left">State</th>
          <th style="padding:8px 10px;text-align:left">Reason</th></tr>
        {trs}
      </table>
      <p style="color:#888;font-size:12px;margin-top:16px">
        Generated by the alert engine · flowchart v1.0 (40W EMA, 2% convergence,
        buffered EXIT) · data via yfinance weekly bars</p>
    </div>"""

    send_email(f"Portfolio Weekly Digest — {date.today().strftime('%d %b')}", html)
    print(f"Digest sent: {len(rows)} holdings, {len(exits)} exits, {len(cautions)} cautions.")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "states"
    {"states": run_states,
     "filings": run_filings,
     "calendar": run_calendar,
     "digest": run_digest}.get(mode, run_states)()
