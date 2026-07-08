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
    return pd.DataFrame(res.data or [])


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
        return f"\nVolume: <b>{vol_ratio:.1f}x</b> 20-wk avg — heavy, institutions likely active"
    if vol_ratio >= 1.5:
        return f"\nVolume: <b>{vol_ratio:.1f}x</b> 20-wk avg — elevated"
    if vol_ratio <= 0.6:
        return f"\nVolume: {vol_ratio:.1f}x 20-wk avg — quiet (weak-hands move?)"
    return f"\nVolume: {vol_ratio:.1f}x 20-wk avg"


def run_states():
    client = sb()
    holdings = get_holdings(client)
    if holdings.empty:
        print("No holdings.")
        return

    # Last known states
    prev = {r["ticker"]: r["state"]
            for r in (client.table("alert_state").select("*").execute().data or [])}

    changes, errors = [], 0
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        if not ticker:
            continue
        try:
            d = signals.current_state(ticker)
        except Exception as e:
            errors += 1
            print(f"⚠️ {ticker}: {e}")
            continue

        state = d["state"]
        old = prev.get(ticker)
        if old != state and state in ALERT_WORTHY:
            name = short_name(h["stock_name"])
            icon = STATE_ICON.get(state, "•")
            msg = (f"{icon} <b>{name}</b> → <b>{state}</b>"
                   + (f"\n(was {old})" if old else "")
                   + f"\n{d.get('reason','')}"
                   + vol_context(d.get("vol_ratio")))
            # BULLISH SIGNAL volume-confirmation layer
            if state == "BULLISH SIGNAL":
                vr = d.get("vol_ratio")
                if vr is not None and not pd.isna(vr):
                    if vr >= 1.5:
                        msg += "\n✅ Breakout volume-CONFIRMED — full size per rules"
                    else:
                        msg += "\n⚠️ Breakout on weak volume — half size per rules"
            changes.append(msg)

        # Upsert latest state regardless (so first run seeds silently next time)
        client.table("alert_state").upsert({
            "ticker": ticker, "state": state,
            "reason": d.get("reason", ""),
            "updated_at": datetime.utcnow().isoformat(),
        }).execute()

    if changes:
        header = f"📊 <b>State changes</b> · {date.today().strftime('%d %b %Y')}\n\n"
        send_telegram(header + "\n\n".join(changes))
        print(f"Sent {len(changes)} state-change alert(s).")
    else:
        print(f"No state changes. ({len(holdings)} holdings checked, {errors} fetch errors)")


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

    alerts = []
    for _, h in holdings.iterrows():
        name = h["stock_name"]
        m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(name))
        if not m:
            continue
        exch, sym = m.group(1), m.group(2).strip()
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
            alerts.append(
                f"📢 <b>{short_name(name)}</b>: {a['headline'][:200]}"
                f"\n{a['date']} · <a href=\"{a['url']}\">filing</a>"
            )
            client.table("filings_seen").insert({
                "fingerprint": fp, "ticker": sym,
                "headline": a["headline"][:300], "filing_date": a["date"] or None,
            }).execute()
            seen.add(fp)

    if alerts:
        send_telegram("🗞 <b>Exchange filings</b>\n\n" + "\n\n".join(alerts[:15]))
        print(f"Sent {len(alerts)} filing alert(s).")
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
    client = sb()
    holdings = get_holdings(client)
    if holdings.empty:
        return

    rows, exits, cautions, adds = [], [], [], []
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        if not ticker:
            continue
        try:
            d = signals.current_state(ticker)
        except Exception:
            continue
        name = short_name(h["stock_name"])
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
