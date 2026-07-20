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


def check_watchlist_entries(client):
    """Watchlist ENTRY alerts (added 17-Jul-2026) — the mirror image of the
    exit-side state alerts. Sweeps every portfolio's watchlist hourly:
    - ZONE alert when a stock touches the 10DMA (1st tranche) or 21DMA
      (2nd & final tranche) per Lakshmi's staged-entry system
    - TARGET alert when CMP reaches the stored target buy price
    Dedup: once per stock per group per day per kind (entry_alert_log).
    Known limit: entry math runs off Yahoo daily bars, so SME-tracked
    watchlist names get skipped silently (same Yahoo blind spot as
    everywhere; bhavcopy-based entry math is a future add if needed)."""
    rows = client.table("watchlist").select("*").execute().data or []
    if not rows:
        return

    today_iso = date.today().isoformat()
    try:
        logged = client.table("entry_alert_log").select("ticker, grp, kind") \
            .eq("alert_date", today_iso).execute().data or []
        already = {(r["ticker"], r["grp"], r["kind"]) for r in logged}
    except Exception:
        already = set()

    # group watchlist rows per ticker: which groups watch it, min target
    by_ticker = {}
    for r in rows:
        m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(r.get("stock_name") or ""))
        if not m:
            continue
        exch, sym = m.group(1), m.group(2).strip()
        ticker = f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"
        grp = PF_GROUP.get(int(r.get("portfolio_id", 1)), "vishal")
        e = by_ticker.setdefault(ticker, {"name": short_name(r["stock_name"]),
                                          "groups": {}, })
        ge = e["groups"].setdefault(grp, {"pfs": [], "targets": []})
        pf = int(r.get("portfolio_id", 1))
        ge["pfs"].append(pf)
        t = r.get("target_buy_price")
        if t:
            # targets are PERSONAL: keep (pf, target) pairs, fire when ANY
            # member's target is hit. (A min() here was caught in testing
            # suppressing one member's legit alert when another's deeper
            # target hadn't been reached yet.)
            ge["targets"].append((pf, float(t)))

    msgs_by_group, to_log = {}, []
    for ticker, e in by_ticker.items():
        d = signals.daily_entry_state(ticker)
        if not d:
            continue                       # Yahoo blind spot or fetch failure
        zone = d["Entry Zone"]
        cmp_ = d["CMP (d)"]
        for grp, ge in e["groups"].items():
            uniq = sorted(set(ge["pfs"]))
            tag = ""
            if grp == "lakshmi":
                tag = "[Both] " if len(uniq) > 1 else f"[{PF_NAME.get(uniq[0], uniq[0])}] "
            if zone in ("TRANCHE 1", "TRANCHE 2") and (ticker, grp, "ZONE") not in already:
                which = ("1st tranche (10DMA ₹{:,.2f})".format(d["10DMA"])
                         if zone == "TRANCHE 1"
                         else "2nd & FINAL tranche (21DMA ₹{:,.2f})".format(d["21DMA"]))
                msgs_by_group.setdefault(grp, []).append(
                    f"🎯 {tag}<b>{e['name']}</b> — entry zone reached\n"
                    f"CMP ₹{cmp_:,.2f} at the {which}")
                to_log.append((ticker, grp, "ZONE"))
            hits = [(pf, t) for pf, t in ge["targets"] if cmp_ <= t]
            if hits and (ticker, grp, "TARGET") not in already:
                whose = ", ".join(
                    f"₹{t:,.2f} ({PF_NAME.get(pf, pf)})" if grp == "lakshmi"
                    else f"₹{t:,.2f}" for pf, t in hits)
                msgs_by_group.setdefault(grp, []).append(
                    f"💰 {tag}<b>{e['name']}</b> — target buy price hit\n"
                    f"CMP ₹{cmp_:,.2f} ≤ target {whose}")
                to_log.append((ticker, grp, "TARGET"))

    sent = 0
    for grp, msgs in msgs_by_group.items():
        if grp not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(msgs)} entry alert(s) for '{grp}' — Telegram off for this group)")
            continue
        chat = chat_id_for_group(grp)
        if not chat:
            continue
        send_telegram("🛒 <b>Watchlist entry alerts</b>\n\n" + "\n\n".join(msgs),
                      chat_id=chat)
        sent += len(msgs)
    for ticker, grp, kind in to_log:
        try:
            client.table("entry_alert_log").upsert({
                "ticker": ticker, "grp": grp,
                "alert_date": today_iso, "kind": kind}).execute()
        except Exception as ex:
            print(f"⚠️ entry_alert_log write failed for {ticker}: {ex}")
    if sent:
        print(f"Sent {sent} watchlist entry alert(s).")
    else:
        print(f"No watchlist entry alerts. ({len(by_ticker)} watchlist tickers checked)")


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

    # Watchlist entry sweep — the buy-side mirror of everything above
    try:
        check_watchlist_entries(client)
    except Exception as e:
        print(f"⚠️ watchlist entry sweep failed (holdings alerts unaffected): {e}")


# ---------------------------------------------------------------------------
# MODE: filings — NSE/BSE corporate announcements (best-effort)
# ---------------------------------------------------------------------------

# --- Filing summarization (requested by Lakshmi, 17-Jul-2026) -------------
# ScoutQuest-style: don't just say a filing exists, say WHAT it says.
# Chain: download attachment PDF -> extract text -> Claude Haiku summary
# (2-4 bullets). Every step degrades gracefully: no API key, scanned PDF,
# download failure, API error -- all fall back to headline+link, never
# block the alert itself.

SUMMARY_MODEL = "claude-haiku-4-5-20251001"
MAX_SUMMARIES_PER_RUN = 10   # cost guard: beyond this, headline-only


MAX_PDF_BYTES = 8 * 1024 * 1024   # 8 MB guard: annual reports etc. get headline-only


def _download_pdf_b64(url: str):
    """Filing PDF as base64, or None. Size-capped: giant documents (annual
    reports, investor decks) fall back to headline-only rather than burning
    tokens on 300 pages nobody asked to summarize."""
    try:
        import base64
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200 or not r.content[:5].startswith(b"%PDF"):
            return None
        if len(r.content) > MAX_PDF_BYTES:
            print(f"  [filings] PDF too large ({len(r.content)//1024} KB) — headline-only")
            return None
        return base64.standard_b64encode(r.content).decode()
    except Exception:
        return None


def summarize_filing(company: str, headline: str, pdf_url: str) -> str:
    """2-4 bullet gist via Claude reading the PDF NATIVELY (v2, 17-Jul-2026).
    The document goes to the model as-is and is read visually -- so scanned
    BSE filings, stamped faxes, and digital PDFs all take the identical
    path. Replaces the old text-extraction step, which returned nothing for
    scanned documents. '' on any failure -> alert falls back to headline."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return ""
    pdf_b64 = _download_pdf_b64(pdf_url)
    if not pdf_b64:
        return ""
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={
                "model": SUMMARY_MODEL, "max_tokens": 250,
                "messages": [{"role": "user", "content": [
                    {"type": "document",
                     "source": {"type": "base64", "media_type": "application/pdf",
                                "data": pdf_b64}},
                    {"type": "text", "text":
                        f"This is an Indian stock exchange filing by {company} "
                        f"(subject: {headline}). Summarize it for a retail "
                        f"investor's Telegram alert.\n"
                        f"Output EXACTLY this format, nothing else:\n"
                        f"Line 1: one emoji + a 5-10 word gist title\n"
                        f"Then 2-4 bullets starting with '- ', each under 15 words, "
                        f"only concrete facts (amounts, dates, names, percentages). "
                        f"No advice, no speculation, no preamble. If the document "
                        f"is unreadable, output exactly: UNREADABLE"}]}],
            }, timeout=90)
        if r.status_code != 200:
            print(f"  [filings] summary API {r.status_code} for {company} — headline-only")
            return ""
        blocks = r.json().get("content", [])
        out = "\n".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
        if not out or out.upper().startswith("UNREADABLE"):
            return ""
        return out[:600]
    except Exception as e:
        print(f"  [filings] summary failed for {company}: {e} — headline-only")
        return ""


MATERIAL_KEYWORDS = [
    "order", "contract", "dividend", "bonus", "split", "buyback", "results",
    "financial result", "acquisition", "pledge", "resignation", "appointment",
    "rating", "fund raise", "preferential", "rights issue", "expansion",
]


def _fingerprint(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:32]


# BSE's announcements API needs the NUMERIC scrip code -- these XBOM
# symbols aren't codes, so queries with them can never match ("No Record
# Found!", caught live 19-Jul-2026). Codes verified during the bhavcopy
# and fundamentals builds.
BSE_FILING_SCRIPS = {
    "CWD-MS": "543378",
    "HSIL-MT": "543916",
    "TRUECOLORS": "544531",
    "LEHAR": "532829",
    "SGRL": "540737",
}


def fetch_bse_announcements(scrip_code: str) -> list:
    """BSE announcements for one scrip. Returns list of dicts; [] on any failure.
    Hardened 19-Jul-2026: explicit 7-day date range (empty date params now
    return 'No Record Found!' even for valid codes), symbol->code mapping,
    and BSE's quirky bare-string empty response treated as normal."""
    scrip_code = BSE_FILING_SCRIPS.get(scrip_code, scrip_code)
    if not str(scrip_code).isdigit():
        print(f"  (BSE: no scrip code known for '{scrip_code}' — add to BSE_FILING_SCRIPS)")
        return []
    try:
        d_to = date.today().strftime("%Y%m%d")
        d_from = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
        url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
               f"?pageno=1&strCat=-1&strPrevDate={d_from}&strScrip={scrip_code}"
               f"&strSearch=P&strToDate={d_to}&strType=C")
        r = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bseindia.com/",
        })
        if r.status_code != 200:
            print(f"  (BSE HTTP {r.status_code} for {scrip_code}: {r.text[:100]!r})")
            return []
        payload = r.json()
        # Defensive: BSE sometimes returns a JSON *string* (block/error page)
        # instead of the expected dict -- the old code crashed with
        # "'str' object has no attribute 'get'" and hid what BSE actually
        # sent. Now the log shows the real payload so we can react.
        if not isinstance(payload, dict):
            if "no record" in str(payload).lower():
                return []          # BSE's way of saying "nothing filed" — normal
            print(f"  (BSE unexpected payload for {scrip_code}: "
                  f"{type(payload).__name__} = {str(payload)[:120]!r})")
            return []
        data = payload.get("Table") or []
        if not isinstance(data, list):
            print(f"  (BSE 'Table' not a list for {scrip_code}: {str(data)[:120]!r})")
            return []
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


_NSE_RSS_CACHE = None

def fetch_nse_rss() -> list:
    """ALL recent NSE corporate announcements in one fetch, via the RSS feed
    on nsearchives.nseindia.com (rewritten 19-Jul-2026).

    WHY: the old per-symbol approach hit www.nseindia.com's API, which
    stonewalls datacenter IPs -- a manual run showed 60/60 read-timeouts,
    meaning the filings feed had silently died for NSE stocks. The archives
    host is the same one bhavcopy.py fetches from daily without issue.
    One request replaces sixty. Cached per process run."""
    global _NSE_RSS_CACHE
    if _NSE_RSS_CACHE is not None:
        return _NSE_RSS_CACHE
    items = []
    try:
        import xml.etree.ElementTree as ET
        from email.utils import parsedate_to_datetime
        r = requests.get(
            "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=25)
        if r.status_code != 200:
            print(f"  (NSE RSS HTTP {r.status_code})")
            _NSE_RSS_CACHE = []
            return []
        root = ET.fromstring(r.content)
        for it in root.iter("item"):
            title = (it.findtext("title") or "").strip()
            desc = (it.findtext("description") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            d = ""
            try:
                d = parsedate_to_datetime(pub).date().isoformat()
            except Exception:
                pass
            items.append({"title": title, "desc": desc, "url": link, "date": d})
        print(f"  (NSE RSS: {len(items)} announcements fetched in one request)")
    except Exception as e:
        print(f"  (NSE RSS fetch failed: {e})")
    _NSE_RSS_CACHE = items
    return items


def fetch_nse_announcements(symbol: str) -> list:
    """Announcements for one NSE symbol, filtered from the shared RSS feed.
    Word-boundary match on the symbol in title/description avoids substring
    collisions (e.g. 'TCL' matching inside another company's text)."""
    out = []
    # Match the TITLE ONLY, anchored at the start ("SYMBOL - Subject" is the
    # feed's convention). Matching descriptions false-positives on short
    # symbols like TCL whenever those letters appear as a word in another
    # company's text -- caught in testing.
    pat = re.compile(rf"^\s*{re.escape(symbol)}\b", re.IGNORECASE)
    for it in fetch_nse_rss():
        if not pat.search(it["title"]):
            continue
        out.append({
            "headline": (it["desc"] or it["title"])[:300],
            "date": it["date"],
            "url": it["url"] or "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
        })
        if len(out) >= 10:
            break
    return out


def run_filings():
    client = sb()
    holdings = get_holdings(client)
    if holdings.empty:
        return

    seen = {r["fingerprint"]
            for r in (client.table("filings_seen").select("fingerprint").execute().data or [])}

    alerts_by_group = {}
    seen_syms = set()
    summaries_done = 0
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
            # ScoutQuest-style gist (17-Jul-2026): summarize the PDF when
            # possible; silently fall back to headline-only otherwise.
            gist = ""
            if summaries_done < MAX_SUMMARIES_PER_RUN:
                gist = summarize_filing(short_name(name), a["headline"], a["url"])
                if gist:
                    summaries_done += 1
            body = (f"📢 <b>{short_name(name)}</b>: {a['headline'][:200]}"
                    + (f"\n\n{gist}" if gist else "")
                    + f"\n{a['date']} · <a href=\"{a['url']}\">filing</a>")
            for g in holder_groups:
                alerts_by_group.setdefault(g, []).append(body)
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
        # Chunked dispatch (19-Jul-2026): pack alerts into as many messages
        # as needed, budgeted by CHARACTERS not count. Replaces a [:15] cap
        # that silently dropped overflow on cluster days (results season),
        # and fixes a worse latent bug: Telegram rejects messages over
        # 4096 chars outright -- with AI gists at up to ~800 chars each,
        # a single capped message could have exceeded that and lost ALL
        # of the day's filing alerts at once.
        header = "🗞 <b>Exchange filings</b>\n\n"
        budget = 3500
        chunk = []
        chunk_len = len(header)
        for a in alerts:
            if chunk and chunk_len + len(a) + 2 > budget:
                send_telegram(header + "\n\n".join(chunk), chat_id=chat)
                chunk, chunk_len = [], len(header)
            chunk.append(a)
            chunk_len += len(a) + 2
        if chunk:
            send_telegram(header + "\n\n".join(chunk), chat_id=chat)
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


# ---------------------------------------------------------------------------
# Digest v2 helpers (19-Jul-2026) — the weekly review meeting, in one email
# ---------------------------------------------------------------------------

def _xirr(cashflows):
    """Annualised XIRR via bisection. cashflows: [(date, amount)], buys
    negative, sells + final value positive. None when undefined."""
    if len(cashflows) < 2:
        return None
    amts = [a for _, a in cashflows]
    if all(a >= 0 for a in amts) or all(a <= 0 for a in amts):
        return None
    t0 = min(d for d, _ in cashflows)
    flows = [((d - t0).days / 365.0, a) for d, a in cashflows]

    def npv(rate):
        return sum(a / ((1.0 + rate) ** t) for t, a in flows)

    lo, hi = -0.95, 15.0
    try:
        if npv(lo) * npv(hi) > 0:
            return None
        for _ in range(120):
            mid = (lo + hi) / 2
            v = npv(mid)
            if abs(v) < 1e-7:
                break
            if npv(lo) * v < 0:
                hi = mid
            else:
                lo = mid
        return round(mid * 100, 2)
    except (OverflowError, ZeroDivisionError):
        return None


def _pf_cashflows(client, pf: int):
    """(date, amount) list from the transactions table for one portfolio.
    Buys negative, sells positive."""
    res = client.table("transactions").select(
        "transaction_type, amount, transaction_date").eq("portfolio_id", pf).execute()
    out = []
    for r in (res.data or []):
        try:
            d = date.fromisoformat(str(r["transaction_date"])[:10])
            amt = float(r["amount"] or 0)
        except (ValueError, TypeError):
            continue
        if amt <= 0:
            continue
        out.append((d, -amt if str(r.get("transaction_type", "buy")).lower() == "buy" else amt))
    return out


def _digest_deliv_strength(client, tickers):
    """{ticker: 4wk delivery avg} for tickers with >=10 stored days."""
    out = {}
    try:
        since = (date.today() - timedelta(days=45)).isoformat()
        res = (client.table("delivery_daily").select("ticker, price_date, deliv_pct")
               .in_("ticker", list(tickers)).gte("price_date", since)
               .order("price_date", desc=True).execute())
        rows = res.data or []
        byt = {}
        for r in rows:
            byt.setdefault(r["ticker"], []).append(float(r["deliv_pct"]))
        for t, vals in byt.items():
            if len(vals) >= 10:
                out[t] = sum(vals[:20]) / min(len(vals), 20)
    except Exception:
        pass
    return out



BENCHMARK_TICKER = "^CNXSC"   # Nifty Smallcap 100 on Yahoo
_BENCH_CACHE = None
_BENCH_LABEL = "Nifty Smallcap 100"


def _benchmark_series():
    """Daily closes of the Nifty Smallcap 100, ~3 years, as a pandas Series
    indexed by date. None on failure -- benchmark sections then degrade to
    a note, per house rules. Cached per process run."""
    global _BENCH_CACHE
    if _BENCH_CACHE is not None:
        return _BENCH_CACHE if _BENCH_CACHE is not False else None
    try:
        import yfinance as yf
        df = yf.download(BENCHMARK_TICKER, period="3y", interval="1d",
                         progress=False, auto_adjust=False)
        if df.empty:
            print("(digest: Yahoo returned EMPTY for ^CNXSC — falling back to our table)")
            raise ValueError("empty")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        s = df["Close"].dropna()
        s.index = pd.to_datetime(s.index).date
        # STUB GUARD (21-Jul-2026): Yahoo once answered with a ~1-day series;
        # "non-empty" let it beat our 257-day ETF table, every old buy got
        # approximated at a flat level, and the digest printed "index made
        # -0.0%" with a fake full-XIRR alpha. A benchmark series must carry
        # real history to be allowed to win.
        if len(s) < 60:
            print(f"(digest: Yahoo ^CNXSC returned a stub ({len(s)} days) — "
                  f"falling back to our table)")
            raise ValueError("empty")
        global _BENCH_LABEL
        _BENCH_LABEL = "Nifty Smallcap 100"
        _BENCH_CACHE = s
        return s
    except Exception as e:
        if str(e) != "empty":
            print(f"(digest: Yahoo benchmark fetch failed: {e} — falling back to our table)")
        # AUTHORITATIVE FALLBACK (19-Jul-2026): our own index history,
        # stored daily by bhavcopy.py from NSE's official ind_close_all
        # file (ticker NIFTYSMLCAP100.IDX in sme_daily_prices). Same
        # own-the-data pattern as every other Yahoo blind spot this week.
        try:
            client = sb()
            # Fallback chain: exact index (if NSE ever restores the file),
            # then Smallcap-250 ETF proxies (priced by our own daily
            # bhavcopy -- the proven path). _BENCH_LABEL records which
            # source won so the email can say so honestly.
            candidates = [
                ("NIFTYSMLCAP100.IDX", "Nifty Smallcap 100"),
                ("HDFCSML250.NS", "Nifty Smallcap 250 (HDFC ETF proxy)"),
                ("MOSMALL250.NS", "Nifty Smallcap 250 (MO ETF proxy)"),
            ]
            for tick, label in candidates:
                res = (client.table("sme_daily_prices")
                       .select("price_date, close").eq("ticker", tick)
                       .order("price_date", desc=True).limit(900).execute())
                rows = res.data or []
                if len(rows) >= 60:      # need real history, not a few days
                    rows.sort(key=lambda r: r["price_date"])
                    s = pd.Series([float(r["close"]) for r in rows],
                                  index=[date.fromisoformat(str(r["price_date"])[:10]) for r in rows])
                    global _BENCH_LABEL
                    _BENCH_LABEL = label
                    print(f"(digest: benchmark = {label}, {len(s)} days from own table)")
                    _BENCH_CACHE = s
                    return s
            print("(digest: no benchmark series has enough history yet — "
                  "run the standard bhavcopy backfill to build the ETF proxy history)")
        except Exception as e2:
            print(f"(digest: own-table benchmark fallback failed: {e2})")
        _BENCH_CACHE = False
        return None


def _level_on(series, d):
    """Index level on date d, or the nearest trading day BEFORE it."""
    for back in range(0, 8):
        dd = d - timedelta(days=back)
        if dd in series.index:
            return float(series[dd])
    return None


_BENCH_APPROX_FROM = None

def _benchmark_xirr(cashflows):
    """Lakshmi's benchmark rule (19-Jul-2026): the shadow portfolio.
    Every actual cashflow (same rupees, same dates) buys/sells the index
    proxy instead; XIRR of that shadow book is the yardstick, portfolio
    XIRR minus it = true alpha.

    PARTIAL-HISTORY HANDLING (21-Jul-2026): the ETF proxy's stored history
    starts ~mid-2025, but real buys predate it. A flow older than the
    series now uses the EARLIEST available level, and the email discloses
    the approximation ("index history from <date>; earlier flows
    approximated"). This slightly flatters the index (assumes it went
    nowhere before its first data point), i.e. it UNDERSTATES alpha --
    the conservative direction for a "should we even be doing this"
    verdict. Honest partial benchmark > eternal 'unavailable'."""
    global _BENCH_APPROX_FROM
    _BENCH_APPROX_FROM = None
    series = _benchmark_series()
    if series is None or len(cashflows) < 1:
        return None
    first_d = series.index[0]
    first_lvl = float(series.iloc[0])
    units = 0.0
    approx = False
    for d, a in cashflows:
        lvl = _level_on(series, d)
        if lvl is None:
            if d < first_d:
                lvl = first_lvl
                approx = True
            else:
                return None      # gap INSIDE the series: genuinely broken
        if a < 0:
            units += (-a) / lvl          # buy day: rupees into the index
        else:
            units = max(0.0, units - a / lvl)   # sell day: rupees out
    if approx:
        _BENCH_APPROX_FROM = first_d
    final_val = units * float(series.iloc[-1])
    return _xirr(cashflows + [(date.today(), final_val)])


def _benchmark_week_move():
    """Index % move over the last ~5 trading days, or None."""
    s = _benchmark_series()
    if s is None or len(s) < 6:
        return None
    return (float(s.iloc[-1]) / float(s.iloc[-6]) - 1) * 100


def _fmt_l(x):
    """Rupees in lakh/crore, compact."""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return "—"
    if abs(x) >= 1e7:
        return f"₹{x/1e7:,.2f} Cr"
    return f"₹{x/1e5:,.1f} L"




def _box(title, inner_html, accent="#1e3a8a", bg="#ffffff"):
    """A titled section card. Inline styles only -- email clients ignore
    stylesheets, so every visual decision must travel inside the tag."""
    if not inner_html:
        return ""
    return (f"<div style='background:{bg};border:1px solid #e2e8f0;"
            f"border-left:4px solid {accent};border-radius:8px;"
            f"padding:14px 18px;margin:14px 0'>"
            f"<div style='font-size:15px;font-weight:700;color:{accent};"
            f"margin-bottom:8px'>{title}</div>"
            f"<div style='font-size:14px;color:#334155;line-height:1.55'>{inner_html}</div>"
            f"</div>")


def _bench_html(xirr, bench):
    """Alpha verdict per Lakshmi's rule: beating the Nifty Smallcap 100 by
    5+ pts = clearly worth it; 2-5 = marginal; below 2 = the index would
    have done the job. Honest '--' when either side is unavailable."""
    if xirr is None or bench is None:
        return (f"<p style='margin:4px 0;color:#888'>vs {_BENCH_LABEL}: "
                "benchmark unavailable this week</p>")
    alpha = xirr - bench
    if alpha >= 5:
        col, verdict = "#16a34a", "beating the index — clearly worth it ✅"
    elif alpha >= 2:
        col, verdict = "#d97706", "ahead, but inside the 2–5pt grey zone"
    else:
        col, verdict = "#dc2626", "NOT beating the index meaningfully — review"
    note = ""
    if _BENCH_APPROX_FROM:
        note = (f"<br><span style='color:#94a3b8;font-size:12px'>index history from "
                f"{_BENCH_APPROX_FROM.strftime('%d %b %Y')}; older buys approximated "
                f"at its first level (understates alpha)</span>")
    return (f"<p style='margin:4px 0'>vs <b>{_BENCH_LABEL}</b> "
            f"(same money, same dates): index would have made {bench:.1f}% "
            f"→ alpha <b style='color:{col}'>{alpha:+.1f} pts</b> — "
            f"<span style='color:{col}'>{verdict}</span>{note}</p>")


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
    """Digest v2 (19-Jul-2026): the weekly review meeting. Per-portfolio
    money numbers with week-over-week trend, states, dead-money flags,
    profit tiers, journal+audit corner, delivery conviction, concentration.
    Every section is individually try/excepted: one broken data layer
    degrades that section to a note, never kills the digest."""
    import json as _json
    today = date.today()
    pf_ids = sorted(int(p) for p in holdings["portfolio_id"].unique())

    # ---- per-ticker compute (once), incl. bars for dead-money ----
    by_ticker = {}
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        if not ticker:
            continue
        e = by_ticker.setdefault(ticker, {"name": short_name(h["stock_name"]),
                                          "pfs": {}, "state": None, "d": {}})
        pf = int(h.get("portfolio_id", 2))
        e["pfs"][pf] = {"qty": float(h.get("quantity") or 0),
                        "cost": float(h.get("purchase_cost") or 0)}

    dead_money, rows, exits, cautions, adds = [], [], [], [], []
    for ticker, e in by_ticker.items():
        try:
            d = signals.current_state(ticker)
            e["d"] = d or {}
            e["state"] = (d or {}).get("state")
        except Exception:
            continue
        owners = sorted(e["pfs"])
        tag = "[Both] " if len(owners) > 1 else f"[{PF_NAME.get(owners[0], owners[0])}] "
        name = f"{tag}{e['name']}"
        st_ = e["state"] or "NO DATA"
        rows.append((name, st_, e["d"].get("reason", "")))
        if st_ == "EXIT":
            exits.append(name)
        elif st_ in ("BE CAUTIOUS", "MOMENTUM FADING"):
            cautions.append(name)
        elif st_ == "MAINTAIN/ADD":
            adds.append(name)
        # dead money: ~13 weeks sideways (within ±10%), state not EXIT
        try:
            bars = signals.fetch_weekly(ticker)
            if len(bars) >= 14 and st_ != "EXIT":
                move = bars["Close"].iloc[-1] / bars["Close"].iloc[-14] - 1
                if abs(move) < 0.10:
                    dead_money.append((name, move * 100))
        except Exception:
            pass

    # ---- per-portfolio money numbers + snapshot diffs ----
    pf_sections = []
    detail_by_pf = {}
    for pf in pf_ids:
        try:
            inv = val = 0.0
            detail = {}
            for ticker, e in by_ticker.items():
                if pf not in e["pfs"]:
                    continue
                p = e["pfs"][pf]
                inv += p["qty"] * p["cost"]
                close = e["d"].get("close")
                v = p["qty"] * float(close) if close else p["qty"] * p["cost"]
                val += v
                pnl_pct = ((float(close) - p["cost"]) / p["cost"] * 100
                           if close and p["cost"] else 0.0)
                detail[ticker] = {"state": (str(e["state"]) if e["state"] else None),
                                  "pnl_pct": float(round(pnl_pct, 2))}
            unreal = val - inv
            raw_cfs = _pf_cashflows(client, pf)
            cfs = raw_cfs + [(today, val)]
            xirr = _xirr(cfs)
            bench = _benchmark_xirr(raw_cfs)
            detail_by_pf[pf] = detail

            # previous snapshot for trend
            prev = None
            try:
                r = (client.table("digest_history").select("*")
                     .eq("portfolio_id", pf).lt("snap_date", today.isoformat())
                     .order("snap_date", desc=True).limit(1).execute())
                prev = (r.data or [None])[0]
            except Exception:
                pass

            def _delta(cur, prev_v, pct=False, pts=False):
                if prev_v is None or cur is None:
                    return "<span style='color:#888'>(baseline set this week)</span>"
                dv = cur - float(prev_v)
                col = "#16a34a" if dv >= 0 else "#dc2626"
                arrow = "▲" if dv >= 0 else "▼"
                if pts:
                    return f"<span style='color:{col}'>{arrow} {abs(dv):.2f} pts WoW</span>"
                return f"<span style='color:{col}'>{arrow} {_fmt_l(abs(dv))} WoW</span>"

            trend_pnl = _delta(unreal, prev.get("unrealised") if prev else None)
            trend_xirr = (_delta(xirr, prev.get("xirr") if prev else None, pts=True)
                          if xirr is not None else "")

            # profit tiers: crossings vs last week's per-ticker pnl_pct
            tiers_html = ""
            try:
                prev_detail = (prev or {}).get("detail") or {}
                if isinstance(prev_detail, str):
                    prev_detail = _json.loads(prev_detail)
                crossed = []
                for t, cur_d in detail.items():
                    cur_p = cur_d["pnl_pct"]
                    prev_p = (prev_detail.get(t) or {}).get("pnl_pct")
                    for tier in (150, 100, 50):
                        if cur_p >= tier and (prev_p is None or prev_p < tier):
                            nm = by_ticker[t]["name"]
                            crossed.append(f"{nm} crossed <b>+{tier}%</b> (now {cur_p:+.0f}%)")
                            break
                if crossed:
                    tiers_html = ("<p>🏆 <b>Profit tiers this week:</b> "
                                  + " · ".join(crossed[:6]) + "</p>")
            except Exception:
                pass

            # concentration: top-5 share
            conc_html = ""
            try:
                vals = sorted((e["pfs"][pf]["qty"] * float(e["d"].get("close") or e["pfs"][pf]["cost"]), e["name"])
                              for t, e in by_ticker.items() if pf in e["pfs"])
                top5 = sum(v for v, _ in vals[-5:])
                share = top5 / val * 100 if val else 0
                warn = " style='color:#d97706'" if share >= 50 else ""
                conc_html = (f"<p{warn}>Top-5 concentration: <b>{share:.0f}%</b> of the book"
                             + (" — worth a look" if share >= 50 else "") + "</p>")
            except Exception:
                pass

            up = unreal >= 0
            pnl_col = "#16a34a" if up else "#dc2626"
            pf_sections.append(f"""
            <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;
                        padding:16px 20px;margin:12px 0">
              <div style="font-size:17px;font-weight:800;color:#0f172a;
                          border-bottom:2px solid #1e3a8a;padding-bottom:6px;
                          margin-bottom:10px">{PF_NAME.get(pf, pf)}</div>
              <table style="width:100%;border-collapse:collapse;font-size:14px">
                <tr>
                  <td style="padding:4px 0;color:#64748b;width:34%">Invested</td>
                  <td style="padding:4px 0;font-weight:700">{_fmt_l(inv)}</td>
                </tr><tr>
                  <td style="padding:4px 0;color:#64748b">Current value</td>
                  <td style="padding:4px 0;font-weight:700">{_fmt_l(val)}</td>
                </tr><tr>
                  <td style="padding:4px 0;color:#64748b">Unrealised P&amp;L</td>
                  <td style="padding:4px 0;font-weight:700;color:{pnl_col}">
                    {_fmt_l(unreal)} ({(unreal/inv*100 if inv else 0):+.1f}%)
                    &nbsp;<span style="font-weight:400;font-size:13px">{trend_pnl}</span></td>
                </tr><tr>
                  <td style="padding:4px 0;color:#64748b">XIRR (annualised)</td>
                  <td style="padding:4px 0;font-weight:700">
                    {f"{xirr:.1f}%" if xirr is not None else "—"}
                    &nbsp;<span style="font-weight:400;font-size:13px">{trend_xirr}</span></td>
                </tr>
              </table>
              <div style="margin-top:8px">{_bench_html(xirr, bench)}</div>
              {tiers_html}{conc_html}
            </div>""")

            # store this week's snapshot (upsert -> reruns safe)
            try:
                client.table("digest_history").upsert({
                    "portfolio_id": int(pf), "snap_date": today.isoformat(),
                    "invested": float(round(inv, 2)),
                    "current_value": float(round(val, 2)),
                    "unrealised": float(round(unreal, 2)),
                    "xirr": float(xirr) if xirr is not None else None,
                    "bench_xirr": float(bench) if bench is not None else None,
                    "detail": detail,
                }, on_conflict="portfolio_id,snap_date").execute()
            except Exception as ex:
                print(f"(digest: snapshot store failed for pf {pf}: {ex})")
        except Exception as ex:
            pf_sections.append(f"<p>({PF_NAME.get(pf, pf)}: numbers unavailable — {ex})</p>")

    # ---- journal + audits this week ----
    journal_html = ""
    try:
        wk_ago = (today - timedelta(days=7)).isoformat()
        jr = client.table("trade_journal").select("*") \
            .in_("portfolio_id", [int(p) for p in pf_ids]).execute().data or []
        entries = [j for j in jr if str(j.get("exit_date", "")) >= wk_ago]
        verdicts = []
        for j in jr:
            for w in (30, 60, 90):
                if str(j.get(f"audited_{w}d") or "") >= wk_ago and j.get(f"price_{w}d"):
                    chg = (float(j[f"price_{w}d"]) - float(j["exit_price"])) / float(j["exit_price"]) * 100
                    verdict = "saved" if chg < 0 else "cost"
                    verdicts.append(f"{short_name(j['ticker'])} +{w}d: exit "
                                    f"<b>{verdict} {abs(chg):.1f}%</b>")
        lines = []
        for j in entries:
            lines.append(f"{short_name(j['ticker'])} sold @ ₹{float(j['exit_price']):,.1f} "
                         f"({j['reason']})" + (f" — <i>{j['notes']}</i>" if j.get("notes") else ""))
        if lines or verdicts:
            journal_html = "<br>".join(lines + verdicts)
    except Exception:
        pass

    # ---- delivery conviction ----
    deliv_html = ""
    try:
        strengths = _digest_deliv_strength(client, tuple(by_ticker.keys()))
        conv = []
        for t, avg in strengths.items():
            e = by_ticker.get(t) or {}
            if avg >= 60 and e.get("state") in ("MAINTAIN/ADD", "BULLISH SIGNAL"):
                conv.append(f"{e['name']} ({avg:.0f}% delivery)")
        if conv:
            deliv_html = ", ".join(sorted(conv)[:8])
    except Exception:
        pass

    # ---- dead money ----
    dead_html = ""
    if dead_money:
        dead_html = ", ".join(f"{n} <span style='color:#64748b'>({m:+.0f}% in 13wk)</span>"
                              for n, m in sorted(dead_money)[:8])

    # ---- states table (unchanged core) ----
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

    action_box = ""
    if exits:
        action_box = ("<div style='background:#fef2f2;border:1px solid #fecaca;"
                      "border-left:4px solid #dc2626;border-radius:8px;"
                      "padding:14px 18px;margin:14px 0'>"
                      "<div style='font-size:15px;font-weight:800;color:#dc2626'>"
                      "⚠️ ACTION NEEDED — EXIT signals</div>"
                      "<div style='font-size:14px;color:#7f1d1d;margin-top:6px'>"
                      + ", ".join(exits) +
                      "</div></div>")

    html = f"""
    <div style="font-family:-apple-system,Segoe UI,Arial,sans-serif;max-width:700px;
                margin:0 auto;background:#f1f5f9;padding:18px">
      <div style="background:#1e3a8a;color:#ffffff;border-radius:10px;
                  padding:20px 24px;margin-bottom:6px">
        <div style="font-size:21px;font-weight:800">📊 Weekly Portfolio Digest</div>
        <div style="font-size:13px;opacity:.85;margin-top:4px">
          {today.strftime('%A, %d %B %Y')} · {len(rows)} holdings scanned ·
          {len(exits)} EXIT · {len(cautions)} caution · {len(adds)} healthy</div>
      </div>

      {action_box}
      {''.join(pf_sections)}

      {_box("💤 Dead money watch — 90+ days sideways", dead_html, accent="#64748b")}
      {_box("🏛 Conviction moves — healthy state + 60%+ delivery", deliv_html, accent="#0891b2")}
      {_box("📓 Journal &amp; audits this week", journal_html, accent="#7c3aed")}

      <div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:10px;
                  padding:16px 18px;margin:14px 0">
        <div style="font-size:15px;font-weight:700;color:#1e3a8a;margin-bottom:10px">
          📋 All holdings — flowchart states</div>
        <table style="border-collapse:collapse;width:100%;font-size:13px">
          <tr style="background:#1e3a8a;color:#fff">
            <th style="padding:8px 10px;text-align:left;border-radius:6px 0 0 0">Stock</th>
            <th style="padding:8px 10px;text-align:left">State</th>
            <th style="padding:8px 10px;text-align:left;border-radius:0 6px 0 0">Reason</th></tr>
          {trs}
        </table>
      </div>

      <div style="color:#94a3b8;font-size:11px;text-align:center;margin-top:14px">
        Generated by the alert engine · flowchart v1.0 (40W EMA) · prices via
        yfinance + official NSE/BSE files · benchmark: Nifty Smallcap 100 ·
        trends vs last Sunday's snapshot</div>
    </div>"""

    send_email(f"Portfolio Weekly Digest — {today.strftime('%d %b')}", html)

    # compact Telegram version of the same review
    try:
        tg = [f"🗓 <b>Weekly digest</b> · {today.strftime('%d %b')}"]
        for pf, sec in zip(pf_ids, pf_sections):
            det = detail_by_pf.get(pf)
            if det is None:
                continue
        for pf in pf_ids:
            det = detail_by_pf.get(pf)
            if det is None:
                continue
            snap = client.table("digest_history").select("*") \
                .eq("portfolio_id", pf).eq("snap_date", today.isoformat()) \
                .limit(1).execute().data
            if snap:
                s = snap[0]
                x = f" · XIRR {float(s['xirr']):.1f}%" if s.get("xirr") is not None else ""
                tg.append(f"<b>{PF_NAME.get(pf, pf)}</b>: {_fmt_l(s['current_value'])} "
                          f"({float(s['unrealised'])/float(s['invested'])*100:+.1f}%){x}")
        if exits:
            tg.append("🔴 EXIT: " + ", ".join(exits))
        if dead_money:
            tg.append(f"💤 {len(dead_money)} stock(s) on dead-money watch")
        tg.append("Full review in the email 📧")
        chat = chat_id_for_group("lakshmi")
        if chat:
            send_telegram("\n".join(tg), chat_id=chat)
    except Exception as ex:
        print(f"(digest: telegram summary failed: {ex})")
    print(f"Digest sent: {len(rows)} holdings, {len(exits)} exits, {len(cautions)} cautions.")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "states"
    {"states": run_states,
     "filings": run_filings,
     "calendar": run_calendar,
     "digest": run_digest}.get(mode, run_states)()
