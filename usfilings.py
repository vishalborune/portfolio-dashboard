"""
usfilings.py — SEC EDGAR filings + earnings-date alerts for Vishal's US book.

The US twin of run_filings (NSE RSS). Source is the SEC's own submissions feed
per company (official, free, JSON): https://data.sec.gov/submissions/CIK##.json
— every form the company or its insiders file, minutes after acceptance.

WHAT GETS ALERTED (a BLACKLIST, never a whitelist — missing a filing is the
cardinal sin, same rule as India):
  • Form 4 / 4/A   insider transactions — PARSED from the XML (free, no model):
                   who, role, BUY/SELL, shares, avg price, $ value, holding after.
                   Award/exercise/tax-withholding/gift codes (A M F G J W C) are
                   routine and skipped unless a real open-market P or S is present.
  • 8-K            material events — item codes decoded (2.02 results, 1.01 deal,
                   5.02 officer/director change, 4.01 AUDITOR CHANGE 🚨, ...) and a
                   Claude Haiku gist of the document text (temperature 0, capped).
  • 10-Q / 10-K    quarterly / annual report filed (⭐). Numbers will come from
                   XBRL companyfacts in the fundamentals phase — not the model.
  • SC 13D/13G     a holder crossing 5% (⭐) — headline + link.
  • everything else: headline + link, except ROUTINE_FORMS (144 notices, S-8
                   employee plans, Form 3/5, FWP, 11-K, SD ...).
Dedup: filings_seen fingerprint = "us|<accession number>" (same table as India).
Earnings dates: Finnhub calendar → a once-a-day brief before the US open.

SEC etiquette: a User-Agent that names the tool and a contact (env
SEC_USER_AGENT to set your own), <= ~8 requests/second. A generic UA is 403'd.

CLI:  python usfilings.py check     read-only: what would be sent (no Telegram, no writes)
      python usfilings.py run       live run (worker/cron entry point)
      python usfilings.py earnings  the earnings-date brief (honours ALERTS_DRY_RUN)
      python usfilings.py audit     last 7 days per company, how each form is classified
"""
from __future__ import annotations
import hashlib, html, json, os, re, sys, time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import requests

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

NY = ZoneInfo("America/New_York")
SEC_UA = os.environ.get("SEC_USER_AGENT", "PortfolioDashboard research@portfolio-dashboard.app")
H = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}
SEC_MIN_GAP = 0.15                 # seconds between SEC requests (<= ~8/s)
LOOKBACK_DAYS = 3                  # first sighting window; dedup covers the rest
MAX_GISTS_PER_RUN = 6              # Haiku cost guard; beyond this, headline-only
MAX_DOC_CHARS = 14000
CACHE_DIR = ".cache"

ROUTINE_FORMS = {"144", "144/A", "3", "3/A", "5", "5/A", "S-8", "S-8 POS", "11-K", "SD",
                 "FWP", "424B2", "424B3", "ARS", "DEFA14A", "PX14A6G", "CERT", "8-A12B",
                 "CORRESP", "UPLOAD", "IRANNOTICE", "NO ACT", "25-NSE", "EFFECT", "N-PX",
                 "SC 13G/A"}       # 13G/A = passive holders' routine amendments (noise)
STAR_FORMS = {"10-Q", "10-K", "10-Q/A", "10-K/A", "SC 13D", "SC 13D/A", "SC 13G", "S-3", "S-3ASR", "424B5", "DEF 14A"}
ITEM_LABELS = {
    "1.01": "material agreement", "1.02": "termination of agreement", "1.03": "BANKRUPTCY 🚨",
    "2.01": "acquisition / disposal", "2.02": "results announced ⭐", "2.03": "new debt",
    "2.04": "debt acceleration 🚨", "2.05": "restructuring / exit costs", "2.06": "impairment 🚨",
    "3.01": "delisting notice 🚨", "3.02": "unregistered share sale (dilution)", "3.03": "rights change",
    "4.01": "AUDITOR CHANGE 🚨", "4.02": "financials NOT to be relied on 🚨",
    "5.01": "change in control", "5.02": "director / officer change", "5.03": "bylaws / fiscal year",
    "5.07": "shareholder vote results", "5.08": "shareholder nominations", "7.01": "Reg FD disclosure",
    "8.01": "other event", "9.01": "exhibits",
}
INSIDER_ROUTINE_CODES = {"A", "M", "F", "G", "J", "W", "C", "D", "I", "L", "U", "X", "Z"}

_last_sec = [0.0]


def _sec_get(url: str, timeout: int = 30):
    gap = time.time() - _last_sec[0]
    if gap < SEC_MIN_GAP:
        time.sleep(SEC_MIN_GAP - gap)
    _last_sec[0] = time.time()
    r = requests.get(url, headers=H, timeout=timeout)
    if r.status_code != 200:
        print(f"  [usfilings] HTTP {r.status_code} {len(r.content)}B for {url[:90]}")
    return r


# ---------------------------------------------------------------------------
# universe + CIK map
# ---------------------------------------------------------------------------

def cik_map() -> dict:
    """{TICKER: (cik, company title)} from SEC's master list, cached 7 days."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, "sec_tickers.json")
    try:
        if os.path.exists(path) and time.time() - os.path.getmtime(path) < 7 * 86400:
            return json.load(open(path, encoding="utf-8"))
    except Exception:
        pass
    r = _sec_get("https://www.sec.gov/files/company_tickers.json")
    if r.status_code != 200:
        try:
            return json.load(open(path, encoding="utf-8"))
        except Exception:
            return {}
    out = {v["ticker"].upper(): (int(v["cik_str"]), v["title"]) for v in r.json().values()}
    json.dump(out, open(path, "w", encoding="utf-8"))
    return out


def universe(client) -> dict:
    """{symbol: {"cik", "title", "held": bool}} for US holdings + watchlist."""
    import usprices
    syms = usprices.us_universe(client)
    held = set()
    try:
        for r in (client.table("holdings").select("stock_name,portfolio_id")
                  .in_("portfolio_id", list(usprices.US_PORTFOLIOS)).execute().data or []):
            s = usprices.us_symbol(r.get("stock_name"))
            if s:
                held.add(s)
    except Exception:
        pass
    cm = cik_map()
    out, missing = {}, []
    for s in syms:
        key = s.replace("-", ".")            # SEC lists BRK.B with a dot
        hit = cm.get(s) or cm.get(key)
        if hit:
            out[s] = {"cik": hit[0], "title": hit[1], "held": s in held}
        else:
            missing.append(s)
    if missing:
        print(f"  [usfilings] ⚠️ no SEC CIK for {missing} — these names get NO filing alerts until resolved")
    return out


# ---------------------------------------------------------------------------
# feed
# ---------------------------------------------------------------------------

def recent_filings(cik: int, since: date) -> list[dict]:
    r = _sec_get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json")
    if r.status_code != 200:
        return []
    rec = r.json().get("filings", {}).get("recent", {})
    out = []
    n = len(rec.get("accessionNumber", []))
    for i in range(n):
        fd = rec["filingDate"][i]
        if fd < since.isoformat():
            break                             # newest first
        out.append({"acc": rec["accessionNumber"][i], "date": fd, "form": rec["form"][i],
                    "doc": rec["primaryDocument"][i], "desc": rec.get("primaryDocDescription", [""] * n)[i] or "",
                    "items": rec.get("items", [""] * n)[i] or "",
                    "accepted": rec.get("acceptanceDateTime", [""] * n)[i] or ""})
    return out


def doc_url(cik: int, acc: str, doc: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{doc}"


def index_url(cik: int, acc: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{acc}-index.htm"


# ---------------------------------------------------------------------------
# Form 4 — parsed, no model
# ---------------------------------------------------------------------------

def parse_form4(cik: int, acc: str, doc: str) -> dict | None:
    raw = doc.split("/")[-1]                  # strip the xsl viewer prefix
    r = _sec_get(doc_url(cik, acc, raw))
    if r.status_code != 200:
        return None
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        print(f"  [usfilings] form4 parse failed {acc}: {e}")
        return None

    def t(node, path):
        e = node.find(path)
        return e.text.strip() if e is not None and e.text else None

    rel = root.find("reportingOwner/reportingOwnerRelationship")
    role = None
    if rel is not None:
        if (t(rel, "officerTitle") or "").strip():
            role = t(rel, "officerTitle")
        elif t(rel, "isDirector") in ("1", "true"):
            role = "Director"
        elif t(rel, "isTenPercentOwner") in ("1", "true"):
            role = "10% owner"
    owner = t(root, "reportingOwner/reportingOwnerId/rptOwnerName")
    buys, sells, after = [], [], None
    for tx in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        code = t(tx, "transactionCoding/transactionCode")
        sh = t(tx, "transactionAmounts/transactionShares/value")
        px = t(tx, "transactionAmounts/transactionPricePerShare/value")
        ad = t(tx, "transactionAmounts/transactionAcquiredDisposedCode/value")
        after = t(tx, "postTransactionAmounts/sharesOwnedFollowingTransaction/value") or after
        try:
            sh, px = float(sh or 0), float(px or 0)
        except ValueError:
            continue
        if code == "P" or (code == "S" and ad == "D") :
            (buys if code == "P" else sells).append((sh, px))
        elif code in INSIDER_ROUTINE_CODES or not code:
            continue
        else:
            (buys if ad == "A" else sells).append((sh, px))
    return {"owner": owner, "role": role, "buys": buys, "sells": sells,
            "after": float(after) if after else None}


def _fmt_insider(sym: str, f: dict, link: str) -> str | None:
    def agg(lots):
        sh = sum(s for s, _ in lots)
        val = sum(s * p for s, p in lots)
        return sh, (val / sh if sh else 0), val
    parts = []
    if f["sells"]:
        sh, avg, val = agg(f["sells"])
        pct = f" ({sh / (f['after'] + sh) * 100:.0f}% of holding)" if f.get("after") is not None and (f["after"] + sh) else ""
        parts.append(f"🔴 <b>SOLD {sh:,.0f}</b> sh @ avg ${avg:,.2f} = <b>${val:,.0f}</b>{pct}")
    if f["buys"]:
        sh, avg, val = agg(f["buys"])
        parts.append(f"🟢 <b>BOUGHT {sh:,.0f}</b> sh @ avg ${avg:,.2f} = <b>${val:,.0f}</b>")
    if not parts:
        return None                           # awards / exercises only — routine
    who = html.escape(f.get("owner") or "insider")
    role = f" ({html.escape(f['role'])})" if f.get("role") else ""
    after = f"\nHolds {f['after']:,.0f} sh after" if f.get("after") is not None else ""
    return (f"👤 <b>{sym}</b> insider — {who}{role}\n" + "\n".join(parts) + after
            + f"\n<a href=\"{link}\">Form 4</a>")


# ---------------------------------------------------------------------------
# 8-K gist (Haiku, temperature 0)
# ---------------------------------------------------------------------------

def _doc_text(cik: int, acc: str, doc: str) -> str:
    r = _sec_get(doc_url(cik, acc, doc))
    if r.status_code != 200:
        return ""
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", r.text, flags=re.S | re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = html.unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt[:MAX_DOC_CHARS]


def gist(company: str, form: str, items_label: str, text: str) -> str:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key or not text:
        return ""
    try:
        from alerts import SUMMARY_MODEL as model
    except Exception:
        model = "claude-haiku-4-5-20251001"
    prompt = (f"This is the text of an SEC {form} filed by {company} ({items_label}). "
              "Write 2-3 short bullet points (max 45 words total) with the concrete facts an "
              "investor needs: what happened, amounts/percentages, dates, names. Then one line "
              "'Why it matters:' (max 20 words). Plain text, no markdown, no preamble.\n\n" + text)
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
                          headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                                   "content-type": "application/json"},
                          json={"model": model, "max_tokens": 300, "temperature": 0,
                                "messages": [{"role": "user", "content": prompt}]}, timeout=60)
        if r.status_code != 200:
            print(f"  [usfilings] gist HTTP {r.status_code}: {r.text[:120]}")
            return ""
        return "".join(b.get("text", "") for b in r.json().get("content", [])).strip()
    except Exception as e:
        print(f"  [usfilings] gist failed: {type(e).__name__}: {e}")
        return ""


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

def _fp(acc: str) -> str:
    return hashlib.sha256(f"us|{acc}".encode()).hexdigest()[:32]


def _seen(client) -> set:
    try:
        rows = (client.table("filings_seen").select("fingerprint")
                .order("id", desc=True).limit(1000).execute().data or [])
        return {r["fingerprint"] for r in rows}
    except Exception as e:
        print(f"  [usfilings] filings_seen load failed: {e}")
        return set()


def _dry() -> bool:
    return os.environ.get("ALERTS_DRY_RUN", "").strip().lower() in ("1", "true", "yes")


def run(client, send: bool = True) -> int:
    uni = universe(client)
    if not uni:
        print("  [usfilings] empty US universe")
        return 0
    seen = _seen(client)
    since = date.today() - timedelta(days=LOOKBACK_DAYS)
    msgs, to_log, gists = [], [], 0
    for sym, info in uni.items():
        cik, title = info["cik"], info["title"]
        tag = "📌 " if info["held"] else "👀 "
        for f in recent_filings(cik, since):
            fp = _fp(f["acc"])
            if fp in seen:
                continue
            form = f["form"]
            link = doc_url(cik, f["acc"], f["doc"]) if f["doc"] else index_url(cik, f["acc"])
            body = None
            if form in ROUTINE_FORMS:
                to_log.append((fp, sym, f"{form} (routine)", f["date"]))
                continue
            if form in ("4", "4/A"):
                parsed = parse_form4(cik, f["acc"], f["doc"])
                body = _fmt_insider(sym, parsed, link) if parsed else None
                if body is None:
                    to_log.append((fp, sym, f"{form} routine/award", f["date"]))
                    continue
                body = tag + body
            elif form in ("8-K", "8-K/A"):
                items = [i.strip() for i in f["items"].split(",") if i.strip()]
                labels = [ITEM_LABELS.get(i, i) for i in items if i != "9.01"] or ["8-K"]
                star = "⭐ " if any("⭐" in l for l in labels) else ""
                flag = "🚨 " if any("🚨" in l for l in labels) else ""
                head = f"{tag}{flag or star}<b>{html.escape(title)}</b> — 8-K: {html.escape(', '.join(labels))}"
                g = ""
                if gists < MAX_GISTS_PER_RUN:
                    g = gist(title, form, ", ".join(labels), _doc_text(cik, f["acc"], f["doc"]))
                    gists += 1 if g else 0
                body = head + (f"\n{html.escape(g)}" if g else "") + f"\n<a href=\"{link}\">filing</a> · {f['date']}"
            else:
                star = "⭐ " if form in STAR_FORMS else ""
                desc = f" — {html.escape(f['desc'])}" if f["desc"] and f["desc"].upper() != form else ""
                body = f"{tag}{star}<b>{html.escape(title)}</b> — {html.escape(form)}{desc}\n<a href=\"{link}\">filing</a> · {f['date']}"
            msgs.append(body)
            to_log.append((fp, sym, f"{form} {f['desc'] or f['items']}"[:280], f["date"]))
    print(f"  [usfilings] {len(uni)} names · {len(msgs)} new alert(s) · {len(to_log) - len(msgs)} routine skipped · {gists} gist(s)")
    if msgs and send:
        import notify
        ok = notify.send_telegram("🇺🇸 <b>US filings</b>\n\n" + "\n\n".join(msgs),
                                  chat_id=notify.chat_for_group("vishal_us"))
        if not ok:
            print("  [usfilings] Telegram send FAILED — nothing marked seen (will retry next run)")
            return 0
    elif msgs:
        print("\n".join("──\n" + m for m in msgs))
    if send and not _dry():
        for fp, sym, head, d in to_log:
            try:
                client.table("filings_seen").insert({"fingerprint": fp, "ticker": sym,
                                                     "headline": head, "filing_date": d}).execute()
            except Exception as e:
                print(f"  [usfilings] seen write failed {sym}: {e}")
    return len(msgs)


# ---------------------------------------------------------------------------
# earnings brief (Finnhub calendar)
# ---------------------------------------------------------------------------

def earnings_brief(client, send: bool = True, days: int = 7) -> int:
    import usprices
    key = usprices._finnhub_key()
    if not key:
        print("  [usfilings] FINNHUB_API_KEY missing — no earnings brief")
        return 0
    uni = universe(client)
    today = datetime.now(NY).date()
    rows = []
    for sym, info in uni.items():
        try:
            r = requests.get("https://finnhub.io/api/v1/calendar/earnings",
                             params={"from": today.isoformat(), "to": (today + timedelta(days=days)).isoformat(),
                                     "symbol": sym, "token": key}, timeout=15)
            for e in (r.json().get("earningsCalendar") or []):
                rows.append((e["date"], sym, info["held"], e.get("hour"), e.get("epsEstimate"), e.get("revenueEstimate")))
        except Exception as ex:
            print(f"  [usfilings] earnings {sym}: {type(ex).__name__}: {ex}")
        time.sleep(1.1)                       # 60/min free tier
    if not rows:
        print("  [usfilings] no earnings in the next 7 days")
        return 0
    rows.sort()
    fp = _fp(f"earnings|{today.isoformat()}")
    if fp in _seen(client):
        print("  [usfilings] earnings brief already sent today")
        return 0
    lines = []
    for d, sym, held, hour, eps, rev in rows:
        when = {"bmo": "before open", "amc": "after close", "dmh": "during market"}.get(hour, hour or "")
        dd = date.fromisoformat(d)
        day = "TODAY" if dd == today else dd.strftime("%a %d %b")
        est = f" · est EPS ${eps:,.2f}" if eps else ""
        rv = f", rev ${rev / 1e9:,.1f}B" if rev else ""
        lines.append(f"{'📌' if held else '👀'} <b>{sym}</b> — {day} {when}{est}{rv}")
    msg = "📅 <b>US earnings — next 7 days</b>\n\n" + "\n".join(lines)
    if send:
        import notify
        if not notify.send_telegram(msg, chat_id=notify.chat_for_group("vishal_us")):
            return 0
        if not _dry():
            try:
                client.table("filings_seen").insert({"fingerprint": fp, "ticker": "__us_earnings__",
                                                     "headline": f"earnings brief {today}", "filing_date": today.isoformat()}).execute()
            except Exception as e:
                print(f"  [usfilings] earnings marker failed: {e}")
    else:
        print(msg)
    return len(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import usprices
    mode = sys.argv[1] if len(sys.argv) > 1 else "check"
    c = usprices._client()
    if mode == "check":
        os.environ["ALERTS_DRY_RUN"] = "1"
        run(c, send=False)
    elif mode == "run":
        run(c, send=True)
    elif mode == "earnings":
        earnings_brief(c, send=True)
    elif mode == "audit":
        for sym, info in universe(c).items():
            fs = recent_filings(info["cik"], date.today() - timedelta(days=7))
            print(f"{sym:<6} {info['title'][:30]:<30} {len(fs)} filing(s) in 7d: "
                  + ", ".join(f"{f['form']}{'(routine)' if f['form'] in ROUTINE_FORMS else ''}" for f in fs))
    else:
        print(__doc__)
