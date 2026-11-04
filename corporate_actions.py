"""
corporate_actions.py — split/bonus adjustment for bhavcopy-sourced prices.

THE PROBLEM THIS SOLVES (root-caused 21-Jul-2026):
bhavcopy.py stores RAW traded prices with no corporate-action adjustment.
When a stock does a split or bonus, its price steps down overnight (e.g. CWD's
4:1 bonus on 02-Jan-2026: ₹1970 -> ₹415). The OLD high prices then sit in
sme_daily_prices next to the NEW low prices, so the weekly EMA-40 averages two
different price scales and lands far too high -- which fired a FALSE 🔴 EXIT on
CWD while it had not actually broken its 40-week EMA at all.

THE FIX (two layers, matching the house rules):
  1. ADJUST (this file's `adjust_prices`): keep the raw prices untouched in the
     DB (they are the authoritative exchange record), and divide the pre-ex-date
     prices down to the post-action scale ON READ. Applied at the single
     chokepoint db.get_sme_daily_prices, so holdings / signals / fundamentals /
     exit-audit all see corrected data automatically. Reversible + auditable.
  2. DETECT (`find_unadjusted_gaps`): scan for the >25% overnight step that is
     the signature of a split/bonus. Any such step WITHOUT a matching entry
     below is an unadjusted corporate action -- flagged loudly so a wrong signal
     can never again pass silently (house rule #2: a wrong number is worse than
     a blank one; #3: every failure says WHY).

TO ADD A NEW EVENT: append one dict to CORPORATE_ACTIONS. Confirm the exact
ratio against the BSE/NSE corporate-action filing first (house rule #5 --
verify against the real source, not a web article), then the person who held
the stock can sanity-check it against the bonus/split they actually received.

`price_divisor` = how many times to DIVIDE pre-ex-date prices:
  - 4:1 bonus  (4 new shares per 1 held) -> 5x shares -> divide by 5
  - 1:1 bonus  (1 new per 1 held)        -> 2x shares -> divide by 2
  - 1:5 split  (face value /5)           -> 5x shares -> divide by 5
  - 1:2 split  (face value /2)           -> 2x shares -> divide by 2
Volume (traded shares) is MULTIPLIED by the same factor for the older rows so
the volume-spike comparison stays on one consistent share scale.
"""
from __future__ import annotations
import json
import math
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# Findings carry ⚠️/emoji and exist to explain a failure; under Windows cp1252 the
# print itself raised UnicodeEncodeError and the detector died BEFORE listing the
# unadjusted tickers it had found (25-Sep-2026). Same fix as alerts.py/bhavcopy.py.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# The registry. `ticker` MUST match the string stored in sme_daily_prices.
CORPORATE_ACTIONS = [
    {
        "ticker": "CWD-MS.BO",
        "ex_date": "2026-01-02",     # ex-date == record date, per BSE filing
        "action": "4:1 bonus",       # 4 new fully-paid shares per 1 held
        "price_divisor": 5.0,        # 5x shares outstanding -> prices /5
        "note": "BSE 543378. Allotted 1,75,79,824 bonus shares. Face value ₹10 "
                "unchanged (bonus, not a face-value split). Verified 21-Jul-2026 "
                "against BSE corporate-action filing + multiple sources.",
    },
    {
        "ticker": "532856.BO",
        "ex_date": "2025-09-23",     # record date per BSE filing; price stepped this day
        "action": "1:1 bonus",       # 1 new fully-paid share per 1 held
        "price_divisor": 2.0,        # 2x shares outstanding -> prices /2
        "note": "BSE 532856 / NSE TIMETECHNO. Time Technoplast's first bonus, "
                "22,69,29,066 shares, face value ₹1. Verified 06-Aug-2026 against "
                "multiple sources AND our own raw signature ₹477.75->₹227.35 on "
                "23-Sep-2025. Unadjusted, this inflated the 40wEMA and fired a FALSE "
                "EXIT (close ₹207.3 vs a bloated 40wEMA ₹211.1).",
    },
    {
        "ticker": "PGIL.NS",
        "ex_date": "2026-09-11",     # price stepped this day in NSE's own file
        "action": "1:1 bonus",       # 1 new fully-paid share per 1 held
        "price_divisor": 2.0,        # 2x shares outstanding -> prices /2
        "note": "Pearl Global Industries. Caught 11-Sep-2026 the same evening via "
                "an INDmoney reconciliation: broker value exceeded the dashboard "
                "by EXACTLY one 65-share PGIL position — the broker had credited "
                "bonus shares. Raw NSE signature ₹2,378.40 (10-Sep) -> ₹1,187.70 "
                "(11-Sep), ratio 2.00. Unadjusted it already fired FALSE ADD21 + "
                "W10EMA alerts at 09:10 that morning (halved price 'arrived' at "
                "the unadjusted DMAs). Held by Vishal (65->130) and Lakshmi "
                "(1,700->3,400); both holdings rows corrected same day.",
    },
    {
        "ticker": "E2E.NS",
        "ex_date": "2026-06-05",     # price stepped this day in NSE's own file
        "action": "9:1 bonus",       # 9 new fully-paid shares per 1 held
        "price_divisor": 10.0,       # 10x shares outstanding -> prices /10
        "note": "E2E Networks. Caught 25-Sep-2026 by Vishal: the flowchart said EXIT "
                "while price sat ABOVE its 50-day EMA. Raw NSE signature ₹4,313.60 "
                "(04-Jun) -> ₹452.90 (05-Jun); Lakshmi's Kite export shows 50 shares "
                "held + 450 bonus credited 03-Jun (9 per 1 held). Unadjusted, the "
                "40wEMA read ₹1,468 against a ₹652 close -> FALSE EXIT for months. "
                "The detector HAD flagged it nightly, but its Telegram goes only to "
                "Lakshmi's group and its local run crashed on cp1252 before printing.",
    },
    {
        "ticker": "524632.BO",
        "ex_date": "2025-03-21",     # price stepped this day in BSE's own file
        "action": "1:10 split",      # face value ₹10 -> ₹1
        "price_divisor": 10.0,       # 10x shares outstanding -> prices /10
        "note": "Shukra Pharmaceuticals (BSE 524632; Vishal holds it as XBOM:524632, "
                "Lakshmi as XBOM:SHUKRAPHAR — both tickers carry the same raw series). "
                "Raw BSE step ₹236.00 (20-Mar-2025) -> ₹24.05 (21-Mar); record/ex-date "
                "21-Mar-2025, FV ₹10 -> ₹1, new ISIN INE551C01044 (Business Standard, "
                "Goodreturns, Trendlyne). Flagged by the gap detector 18-Oct-2026 after "
                "the fixed backfill run; it had sat unregistered since the 02-Sep backfill.",
    },
    {
        "ticker": "SHUKRAPHAR.BO",
        "ex_date": "2025-03-21",     # price stepped this day in BSE's own file
        "action": "1:10 split",      # face value ₹10 -> ₹1
        "price_divisor": 10.0,       # 10x shares outstanding -> prices /10
        "note": "Shukra Pharmaceuticals (BSE 524632; Vishal holds it as XBOM:524632, "
                "Lakshmi as XBOM:SHUKRAPHAR — both tickers carry the same raw series). "
                "Raw BSE step ₹236.00 (20-Mar-2025) -> ₹24.05 (21-Mar); record/ex-date "
                "21-Mar-2025, FV ₹10 -> ₹1, new ISIN INE551C01044 (Business Standard, "
                "Goodreturns, Trendlyne). Flagged by the gap detector 18-Oct-2026 after "
                "the fixed backfill run; it had sat unregistered since the 02-Sep backfill.",
    },
    # The next five were invisible until the scan covered every ticker (04-Nov-2026).
    # Each ratio is from NSE's own corporate-action record (data/corp_actions.json)
    # and matches the raw step in NSE's file.
    {
        "ticker": "KOTAKBANK.NS",
        "ex_date": "2026-01-14",
        "action": "1:5 split (FV ₹5 → ₹1)",
        "price_divisor": 5.0,
        "note": "₹2,132.60 → ₹421.00. Unadjusted it put the 40wEMA at ₹605 against a ₹459 "
                "close: a FALSE EXIT on Lakshmi's watchlist, added 02-Nov-2026.",
    },
    {
        "ticker": "COFORGE.NS",
        "ex_date": "2025-06-04",
        "action": "1:5 split (FV ₹10 → ₹2)",
        "price_divisor": 5.0,
        "note": "₹8,499.00 → ₹1,724.50. Lakshmi holds and watches it.",
    },
    {
        "ticker": "GLOBAL.NS",
        "ex_date": "2024-12-10",
        "action": "2:5 split (FV ₹5 → ₹2)",
        "price_divisor": 2.5,
        "note": "₹204.52 → ₹86.04. Lakshmi holds it.",
    },
    {
        "ticker": "JTLIND.NS",
        "ex_date": "2024-11-14",
        "action": "1:2 split (FV ₹2 → ₹1)",
        "price_divisor": 2.0,
        "note": "₹183.79 → ₹98.15. On Lakshmi's watchlist.",
    },
    {
        "ticker": "BECTORFOOD.NS",
        "ex_date": "2025-12-12",
        "action": "1:5 split (FV ₹10 → ₹2)",
        "price_divisor": 5.0,
        "note": "₹1,310.30 → ₹271.20. Not held or watched today; its rows are still stored.",
    },
]

# Big steps that are checked and are NOT a share-count change, so there is no ratio to
# register. Listed so they stop being re-flagged every night and burying a real finding.
EXPLAINED_STEPS = {
    ("STAR.NS", "2024-12-06"): "OneSource demerger (record date 06-Dec-2024, 1 for 2): value moved to a new company",
    ("STLTECH.NS", "2025-04-24"): "STL Networks demerger (record date 24-Apr-2025, 1 for 1): value moved to a new company",
    ("NBIS", "2025-01-27"): "real move: no split on record",
    ("NBIS", "2025-09-09"): "real move: no split on record",
    ("NBIS", "2026-07-30"): "real move: no split on record",
    ("NBIS", "2026-08-12"): "real move: no split on record",
    ("RKLB", "2024-11-13"): "real move: no split on record",
    ("RKLB", "2025-01-21"): "real move: no split on record",
    ("RKLB", "2026-05-08"): "real move: no split on record",
}

GAP_THRESHOLD_PCT = 25.0   # a >25% overnight close move == probable split/bonus
_EX_DATE_TOLERANCE_DAYS = 3  # data date vs official ex-date can differ slightly
_MAX_OVERNIGHT_DAYS = 7    # rows further apart than this aren't an overnight step
                           # (a sold stock's stored rows can be months apart)

# ---------------------------------------------------------------------------
# AUTOMATIC RECORD (04-Nov-2026, Vishal: "we just need the right data ... you figure
# out what is needed"). The registry above needed a person to notice a gap, and a
# 1:4 bonus only steps the price 20%, so the detector could never see it. `sync`
# refreshes this file from the exchanges' own records, future ex-dates included;
# a GitHub job commits it, and adjust_prices / apply_due read it from there.
# ---------------------------------------------------------------------------
AUTO_FILE = Path(__file__).with_name("data") / "corp_actions_auto.json"
AUTO_CREDIT_FROM = "2026-11-05"   # apply_due leaves older events alone (all audited OK)
_STEP_TOLERANCE = 1.6             # raw step vs factor, same tolerance as scanner.adjust
_warned: set = set()


def _today_ist() -> date:
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).date()


def _load_auto() -> list[dict]:
    """The file as registry-shaped dicts, one per (ticker, ex-date); same-day
    actions multiply (a split and a bonus on one date). A hand-verified entry in
    CORPORATE_ACTIONS wins over anything within a week of it."""
    try:
        rows = json.load(open(AUTO_FILE, encoding="utf-8"))
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"  ⚠️ [corp-actions] {AUTO_FILE.name} unreadable: {type(e).__name__}: {e}")
        return []
    merged: dict = {}
    for r in rows:
        k = (r["ticker"], r["ex"])
        m = merged.setdefault(k, {"ticker": r["ticker"], "ex_date": r["ex"], "price_divisor": 1.0,
                                  "action": r["subject"], "note": r.get("source", ""), "auto": True})
        if m["price_divisor"] != 1.0:
            m["action"] += " + " + r["subject"]
        m["price_divisor"] *= float(r["factor"])
    hand = [(a["ticker"], pd.Timestamp(a["ex_date"])) for a in CORPORATE_ACTIONS]
    return [a for a in merged.values()
            if not any(t == a["ticker"] and abs((x - pd.Timestamp(a["ex_date"])).days) <= 7 for t, x in hand)]


AUTO_ACTIONS = _load_auto()


def _all_actions() -> list[dict]:
    return CORPORATE_ACTIONS + AUTO_ACTIONS


def _actions_for(ticker: str) -> list[dict]:
    return [a for a in _all_actions() if a["ticker"] == ticker]


def _step_fits(df: pd.DataFrame, dates: pd.Series, a: dict, ticker_col: str) -> bool:
    """An automatic event must match the real price step once rows exist on both
    sides of it, so a wrong or cancelled record can't divide good prices. With no
    rows after the ex-date yet (ex-date morning) the record is all there is."""
    if "close" not in df.columns:
        return True
    ex = pd.Timestamp(a["ex_date"])
    mine = df[ticker_col] == a["ticker"]
    before, after = df.loc[mine & (dates < ex)], df.loc[mine & (dates >= ex)]
    if before.empty or after.empty:
        return True
    d0, d1 = dates[before.index].idxmax(), dates[after.index].idxmin()
    if (dates[d1] - dates[d0]).days > 10:
        return True                 # a hole in our data: no step to compare
    c0, c1 = float(df.loc[d0, "close"]), float(df.loc[d1, "close"])
    f = float(a["price_divisor"])
    fits = c1 > 0 and f / _STEP_TOLERANCE <= c0 / c1 <= f * _STEP_TOLERANCE
    if not fits and (a["ticker"], a["ex_date"]) not in _warned:
        _warned.add((a["ticker"], a["ex_date"]))
        print(f"  ⚠️ [corp-actions] {a['ticker']} {a['ex_date']}: record says ÷{f:g} but the price "
              f"went {c0:g} → {c1:g} — NOT applied")
    return fits


def adjust_prices(df: pd.DataFrame,
                  ticker_col: str = "ticker",
                  date_col: str = "price_date") -> pd.DataFrame:
    """Return a COPY of `df` with pre-ex-date OHLC divided (and volume
    multiplied) by each registered action's factor. Rows on/after an ex-date
    are left exactly as stored. A ticker with no registered action is returned
    unchanged. Safe on empty frames and missing columns."""
    if df is None or df.empty or ticker_col not in df.columns or date_col not in df.columns:
        return df
    out = df.copy()
    dates = pd.to_datetime(out[date_col])
    price_cols = [c for c in ("open", "high", "low", "close") if c in out.columns]
    has_vol = "volume" in out.columns
    today = pd.Timestamp(_today_ist())
    present = set(out[ticker_col])
    for a in _all_actions():
        if a["ticker"] not in present:
            continue
        ex = pd.Timestamp(a["ex_date"])
        f = float(a["price_divisor"])
        if f <= 0 or ex > today:      # a known future ex-date: nothing changes until it arrives
            continue
        mask = (out[ticker_col] == a["ticker"]) & (dates < ex)
        if not mask.any():
            continue
        if a.get("auto") and not _step_fits(df, dates, a, ticker_col):
            continue
        for c in price_cols:
            out.loc[mask, c] = out.loc[mask, c] / f
        if has_vol:
            out.loc[mask, "volume"] = out.loc[mask, "volume"] * f
    return out


def _is_explained(ticker: str, gap_date: pd.Timestamp) -> bool:
    """True if a >25% step on this date is accounted for by a registered
    action (within a few days of its ex-date) or a checked EXPLAINED_STEPS entry."""
    if (ticker, gap_date.date().isoformat()) in EXPLAINED_STEPS:
        return True
    for a in _actions_for(ticker):
        if abs((gap_date - pd.Timestamp(a["ex_date"])).days) <= _EX_DATE_TOLERANCE_DAYS:
            return True
    return False


def find_unadjusted_gaps(df: pd.DataFrame,
                         ticker_col: str = "ticker",
                         date_col: str = "price_date",
                         threshold: float = GAP_THRESHOLD_PCT) -> list[dict]:
    """Scan RAW (unadjusted) daily prices for >threshold% overnight close moves
    that have NO matching registry entry -- i.e. a split/bonus we haven't
    accounted for yet. Returns a list of finding dicts (empty == all clear)."""
    findings: list[dict] = []
    if df is None or df.empty or ticker_col not in df.columns or "close" not in df.columns:
        return findings
    work = df.copy()
    work[date_col] = pd.to_datetime(work[date_col])
    for ticker, g in work.groupby(ticker_col):
        g = g.sort_values(date_col).reset_index(drop=True)
        pct = g["close"].pct_change() * 100
        for i in g.index[pct.abs() > threshold]:
            gap_date = g[date_col].iloc[i]
            if (gap_date - g[date_col].iloc[i - 1]).days > _MAX_OVERNIGHT_DAYS:
                continue
            if _is_explained(ticker, gap_date):
                continue
            prev_close = g["close"].iloc[i - 1] if i > 0 else float("nan")
            cur_close = g["close"].iloc[i]
            findings.append({
                "ticker": ticker,
                "date": gap_date.date().isoformat(),
                "prev_close": round(float(prev_close), 2),
                "close": round(float(cur_close), 2),
                "pct": round(float(pct.iloc[i]), 1),
                "implied_factor": round(float(prev_close / cur_close), 2) if cur_close else None,
            })
    return findings


def _stored_tickers(client) -> list[str]:
    tickers, last = [], None
    while True:
        q = client.table("sme_daily_prices").select("ticker").order("ticker").limit(1)
        r = (q.gt("ticker", last) if last else q).execute().data or []
        if not r:
            return tickers
        last = r[0]["ticker"]
        tickers.append(last)


def scan_supabase(client) -> list[dict]:
    """Fetch RAW prices for every ticker in sme_daily_prices and return any
    unadjusted split/bonus signatures. Per-ticker query keeps each under the
    1000-row cap. Best-effort: DB hiccups return None rather than crash a caller
    (the daily job wires this in, and must never be broken by a diagnostic).

    The ticker list walks the table one ticker at a time (04-Nov-2026): a plain
    select("ticker") hit the silent 1000-row cap, so the scan saw only the 67
    tickers in the first 1000 of 65,000+ rows, while still reporting all clear."""
    try:
        tickers = _stored_tickers(client)
        findings = []
        for t in tickers:
            rows = (client.table("sme_daily_prices").select("ticker, price_date, close")
                    .eq("ticker", t).order("price_date", desc=True).execute().data or [])
            if rows:
                findings.extend(find_unadjusted_gaps(pd.DataFrame(rows)))
        print(f"  [corp-actions] scanned {len(tickers)} tickers")
        return findings
    except Exception as e:
        print(f"  [corp-actions] scan failed: {type(e).__name__}: {e}")
        return None


def report(findings: list[dict] | None, n_tickers: int | None = None) -> None:
    """Print a scan result the same way for the CLI and the daily-job hook.
    None means the scan didn't run, which must never read as all clear."""
    if findings is None:
        print("[corp-actions scan] ⚠️ did not complete — result unknown, NOT all clear")
        return
    header = "[corp-actions scan]"
    if n_tickers is not None:
        header += f" {n_tickers} tickers checked,"
    print(f"{header} threshold {GAP_THRESHOLD_PCT}%")
    if not findings:
        print("  ✅ all clear — no unadjusted split/bonus signatures")
        return
    print(f"  ⚠️ {len(findings)} UNADJUSTED corporate-action signature(s) — "
          f"add each to corporate_actions.CORPORATE_ACTIONS after verifying the ratio:")
    for f in findings:
        print(f"    {f['ticker']}  {f['date']}: {f['prev_close']} -> {f['close']} "
              f"({f['pct']:+}%)  implied ~{f['implied_factor']}x")


# ---------------------------------------------------------------------------
# keeping the automatic record current, and acting on it
# ---------------------------------------------------------------------------

def parse_factor(subject: str):
    """Shares multiplier from NSE/BSE wording, or None if it isn't a split/bonus.
    'From Rs 10/- ... To Rs 2/-' -> 5 · 'Bonus 2:1' -> 3 · both in one line multiply."""
    subject = str(subject)
    f = 1.0
    m = re.search(r"from\s+r[se]\.?\s*([\d.]+).*?to\s+r[se]\.?\s*([\d.]+)", subject, re.I)
    if m and re.search(r"split|sub-?div|consolidat", subject, re.I) and float(m.group(2)) > 0:
        f *= float(m.group(1)) / float(m.group(2))
    m = re.search(r"bonus\D*(\d+)\s*:\s*(\d+)", subject, re.I)
    if m and int(m.group(2)) > 0:
        f *= (int(m.group(1)) + int(m.group(2))) / int(m.group(2))
    return round(f, 6) if f != 1.0 else None


def _fetch_bse(code: str, frm: date, to: date) -> list[tuple]:
    import requests
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0 Safari/537.36",
         "Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com",
         "Accept": "application/json"}
    r = requests.get("https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w",
                     params={"Fdate": f"{frm:%Y%m%d}", "TDate": f"{to:%Y%m%d}", "Purposecode": "",
                             "ddlcategorys": "E", "ddlindustrys": "", "scripcode": code,
                             "segment": "0", "strSearch": "S"}, headers=h, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}, {len(r.content)} bytes, starts {r.text[:80]!r}")
    out = []
    for x in r.json() or []:
        f = parse_factor(x.get("Purpose"))
        ex = str(x.get("exdate") or "")
        if f and len(ex) == 8:
            out.append((f"{ex[:4]}-{ex[4:6]}-{ex[6:]}", f, " ".join(str(x["Purpose"]).split())))
    return out


def _yahoo_splits(ticker: str) -> list[tuple]:
    import yfinance as yf
    sp = yf.Ticker(ticker).splits
    return [] if sp is None else [(k.strftime("%Y-%m-%d"), float(v), f"Split x{float(v):g} (Yahoo)") for k, v in sp.items()]


def _raw_step_fits(client, ticker: str, ex: str, f: float) -> bool:
    b = (client.table("sme_daily_prices").select("price_date,close").eq("ticker", ticker)
         .lt("price_date", ex).order("price_date", desc=True).limit(1).execute().data or [])
    a = (client.table("sme_daily_prices").select("price_date,close").eq("ticker", ticker)
         .gte("price_date", ex).order("price_date").limit(1).execute().data or [])
    if not a or not b or not float(a[0]["close"]):
        return False
    return f / _STEP_TOLERANCE <= float(b[0]["close"]) / float(a[0]["close"]) <= f * _STEP_TOLERANCE


def _write_auto(rows: list[dict]) -> None:
    global AUTO_ACTIONS
    AUTO_FILE.parent.mkdir(exist_ok=True)
    json.dump(sorted(rows, key=lambda e: (e["ex"], e["ticker"], e["subject"])),
              open(AUTO_FILE, "w", encoding="utf-8", newline="\n"), ensure_ascii=False, indent=0)
    AUTO_ACTIONS = _load_auto()


def sync(client, frm: date | None = None) -> tuple[bool, list[str]]:
    """Refresh AUTO_FILE for every stored ticker. Returns (file changed, sources that
    failed). A failed source keeps what the file already has and is reported, never
    silently read as 'no events'. NSE lists ex-dates in advance, so a normal run
    knows about a split days before it happens."""
    today = _today_ist()
    to = today + timedelta(days=120)
    frm = frm or today - timedelta(days=90)
    have = json.load(open(AUTO_FILE, encoding="utf-8")) if AUTO_FILE.exists() else []
    tickers = _stored_tickers(client)
    found, failed = [], []

    nse = {t[:-3]: t for t in tickers if t.endswith(".NS")}
    try:
        import scanner
        for e in scanner._fetch_corporate_actions(frm, to):
            if e["symbol"] in nse:
                found.append({"ticker": nse[e["symbol"]], "ex": e["ex"], "factor": e["factor"],
                              "subject": " ".join(e["subject"].split()), "source": "NSE"})
    except Exception as e:
        failed.append(f"NSE: {type(e).__name__}: {str(e)[:120]}")

    import bhavcopy
    for t in (t for t in tickers if t.endswith(".BO")):
        code = t[:-3] if t[:-3].isdigit() else (bhavcopy.SME_STOCKS.get(t) or {}).get("scrip_code")
        if not code:
            failed.append(f"BSE {t}: no scrip code")
            continue
        try:
            found += [{"ticker": t, "ex": ex, "factor": f, "subject": s, "source": "BSE"}
                      for ex, f, s in _fetch_bse(str(code), frm, to)]
        except Exception as e:
            failed.append(f"BSE {t}: {type(e).__name__}: {str(e)[:120]}")

    # US bars are stored split-adjusted, so a split only needs registering when the
    # stored rows really do step; otherwise adjusting would divide twice.
    for t in (t for t in tickers if "." not in t and not t.startswith("^")):
        try:
            found += [{"ticker": t, "ex": ex, "factor": f, "subject": s, "source": "Yahoo"}
                      for ex, f, s in _yahoo_splits(t)
                      if ex >= frm.isoformat() and _raw_step_fits(client, t, ex, f)]
        except Exception as e:
            failed.append(f"Yahoo {t}: {type(e).__name__}: {str(e)[:120]}")

    # Keep an event only if it can touch stored rows; drop what a person already registered.
    first: dict = {}
    def _relevant(e) -> bool:
        if any(a["ticker"] == e["ticker"] and abs((pd.Timestamp(a["ex_date"]) - pd.Timestamp(e["ex"])).days) <= 7
               for a in CORPORATE_ACTIONS):
            return False
        if e["ticker"] not in first:
            r = (client.table("sme_daily_prices").select("price_date").eq("ticker", e["ticker"])
                 .order("price_date").limit(1).execute().data or [])
            first[e["ticker"]] = r[0]["price_date"] if r else "9999"
        return first[e["ticker"]] < e["ex"]

    merged = {(e["ticker"], e["ex"], e["subject"].lower()): e for e in have}
    for e in found:
        k = (e["ticker"], e["ex"], e["subject"].lower())
        if k not in merged and _relevant(e):
            merged[k] = e
    if len(merged) != len(have):
        _write_auto(list(merged.values()))

    # Last net: a fresh crash no record explains. Yahoo also lists splits and bonuses.
    recent = (today - timedelta(days=30)).isoformat()
    for g in (scan_supabase(client) or []):
        if g["pct"] < 0 and g["date"] >= recent:
            try:
                hit = [x for x in _yahoo_splits(g["ticker"]) if abs((pd.Timestamp(x[0]) - pd.Timestamp(g["date"])).days) <= 5]
            except Exception as e:
                print(f"  [corp-actions] Yahoo check for {g['ticker']} failed: {type(e).__name__}")
                continue
            if hit and _raw_step_fits(client, g["ticker"], hit[0][0], hit[0][1]):
                merged[(g["ticker"], hit[0][0], hit[0][2].lower())] = {
                    "ticker": g["ticker"], "ex": hit[0][0], "factor": hit[0][1], "subject": hit[0][2], "source": "Yahoo"}
                _write_auto(list(merged.values()))
    changed = len(merged) != len(have)
    print(f"[corp-actions sync] {len(tickers)} tickers, {len(found)} events seen, "
          f"{len(merged)} on file, changed={changed}, failed={failed or 'none'}")
    return changed, failed


def _ticker_of(stock_name: str):
    m = re.search(r"\((X[A-Z]+):([^)]+)\)", str(stock_name))
    if not m:
        return None
    sym = m.group(2).strip()
    return f"{sym}.NS" if m.group(1) == "XNSE" else f"{sym}.BO" if m.group(1) == "XBOM" else sym


def _lot_note(a: dict, f: float, ex: str) -> str:
    """db.fifo_lots reads 'Auto split x<f>': a split rescales the older lots, whereas
    bonus shares stay a separate zero-cost lot."""
    what = a.get("action", "")
    is_split = re.search(r"split|sub-?div", what, re.I) and not re.search(r"bonus", what, re.I)
    return f"Auto split x{f:g}: {what} (ex {ex})" if is_split else f"Auto bonus: {what} (ex {ex})"


def apply_due(client, today: date | None = None, dry_run: bool = False, actions: list | None = None) -> list[str]:
    """On the ex-date, before the market opens, bring OUR records onto the new share
    scale. Without this a 1:5 split reads as an 80% loss on the holding and fires the
    10%-below-cost stop, and a watchlist target above the new price fires at once.

    HOLDING: a price-0 lot for the new shares (same convention as the PGIL bonus, and
    what fy_tracker's rebuild from raw closes needs), quantity up, average down, money
    invested unchanged. WATCHLIST: target and
    support levels divided. Idempotent: a price-0 lot near the ex-date (ours or typed
    by hand) or the marker in the watchlist note means it's done. A consolidation
    (fewer shares) or a position it can't size safely is logged and left alone."""
    today = today or _today_ist()
    done: list[str] = []
    due = [a for a in (actions if actions is not None else _all_actions())
           if AUTO_CREDIT_FROM <= a["ex_date"] <= today.isoformat()]
    if not due:
        return done
    holdings = client.table("holdings").select("*").execute().data or []
    watch = client.table("watchlist").select("*").execute().data or []
    for a in due:
        f, ex = float(a["price_divisor"]), a["ex_date"]
        rows = [h for h in holdings if _ticker_of(h["stock_name"]) == a["ticker"]]
        if rows and f < 1:
            print(f"⚠️ [corp-actions] {a['ticker']} consolidation ÷{f:g}: holdings not changed automatically")
            rows = []
        for pf in sorted({h["portfolio_id"] for h in rows}):
            mine = [h for h in rows if h["portfolio_id"] == pf]
            sym = re.search(r":([^)]+)\)", mine[0]["stock_name"]).group(1).strip()
            tx = (client.table("transactions").select("transaction_type,quantity,price,transaction_date")
                  .eq("portfolio_id", pf).ilike("stock_name", f"%:{sym})%").execute().data or [])
            lo = (pd.Timestamp(ex) - timedelta(days=7)).date().isoformat()
            hi = (pd.Timestamp(ex) + timedelta(days=60)).date().isoformat()
            if any(float(t["price"] or 0) == 0 and t["transaction_type"] == "buy"
                   and lo <= str(t["transaction_date"])[:10] <= hi for t in tx):
                continue
            since = [t for t in tx if str(t["transaction_date"])[:10] >= ex]
            if since and len(mine) > 1:
                print(f"⚠️ [corp-actions] {a['ticker']} pf{pf}: traded since ex-date across {len(mine)} rows — not credited")
                continue
            for h in mine:
                held = float(h["quantity"] or 0)
                net = sum(float(t["quantity"]) * (1 if t["transaction_type"] == "buy" else -1) for t in since)
                q = held - net
                add = q * (f - 1)
                add = round(add, 6) if a["ticker"][-3:] not in (".NS", ".BO") else float(math.floor(add + 1e-6))
                if q <= 0 or add <= 0:
                    continue
                cost = float(h.get("purchase_cost") or 0)
                invested = float(h.get("amount_invested") or held * cost)
                new_q = held + add
                done.append(f"{a['ticker']} pf{pf}: {held:g} + {add:g} = {new_q:g} shares, avg {cost:,.2f} → {invested / new_q:,.2f}")
                print(f"[corp-actions] {done[-1]}")
                if dry_run:
                    continue
                client.table("holdings").update({"quantity": float(new_q), "purchase_cost": round(invested / new_q, 4),
                                                 "amount_invested": float(invested)}).eq("id", h["id"]).execute()
                try:
                    client.table("transactions").insert({
                        "stock_name": h["stock_name"], "transaction_type": "buy", "quantity": float(add), "price": 0.0,
                        "amount": 0.0, "transaction_date": ex, "notes": _lot_note(a, f, ex),
                        "holding_id": h["id"], "portfolio_id": pf}).execute()
                except Exception:
                    # no lot means no idempotency marker: undo, so the next run credits once, not twice
                    client.table("holdings").update({"quantity": held, "purchase_cost": cost,
                                                     "amount_invested": float(invested)}).eq("id", h["id"]).execute()
                    raise
        for w in (w for w in watch if _ticker_of(w["stock_name"]) == a["ticker"]):
            mark = f"[auto ÷{f:g} ex {ex}]"
            notes = str(w.get("notes") or "")
            if mark in notes or str(w.get("date_added") or "")[:10] >= ex:
                continue
            levels = {k: round(float(w[k]) / f, 2) for k in ("target_buy_price", "support_minor", "support_major") if w.get(k)}
            if not levels:
                continue
            done.append(f"{a['ticker']} watchlist pf{w['portfolio_id']}: {levels}")
            print(f"[corp-actions] {done[-1]}")
            if not dry_run:
                client.table("watchlist").update({**levels, "notes": (notes + " " + mark).strip()}).eq("id", w["id"]).execute()
    return done


def recent_unexplained(client, ticker: str, days: int = 5):
    """A raw one-session fall over the threshold in the last few days that no record
    explains. It looks like an unregistered split, so the alert engine holds stop and
    state alerts on the name until sync resolves it or the window passes. A wrong EXIT
    is worse than one that is a few days late. Never raises."""
    try:
        rows = (client.table("sme_daily_prices").select("ticker,price_date,close").eq("ticker", ticker)
                .order("price_date", desc=True).limit(12).execute().data or [])
        since = (_today_ist() - timedelta(days=days)).isoformat()
        hits = [g for g in find_unadjusted_gaps(pd.DataFrame(rows)) if g["pct"] < 0 and g["date"] >= since] if rows else []
        return hits[-1] if hits else None
    except Exception as e:
        print(f"  [corp-actions] gap check failed for {ticker}: {type(e).__name__}")
        return None


def restate_to_sale_scale(ticker: str, price: float, sold_on: date, priced_on: date, restated: bool) -> float:
    """A later price on the same share scale as a sale: a bonus after the sale cuts
    the price without the seller having saved anything. `restated` = the series is
    already on today's scale (Yahoo), so every event since the sale counts; a raw
    series only needs the events up to the price's own date."""
    upto = _today_ist() if restated else priced_on
    f = 1.0
    for a in _actions_for(ticker):
        if sold_on < pd.Timestamp(a["ex_date"]).date() <= upto:
            f *= float(a["price_divisor"])
    return price * f


if __name__ == "__main__":
    # Diagnostic: run the detector against the live table. Read-only.
    #   python corporate_actions.py            -> scan for unadjusted gaps
    #   python corporate_actions.py sync [full] -> refresh the record, apply what's due (WRITES)
    #   python corporate_actions.py due        -> what apply_due would change today, no writes
    import os
    import sys
    from supabase import create_client

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not (url and key):
        # local run: fall back to the Streamlit secrets file
        try:
            import tomllib
            from pathlib import Path
            s = tomllib.load(open(Path(__file__).with_name(".streamlit") / "secrets.toml", "rb"))
            url, key = s["SUPABASE_URL"], s["SUPABASE_SERVICE_KEY"]
        except Exception as e:
            print(f"No Supabase creds (env or .streamlit/secrets.toml): {e}")
            sys.exit(1)
    client = create_client(url, key)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if cmd == "sync":
        _, bad = sync(client, date(2024, 6, 1) if "full" in sys.argv else None)
        apply_due(client)
        report(scan_supabase(client))
        sys.exit(1 if bad else 0)     # a red run emails the owner; nothing else needs a person
    elif cmd == "due":
        print(apply_due(client, dry_run=True) or "nothing due")
    else:
        report(scan_supabase(client))
