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
import sys
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


def _actions_for(ticker: str) -> list[dict]:
    return [a for a in CORPORATE_ACTIONS if a["ticker"] == ticker]


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
    for a in CORPORATE_ACTIONS:
        ex = pd.Timestamp(a["ex_date"])
        f = float(a["price_divisor"])
        if f <= 0:
            continue
        mask = (out[ticker_col] == a["ticker"]) & (dates < ex)
        if not mask.any():
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


def scan_supabase(client) -> list[dict]:
    """Fetch RAW prices for every ticker in sme_daily_prices and return any
    unadjusted split/bonus signatures. Per-ticker query keeps each under the
    1000-row cap. Best-effort: DB hiccups return None rather than crash a caller
    (the daily job wires this in, and must never be broken by a diagnostic).

    The ticker list walks the table one ticker at a time (04-Nov-2026): a plain
    select("ticker") hit the silent 1000-row cap, so the scan saw only the 67
    tickers in the first 1000 of 65,000+ rows, while still reporting all clear."""
    try:
        tickers, last = [], None
        while True:
            q = client.table("sme_daily_prices").select("ticker").order("ticker").limit(1)
            r = (q.gt("ticker", last) if last else q).execute().data or []
            if not r:
                break
            last = r[0]["ticker"]
            tickers.append(last)
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


if __name__ == "__main__":
    # Diagnostic: run the detector against the live table. Read-only.
    #   python corporate_actions.py            -> scan for unadjusted gaps
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
    report(scan_supabase(client))
