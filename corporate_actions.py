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
import pandas as pd

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
]

GAP_THRESHOLD_PCT = 25.0   # a >25% overnight close move == probable split/bonus
_EX_DATE_TOLERANCE_DAYS = 3  # data date vs official ex-date can differ slightly


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
    action (within a few days of its ex-date)."""
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
    1000-row cap. Best-effort: DB hiccups return [] rather than crash a caller
    (the daily job wires this in, and must never be broken by a diagnostic)."""
    try:
        tickers = sorted({r["ticker"] for r in
                          (client.table("sme_daily_prices").select("ticker").execute().data or [])})
        frames = []
        for t in tickers:
            rows = (client.table("sme_daily_prices").select("ticker, price_date, close")
                    .eq("ticker", t).order("price_date", desc=True).execute().data or [])
            if rows:
                frames.append(pd.DataFrame(rows))
        raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return find_unadjusted_gaps(raw)
    except Exception as e:
        print(f"  [corp-actions] scan failed: {type(e).__name__}: {e}")
        return []


def report(findings: list[dict], n_tickers: int | None = None) -> None:
    """Print a scan result the same way for the CLI and the daily-job hook."""
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
