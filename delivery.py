"""
delivery.py — Sprint 3 fast-follow: daily delivery % for tracked holdings.

WHAT DELIVERY % MEANS
Of everything traded in a stock today, what share was actually taken as
delivery (demat transfer) vs squared off intraday? High delivery on an
up-move = genuine accumulation. Low delivery = speculative churn.
Agreed design: this is DISPLAYED context only — it never gates or changes
any flowchart state or alert.

DATA SOURCE
NSE's official end-of-day "security-wise full bhavdata" CSV:
  https://archives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv
One file per trading day, includes DELIV_QTY and DELIV_PER per symbol,
covering all NSE series (EQ/BE and the SME series SM/ST where reported).

SCOPE (honest limits)
- NSE holdings only in v1. BSE's delivery file is scrip-code keyed and we
  don't hold code mappings for most BSE names — those show "—" in the
  dashboard rather than a guessed number. BSE can follow if wanted.
- Fragile-by-nature like the filings monitor: NSE occasionally changes
  endpoints or adds bot-blocking. Everything degrades gracefully — a
  failed day is skipped, never breaks the workflow or the dashboard.

WHICH STOCKS
Tracked dynamically: every NSE holding across ALL portfolios (queried from
the holdings table at runtime) — no hardcoded lists to maintain.

Usage:  python delivery.py today       (daily scheduled job)
        python delivery.py backfill    (one-time, ~6 weeks of history)
Env:    SUPABASE_URL, SUPABASE_SERVICE_KEY
"""
import io
import os
import re
import sys
import time
from datetime import date, timedelta

import pandas as pd
import requests
from supabase import create_client

NSE_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept": "text/csv,*/*",
}
BACKFILL_DAYS = 42   # ~6 weeks calendar -> ~28 trading days (>1 month rolling)


def _client():
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


# ---------------------------------------------------------------------------
# Which tickers to track (all NSE holdings, every portfolio)
# ---------------------------------------------------------------------------

def tracked_nse_symbols(client) -> dict:
    """{'ADFFOODS': 'ADFFOODS.NS', ...} for every NSE holding in the db."""
    res = client.table("holdings").select("stock_name").execute()
    out = {}
    for row in (res.data or []):
        m = re.search(r"\(XNSE:([^)]+)\)", str(row.get("stock_name") or ""))
        if m:
            sym = m.group(1).strip()
            out[sym] = f"{sym}.NS"
    return out


# ---------------------------------------------------------------------------
# Fetch + parse one day's NSE file
# ---------------------------------------------------------------------------

def fetch_nse_day(d: date) -> pd.DataFrame:
    """The full NSE delivery CSV for one date. Empty df on any failure
    (holiday, weekend, file not yet published, blocking) — by design."""
    url = NSE_URL.format(ddmmyyyy=d.strftime("%d%m%Y"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200 or not r.text or "SYMBOL" not in r.text[:200]:
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(r.text))
        # NSE pads both column names and values with spaces
        df.columns = [c.strip() for c in df.columns]
        for c in ("SYMBOL", "SERIES", "DELIV_QTY", "DELIV_PER", "TTL_TRD_QNTY"):
            if c in df.columns and df[c].dtype == object:
                df[c] = df[c].astype(str).str.strip()
        return df
    except Exception:
        return pd.DataFrame()


def extract_rows(df: pd.DataFrame, symbols: dict, d: date) -> list:
    """Rows for our tracked symbols only, delivery fields cleaned.
    NSE prints '-' where delivery reporting doesn't apply — skipped."""
    if df.empty:
        return []
    hits = df[df["SYMBOL"].isin(symbols.keys())]
    rows = []
    for _, r in hits.iterrows():
        dp, dq, tq = r.get("DELIV_PER"), r.get("DELIV_QTY"), r.get("TTL_TRD_QNTY")
        try:
            deliv_pct = float(dp)
        except (TypeError, ValueError):
            continue  # '-' or missing: no delivery reporting for this row
        def _num(x):
            try:
                return float(str(x).replace(",", ""))
            except (TypeError, ValueError):
                return None
        rows.append({
            "ticker": symbols[r["SYMBOL"]],
            "price_date": d.isoformat(),
            "deliv_pct": deliv_pct,
            "deliv_qty": _num(dq),
            "traded_qty": _num(tq),
        })
    return rows


def store_rows(client, rows: list):
    for row in rows:
        try:
            client.table("delivery_daily").upsert(
                row, on_conflict="ticker,price_date").execute()
        except Exception as e:
            print(f"  [delivery] store failed {row['ticker']} {row['price_date']}: {e}")


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def update_today(client):
    d = date.today()
    if d.weekday() >= 5:
        print(f"[delivery] {d}: weekend — nothing to fetch")
        return
    symbols = tracked_nse_symbols(client)
    if not symbols:
        print("[delivery] no NSE holdings found — nothing to track")
        return
    day = fetch_nse_day(d)
    rows = extract_rows(day, symbols, d)
    if rows:
        store_rows(client, rows)
        print(f"[delivery] {d}: stored {len(rows)}/{len(symbols)} tracked NSE symbols")
    else:
        print(f"[delivery] {d}: no data (holiday, not yet published, or fetch blocked) — skipped")


def backfill(client, days: int = BACKFILL_DAYS):
    symbols = tracked_nse_symbols(client)
    if not symbols:
        print("[delivery] no NSE holdings found — nothing to backfill")
        return
    d = date.today() - timedelta(days=days)
    found = 0
    while d <= date.today():
        if d.weekday() < 5:
            day = fetch_nse_day(d)
            rows = extract_rows(day, symbols, d)
            if rows:
                store_rows(client, rows)
                found += 1
                print(f"  [delivery backfill] {d}: {len(rows)} symbols stored")
            time.sleep(1)   # be polite to NSE's archive server
        d += timedelta(days=1)
    print(f"[delivery] Backfill complete: {found} trading days stored")


if __name__ == "__main__":
    client = _client()
    mode = sys.argv[1] if len(sys.argv) > 1 else "today"
    if mode == "backfill":
        backfill(client)
    else:
        update_today(client)
