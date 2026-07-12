"""
delivery.py — Sprint 3 fast-follow: daily delivery % for tracked holdings.
v2 (13 Jul 2026): BSE coverage added; per-day logging always on.

WHAT DELIVERY % MEANS
Of everything traded in a stock today, what share was actually taken as
delivery (demat transfer) vs squared off intraday? High delivery on an
up-move = genuine accumulation. Low delivery = speculative churn.
Agreed design: DISPLAYED context only — never gates any state or alert.

DATA SOURCES
- NSE: official "security-wise full bhavdata" CSV (one per trading day),
  includes DELIV_QTY / DELIV_PER for every symbol incl. SME series:
    https://archives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv
- BSE (v2): official gross delivery file (one zip per trading day),
  pipe-delimited TXT keyed by scrip code:
    https://www.bseindia.com/BSEDATA/gross/YYYY/SCBSEALLDDMM.zip

WHICH STOCKS
Tracked dynamically from the holdings table (all portfolios):
- NSE: every XNSE symbol.
- BSE: every XBOM symbol. Numeric symbols (e.g. 539997 = Kwality,
  542669 = BMW Industries) ARE the scrip code. Non-numeric BSE-SME
  symbols need the explicit map below (codes confirmed during the
  bhavcopy build). Unknown non-numeric BSE symbols are skipped loudly.

LOGGING LESSON (from the first backfill run): every processed day prints
an outcome line, success OR failure. A silent spinner is undiagnosable.

Usage:  python delivery.py today       (daily scheduled job)
        python delivery.py backfill    (one-time, ~6 weeks of history)
Env:    SUPABASE_URL, SUPABASE_SERVICE_KEY
"""
import io
import os
import re
import sys
import time
import zipfile
from datetime import date, timedelta

import pandas as pd
import requests
from supabase import create_client

NSE_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
BSE_URL = "https://www.bseindia.com/BSEDATA/gross/{yyyy}/SCBSEALL{ddmm}.zip"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept": "*/*",
    "Referer": "https://www.bseindia.com/",
}
TIMEOUT = 15          # fail fast; a hung request tells us nothing a failed one doesn't
BACKFILL_DAYS = 42    # ~6 weeks calendar -> ~28 trading days

# BSE-SME symbols whose dashboard ticker is NOT the scrip code.
# Codes confirmed against BSE during the bhavcopy build (Jul 2026).
BSE_SCRIP_OVERRIDES = {
    "CWD-MS": "543378",     # CWD Ltd
    "HSIL-MT": "543916",    # Hemant Surgical Industries
}


def _client():
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


# ---------------------------------------------------------------------------
# Which tickers to track (all portfolios)
# ---------------------------------------------------------------------------

def tracked_symbols(client):
    """Two maps from the holdings table:
    nse: {'ADFFOODS': 'ADFFOODS.NS', ...}
    bse: {'539997': '539997.BO', '543378': 'CWD-MS.BO', ...}  (scrip code -> dashboard ticker)
    """
    res = client.table("holdings").select("stock_name").execute()
    nse, bse = {}, {}
    for row in (res.data or []):
        name = str(row.get("stock_name") or "")
        m = re.search(r"\(XNSE:([^)]+)\)", name)
        if m:
            sym = m.group(1).strip()
            nse[sym] = f"{sym}.NS"
            continue
        m = re.search(r"\(XBOM:([^)]+)\)", name)
        if m:
            sym = m.group(1).strip()
            if sym.isdigit():
                bse[sym] = f"{sym}.BO"
            elif sym in BSE_SCRIP_OVERRIDES:
                bse[BSE_SCRIP_OVERRIDES[sym]] = f"{sym}.BO"
            else:
                print(f"[delivery] BSE symbol '{sym}' has no scrip-code mapping — "
                      f"skipped. Add it to BSE_SCRIP_OVERRIDES to track it.")
    return nse, bse


# ---------------------------------------------------------------------------
# NSE side
# ---------------------------------------------------------------------------

def fetch_nse_day(d: date) -> pd.DataFrame:
    url = NSE_URL.format(ddmmyyyy=d.strftime("%d%m%Y"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200 or not r.text or "SYMBOL" not in r.text[:200]:
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        for c in ("SYMBOL", "SERIES", "DELIV_QTY", "DELIV_PER", "TTL_TRD_QNTY"):
            if c in df.columns and df[c].dtype == object:
                df[c] = df[c].astype(str).str.strip()
        return df
    except Exception:
        return pd.DataFrame()


def _num(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def extract_nse_rows(df: pd.DataFrame, symbols: dict, d: date) -> list:
    if df.empty:
        return []
    hits = df[df["SYMBOL"].isin(symbols.keys())]
    rows = []
    for _, r in hits.iterrows():
        try:
            deliv_pct = float(r.get("DELIV_PER"))
        except (TypeError, ValueError):
            continue  # '-' = no delivery reporting for this series
        rows.append({"ticker": symbols[r["SYMBOL"]], "price_date": d.isoformat(),
                     "deliv_pct": deliv_pct, "deliv_qty": _num(r.get("DELIV_QTY")),
                     "traded_qty": _num(r.get("TTL_TRD_QNTY"))})
    return rows


# ---------------------------------------------------------------------------
# BSE side (v2)
# ---------------------------------------------------------------------------

def fetch_bse_day(d: date) -> pd.DataFrame:
    """BSE gross delivery file for one date. Zip containing one pipe-delimited
    TXT. Empty df on any failure (holiday, not published, blocked)."""
    url = BSE_URL.format(yyyy=d.strftime("%Y"), ddmm=d.strftime("%d%m"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200 or not r.content:
            return pd.DataFrame()
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        name = zf.namelist()[0]
        text = zf.read(name).decode("utf-8", errors="replace")
        sep = "|" if "|" in text.splitlines()[0] else ","
        df = pd.read_csv(io.StringIO(text), sep=sep)
        df.columns = [str(c).strip().upper() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


def _find_col(df, *needles):
    """First column whose name contains ALL the given substrings."""
    for c in df.columns:
        if all(n in c for n in needles):
            return c
    return None


def extract_bse_rows(df: pd.DataFrame, scrips: dict, d: date) -> list:
    """scrips: {scrip_code: dashboard_ticker}. Column names in BSE's file
    have shifted over the years, so locate them by substring, not position."""
    if df.empty:
        return []
    c_code = _find_col(df, "SCRIP")
    c_pct = _find_col(df, "DELV", "PER") or _find_col(df, "DELIVERY", "PER")
    c_dqty = _find_col(df, "DELIVERY", "QTY") or _find_col(df, "DELV", "QTY")
    c_vol = _find_col(df, "VOLUME")
    if not c_code or not c_pct:
        print(f"  [delivery] BSE file for {d}: unrecognised columns {list(df.columns)[:8]} — skipped")
        return []
    codes = df[c_code].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    hits = df[codes.isin(scrips.keys())]
    rows = []
    for _, r in hits.iterrows():
        code = str(r[c_code]).strip().replace(".0", "")
        deliv_pct = _num(r[c_pct])
        if deliv_pct is None:
            continue
        rows.append({"ticker": scrips[code], "price_date": d.isoformat(),
                     "deliv_pct": deliv_pct,
                     "deliv_qty": _num(r[c_dqty]) if c_dqty else None,
                     "traded_qty": _num(r[c_vol]) if c_vol else None})
    return rows


# ---------------------------------------------------------------------------
# Store + modes
# ---------------------------------------------------------------------------

def store_rows(client, rows: list):
    for row in rows:
        try:
            client.table("delivery_daily").upsert(
                row, on_conflict="ticker,price_date").execute()
        except Exception as e:
            print(f"  [delivery] store failed {row['ticker']} {row['price_date']}: {e}")


def process_day(client, d: date, nse: dict, bse: dict) -> int:
    """Fetch + store one day, ALWAYS printing the outcome. Returns rows stored."""
    nse_rows = extract_nse_rows(fetch_nse_day(d), nse, d) if nse else []
    bse_rows = extract_bse_rows(fetch_bse_day(d), bse, d) if bse else []
    rows = nse_rows + bse_rows
    if rows:
        store_rows(client, rows)
        print(f"[delivery] {d}: stored {len(nse_rows)} NSE + {len(bse_rows)} BSE symbols")
    else:
        print(f"[delivery] {d}: no data (holiday, not yet published, or fetch blocked)")
    return len(rows)


def update_today(client):
    d = date.today()
    if d.weekday() >= 5:
        print(f"[delivery] {d}: weekend — nothing to fetch")
        return
    nse, bse = tracked_symbols(client)
    if not (nse or bse):
        print("[delivery] no holdings found — nothing to track")
        return
    process_day(client, d, nse, bse)


def backfill(client, days: int = BACKFILL_DAYS):
    nse, bse = tracked_symbols(client)
    if not (nse or bse):
        print("[delivery] no holdings found — nothing to backfill")
        return
    print(f"[delivery] Backfill: tracking {len(nse)} NSE symbols, {len(bse)} BSE scrips")
    d = date.today() - timedelta(days=days)
    days_with_data = 0
    while d <= date.today():
        if d.weekday() < 5:
            if process_day(client, d, nse, bse):
                days_with_data += 1
            time.sleep(1)   # be polite to the exchange servers
        d += timedelta(days=1)
    print(f"[delivery] Backfill complete: {days_with_data} trading days stored")


if __name__ == "__main__":
    client = _client()
    mode = sys.argv[1] if len(sys.argv) > 1 else "today"
    if mode == "backfill":
        backfill(client)
    else:
        update_today(client)
