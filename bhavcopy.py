"""
bhavcopy.py — free, official EOD price pipeline for the 6 SME/Emerge stocks
that Yahoo Finance doesn't carry (CWD, Hemant Surgical, OBSC Precision,
Thaai Castings, Utssav CG Gold, Vision Infra).

Source: NSE and BSE's own daily bhavcopy files. Free, no auth, no API key —
genuinely public exchange data, published once per trading day after close.

Design principle (same as the rest of this codebase): best-effort, degrade
gracefully. If a day's bhavcopy is late/missing/unreachable, skip it and
try again tomorrow. Never crash the caller.

Usage:
    python bhavcopy.py today       # fetch + store today's close for all 6
    python bhavcopy.py backfill    # fetch ~2 years of history (run once)
"""

import io
import re
import sys
import zipfile
from datetime import date, datetime, timedelta

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# The 6 SME stocks Yahoo doesn't cover. NSE ones are matched by exact SYMBOL
# (confident — bhavcopy's SYMBOL column is authoritative). The 2 BSE ones
# are matched by company-name substring since I don't have their numeric
# scrip codes confirmed — guessing a scrip code wrong risks silently pulling
# a DIFFERENT company's price (exactly the KPL/BMW mistake from earlier
# today). Name-matching is slower but safe; once the first successful run
# confirms the matched SC_CODE, hardcode it here for speed (see the comment
# in fetch_bse_bhavcopy).
SME_STOCKS = {
    "OBSCP.NS":  {"exchange": "NSE", "symbol": "OBSCP"},
    "TCL.NS":    {"exchange": "NSE", "symbol": "TCL"},
    "UTSSAV.NS": {"exchange": "NSE", "symbol": "UTSSAV"},
    "VIESL.NS":  {"exchange": "NSE", "symbol": "VIESL"},
    # Abinaya's NSE Emerge holding (added 13-Jul-2026). SSEGL trades in
    # series ST (Trade-for-Trade surveillance) but still appears in the
    # standard NSE sec_bhavdata_full file under symbol SSEGL, so the same
    # exact-symbol match works.
    "SSEGL.NS":  {"exchange": "NSE", "symbol": "SSEGL"},
    # Yahoo blind spots (added 15-Jul-2026): these two are MAINBOARD NSE
    # stocks with verified-correct symbols, but Yahoo persistently serves
    # nothing for them. This pipeline works for any NSE stock, not just
    # SME -- the official daily file has every symbol. So they're priced
    # here instead of relying on Yahoo ever fixing its coverage.
    "LEHAR.NS":  {"exchange": "NSE", "symbol": "LEHAR"},   # Lehar Footwears (Abinaya)
    "SGRL.NS":   {"exchange": "NSE", "symbol": "SGRL"},    # Shree Ganesh Remedies (Abinaya)
    # BSE's SC_NAME field is a short abbreviated code (e.g. "AEGISLOG" for
    # Aegis Logistics), NOT a full company name -- confirmed against a real
    # bhavcopy pull on 12-Jul-2026. These are the ticker root before the
    # -MS/-MT segment suffix, matching that convention.
    # Scrip codes confirmed via live bhavcopy match on 12-Jul-2026 (exact
    # SC_NAME match, verified: CWD -> 543378, HSIL -> 543916). name_hint
    # kept as a fallback in case a scrip code ever changes.
    "CWD-MS.BO": {"exchange": "BSE", "name_hint": "CWD", "scrip_code": "543378"},
    "HSIL-MT.BO": {"exchange": "BSE", "name_hint": "HSIL", "scrip_code": "543916"},
    # Abinaya's BSE SME holding (added 13-Jul-2026). Scrip code 544531
    # confirmed via BSE listing + screener.in. Matched by scrip_code, the
    # safe path; name_hint kept only as documentation fallback.
    "TRUECOLORS.BO": {"exchange": "BSE", "name_hint": "TRUECOLORS", "scrip_code": "544531"},
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def fetch_nse_bhavcopy(d: date) -> pd.DataFrame:
    """Full NSE bhavcopy for one date. Empty df on any failure (holiday,
    not-yet-published, network issue) — caller should just skip the day."""
    url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200 or len(r.content) < 500:
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        # Known column name across NSE bhavcopy vintages: SYMBOL, CLOSE_PRICE
        # (older files use 'CLOSE'). Normalize once here.
        if "CLOSE_PRICE" in df.columns:
            df = df.rename(columns={"CLOSE_PRICE": "CLOSE"})
        df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()
        return df
    except Exception:
        return pd.DataFrame()


def fetch_bse_bhavcopy(d: date) -> pd.DataFrame:
    """Full BSE bhavcopy for one date (unzipped). Empty df on failure."""
    url = f"https://www.bseindia.com/bsedata/newbhavcopy/bhavcopy{d:%d%m%y}_CSV.ZIP"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200 or len(r.content) < 500:
            return pd.DataFrame()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                df = pd.read_csv(f)
        df.columns = [c.strip().upper() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


def extract_prices_for_date(d: date) -> dict:
    """Returns {ticker: close_price} for whichever of the 6 stocks were
    found in that day's bhavcopy files. Missing ones are simply absent
    from the dict — never a crash, never a fabricated price."""
    out = {}

    nse_needed = {v["symbol"]: k for k, v in SME_STOCKS.items()
                  if v["exchange"] == "NSE"}
    if nse_needed:
        nse_df = fetch_nse_bhavcopy(d)
        if not nse_df.empty:
            o_col = next((c for c in nse_df.columns if c in ("OPEN_PRICE", "OPEN")), None)
            h_col = next((c for c in nse_df.columns if c in ("HIGH_PRICE", "HIGH")), None)
            l_col = next((c for c in nse_df.columns if c in ("LOW_PRICE", "LOW")), None)
            v_col = next((c for c in nse_df.columns if c in ("TTL_TRD_QNTY", "TOTTRDQTY", "VOLUME")), None)
            for sym, ticker in nse_needed.items():
                match = nse_df[nse_df["SYMBOL"] == sym]
                if not match.empty:
                    row = match.iloc[0]
                    try:
                        out[ticker] = {
                            "open": float(row[o_col]) if o_col else float(row["CLOSE"]),
                            "high": float(row[h_col]) if h_col else float(row["CLOSE"]),
                            "low": float(row[l_col]) if l_col else float(row["CLOSE"]),
                            "close": float(row["CLOSE"]),
                            "volume": float(row[v_col]) if v_col else 0.0,
                        }
                    except (ValueError, KeyError):
                        pass

    bse_needed = {k: v for k, v in SME_STOCKS.items() if v["exchange"] == "BSE"}
    if bse_needed:
        bse_df = fetch_bse_bhavcopy(d)
        if bse_df.empty:
            print(f"  [bhavcopy] BSE bhavcopy for {d} came back empty "
                  f"(download failed, or file not yet published for this date) "
                  f"-- {list(bse_needed.keys())} will be missing this run.")
        else:
            print(f"  [bhavcopy] BSE bhavcopy for {d}: {len(bse_df)} rows downloaded OK. "
                  f"Columns: {list(bse_df.columns)}")
        if not bse_df.empty:
            name_col = next((c for c in bse_df.columns if "NAME" in c), None)
            close_col = next((c for c in bse_df.columns if c in ("CLOSE", "CLOSE_PRICE")), None)
            code_col = next((c for c in bse_df.columns if "CODE" in c), None)
            o_col = next((c for c in bse_df.columns if "OPEN" in c), None)
            h_col = next((c for c in bse_df.columns if "HIGH" in c), None)
            l_col = next((c for c in bse_df.columns if "LOW" in c), None)
            v_col = next((c for c in bse_df.columns if "VOL" in c or "QTY" in c), None)
            if name_col and close_col:
                for ticker, cfg in bse_needed.items():
                    if cfg.get("scrip_code") and code_col:
                        match = bse_df[bse_df[code_col].astype(str) == str(cfg["scrip_code"])]
                        strategy = "scrip_code"
                    else:
                        names_upper = bse_df[name_col].astype(str).str.upper().str.strip()
                        hint = cfg["name_hint"].upper()
                        match = bse_df[names_upper == hint]
                        strategy = "exact"
                        if match.empty:
                            match = bse_df[names_upper.str.startswith(hint, na=False)]
                            strategy = "startswith"
                        if match.empty:
                            match = bse_df[names_upper.str.contains(hint, na=False)]
                            strategy = "contains"
                    if match.empty:
                        print(f"  [bhavcopy] No BSE row matched '{cfg['name_hint']}' "
                              f"for {ticker} (tried exact/startswith/contains). "
                              f"Sample names in file: "
                              f"{bse_df[name_col].astype(str).head(8).tolist()}")
                        continue
                    if len(match) > 1:
                        print(f"  [bhavcopy] WARNING: '{cfg['name_hint']}' matched "
                              f"{len(match)} rows via {strategy} match for {ticker} -- "
                              f"using the first. Consider a tighter hint.")
                    row = match.iloc[0]
                    try:
                        out[ticker] = {
                            "open": float(row[o_col]) if o_col else float(row[close_col]),
                            "high": float(row[h_col]) if h_col else float(row[close_col]),
                            "low": float(row[l_col]) if l_col else float(row[close_col]),
                            "close": float(row[close_col]),
                            "volume": float(row[v_col]) if v_col else 0.0,
                        }
                        if code_col and not cfg.get("scrip_code"):
                            print(f"  [bhavcopy] Matched {ticker} -> "
                                  f"{row[name_col]} (scrip code {row[code_col]}). "
                                  f"Verify and hardcode in SME_STOCKS for speed.")
                    except (ValueError, KeyError) as e:
                        print(f"  [bhavcopy] Matched a row for {ticker} but couldn't "
                              f"parse price fields: {e}")
    return out


def store_prices(client, d: date, prices: dict):
    """Upsert into the sme_daily_prices table. One row per (ticker, date).
    `prices` maps ticker -> dict with open/high/low/close/volume."""
    for ticker, ohlcv in prices.items():
        try:
            client.table("sme_daily_prices").upsert({
                "ticker": ticker,
                "price_date": d.isoformat(),
                "open": ohlcv["open"], "high": ohlcv["high"],
                "low": ohlcv["low"], "close": ohlcv["close"],
                "volume": ohlcv["volume"],
            }, on_conflict="ticker,price_date").execute()
        except Exception as e:
            print(f"  [bhavcopy] store failed for {ticker} on {d}: {e}")


def update_today(client):
    """Called by the daily scheduled job, after market close."""
    d = date.today()
    prices = extract_prices_for_date(d)
    if prices:
        store_prices(client, d, prices)
        print(f"[bhavcopy] {d}: stored {len(prices)}/{len(SME_STOCKS)} prices "
              f"({', '.join(prices.keys())})")
    else:
        print(f"[bhavcopy] {d}: no data found (holiday, weekend, or not yet published)")


def backfill(client, days: int = 730):
    """One-time backfill of ~2 years so the weekly EMA system has enough
    history. Skips weekends outright; genuine holidays just come back empty
    and are silently skipped (no crash, no wasted retries)."""
    start = date.today() - timedelta(days=days)
    d = start
    done, found = 0, 0
    while d <= date.today():
        if d.weekday() < 5:  # Mon-Fri only
            prices = extract_prices_for_date(d)
            if prices:
                store_prices(client, d, prices)
                found += 1
            done += 1
            if done % 20 == 0:
                print(f"  [bhavcopy backfill] {d} — {found}/{done} trading days had data so far")
        d += timedelta(days=1)
    print(f"[bhavcopy] Backfill complete: {found} days stored out of {done} trading days scanned")


if __name__ == "__main__":
    import os
    from supabase import create_client
    client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    mode = sys.argv[1] if len(sys.argv) > 1 else "today"
    if mode == "backfill":
        backfill(client)
    else:
        update_today(client)
