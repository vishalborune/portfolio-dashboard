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
import time
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
    # Yahoo blind spots (added 15-Jul-2026, corrected same day): initially
    # assumed NSE-listed based on a web search that turned out to conflate
    # exchanges. Ground truth from the ACTUAL NSE bhavcopy file (checked via
    # `python bhavcopy.py check`) proved neither symbol exists there. Both
    # are BSE-ONLY. Scrip codes confirmed directly from bseindia.com URLs:
    # Lehar Footwears -> bseindia.com/.../lehar/532829
    # Shree Ganesh Remedies -> bseindia.com/.../sgrl/540737 (also directly
    # confirmed BSE volume nonzero / NSE volume zero on a 3rd-party quote page).
    "LEHAR.BO":  {"exchange": "BSE", "name_hint": "LEHAR", "scrip_code": "532829"},
    "SGRL.BO":   {"exchange": "BSE", "name_hint": "SGRL", "scrip_code": "540737"},
    # Benchmark ETF proxies (20-Jul-2026): NSE discontinued the legacy
    # index CSV (their all-reports page: old formats dead w.e.f. 08-Jul-2024),
    # and exact Smallcap-100 data sits behind IP-blocked endpoints. These
    # ETFs track the Nifty Smallcap 250 and trade as ordinary NSE equities,
    # so the SAME daily bhavcopy we already fetch prices them. Both
    # candidates listed; whichever the real NSE file contains gets stored,
    # the other logs NOT FOUND daily and is harmless. The digest tries them
    # in order and labels the benchmark as an ETF proxy.
    "HDFCSML250.NS": {"exchange": "NSE", "symbol": "HDFCSML250"},
    "MOSMALL250.NS": {"exchange": "NSE", "symbol": "MOSMALL250"},
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
    # BSE holdings stored by NUMERIC scrip code (added 22-Jul-2026). These were
    # being priced by YAHOO -- the blind spot rule #1 warns about -- and it bit:
    # Yahoo's live quote said Rs 498.65 for Kwality while BSE's own file said
    # Rs 2,689.10, firing a false loss-stop AND trailing-stop on a holding that
    # is actually up ~210%. Own the data instead. Every scrip code + SC_NAME
    # below was VERIFIED against the real BSE bhavcopy for 21-Jul-2026 (rule #5).
    "539997.BO": {"exchange": "BSE", "name_hint": "KPL",        "scrip_code": "539997"},
    "532856.BO": {"exchange": "BSE", "name_hint": "TIMETECHNO", "scrip_code": "532856"},
    "532829.BO": {"exchange": "BSE", "name_hint": "LEHAR",      "scrip_code": "532829"},
    "542669.BO": {"exchange": "BSE", "name_hint": "BMW",        "scrip_code": "542669"},
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def fetch_nse_bhavcopy(d: date) -> pd.DataFrame:
    """Full NSE bhavcopy for one date. Empty df on any failure.

    Hardened 21-Jul-2026 after an evening where every NSE endpoint failed
    while BSE worked: (a) tries BOTH mirror hosts -- nsearchives threw
    503s that night while its sibling may serve fine; (b) FORENSIC logging
    on every failure: status, bytes, and a snippet of the actual response,
    so "came back empty" can never again hide whether it's timing (404),
    blocking (403), their outage (5xx), or a format change (200 + junk)."""
    for host in ("nsearchives.nseindia.com", "archives.nseindia.com"):
        url = f"https://{host}/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                print(f"  [bhavcopy] NSE {host} for {d}: HTTP {r.status_code}, "
                      f"{len(r.content)} bytes")
                continue
            if len(r.content) < 500 or "SYMBOL" not in r.text[:300]:
                print(f"  [bhavcopy] NSE {host} for {d}: 200 OK but unexpected "
                      f"content ({len(r.content)} bytes, starts: {r.text[:80]!r})")
                continue
            df = pd.read_csv(io.StringIO(r.text))
            df.columns = [c.strip() for c in df.columns]
            if "CLOSE_PRICE" in df.columns:
                df = df.rename(columns={"CLOSE_PRICE": "CLOSE"})
            df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()
            return df
        except Exception as e:
            print(f"  [bhavcopy] NSE {host} for {d}: {type(e).__name__}: {e}")
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




# --- Benchmark index (added 19-Jul-2026) -----------------------------------
# The digest benchmarks portfolio XIRR against the Nifty Smallcap 100.
# Yahoo's ^CNXSC proved unreliable, so we own the data: NSE publishes
# official daily index closes on the same archives host as the bhavcopy.
# Stored in sme_daily_prices under a synthetic ticker -- the table fits
# (ticker/date/OHLC), no new schema. Rides the daily job AND the backfill.
INDEX_TRACK = {"NIFTYSMLCAP100.IDX": "Nifty Smallcap 100"}


def fetch_index_closes(d: date) -> dict:
    """{synthetic_ticker: ohlcv} from NSE's official daily index file.
    Empty dict on any failure (holiday, not published, unreachable)."""
    # archives.nseindia.com: the host delivery.py provably fetches from
    # GitHub Actions daily. The nsearchives variant HUNG to timeout on every
    # request during the 19-Jul backfill (490 x 20s = the 5.5-hour run).
    url = f"https://archives.nseindia.com/content/indices/ind_close_all_{d:%d%m%Y}.csv"
    out = {}
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200 or len(r.content) < 200:
            print(f"  [bhavcopy] index file for {d}: HTTP {r.status_code}, "
                  f"{len(r.content)} bytes — skipped")
            return {}
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        name_col = next((c for c in df.columns if "Index Name" in c), None)
        close_col = next((c for c in df.columns if "Closing" in c), None)
        o_col = next((c for c in df.columns if "Open" in c), None)
        h_col = next((c for c in df.columns if "High" in c), None)
        l_col = next((c for c in df.columns if "Low" in c), None)
        if not name_col or not close_col:
            print(f"  [bhavcopy] index file {d}: unrecognised columns "
                  f"{list(df.columns)[:6]} — skipped")
            return {}
        names = df[name_col].astype(str).str.strip().str.upper()
        for ticker, idx_name in INDEX_TRACK.items():
            m = df[names == idx_name.upper()]
            if m.empty:
                print(f"  [bhavcopy] index '{idx_name}' not found in file for {d}")
                continue
            row = m.iloc[0]
            try:
                close = float(row[close_col])
                out[ticker] = {
                    "open": float(row[o_col]) if o_col else close,
                    "high": float(row[h_col]) if h_col else close,
                    "low": float(row[l_col]) if l_col else close,
                    "close": close, "volume": 0.0,
                }
            except (ValueError, KeyError) as e:
                print(f"  [bhavcopy] index row parse failed for {d}: {e}")
    except Exception as e:
        print(f"  [bhavcopy] index fetch failed for {d}: {e}")
    return out


def extract_prices_for_date(d: date) -> dict:
    """Returns {ticker: close_price} for whichever of the 6 stocks were
    found in that day's bhavcopy files. Missing ones are simply absent
    from the dict — never a crash, never a fabricated price."""
    out = {}

    nse_needed = {v["symbol"]: k for k, v in SME_STOCKS.items()
                  if v["exchange"] == "NSE"}
    if nse_needed:
        nse_df = fetch_nse_bhavcopy(d)
        if nse_df.empty:
            print(f"  [bhavcopy] NSE bhavcopy for {d} came back empty "
                  f"(download failed, or not yet published) -- "
                  f"{list(nse_needed.values())} will be missing this run.")
        else:
            o_col = next((c for c in nse_df.columns if c in ("OPEN_PRICE", "OPEN")), None)
            h_col = next((c for c in nse_df.columns if c in ("HIGH_PRICE", "HIGH")), None)
            l_col = next((c for c in nse_df.columns if c in ("LOW_PRICE", "LOW")), None)
            v_col = next((c for c in nse_df.columns if c in ("TTL_TRD_QNTY", "TOTTRDQTY", "VOLUME")), None)
            for sym, ticker in nse_needed.items():
                match = nse_df[nse_df["SYMBOL"] == sym]
                if match.empty:
                    # THIS is the line that was missing before 15-Jul-2026 --
                    # NSE failures were previously silent, indistinguishable
                    # from "never tried". Now every miss is visible.
                    sample = nse_df["SYMBOL"].head(5).tolist()
                    print(f"  [bhavcopy] No NSE row matched symbol '{sym}' for {ticker} "
                          f"on {d}. Sample symbols in file: {sample}")
                    continue
                row = match.iloc[0]
                try:
                    out[ticker] = {
                        "open": float(row[o_col]) if o_col else float(row["CLOSE"]),
                        "high": float(row[h_col]) if h_col else float(row["CLOSE"]),
                        "low": float(row[l_col]) if l_col else float(row["CLOSE"]),
                        "close": float(row["CLOSE"]),
                        "volume": float(row[v_col]) if v_col else 0.0,
                    }
                except (ValueError, KeyError) as e:
                    print(f"  [bhavcopy] Matched NSE row for {ticker} on {d} but "
                          f"couldn't parse price fields: {e}")

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


def index_backfill(client, days: int = 760):
    """DEDICATED fast index backfill (20-Jul-2026). The index fetch was
    originally bolted into the main backfill -- one extra hanging request
    inside every one of ~490 day-iterations turned a ~1hr job into 5.5hrs
    with zero rows to show. Lesson: never couple a new fragile fetch into
    the longest-running job. This walks the same dates fetching ONLY the
    small official index CSV; ~15-20 min, every day logged."""
    d = date.today() - timedelta(days=days)
    stored = 0
    while d <= date.today():
        if d.weekday() < 5:
            prices = fetch_index_closes(d)
            if prices:
                store_prices(client, d, prices)
                stored += 1
                if stored % 20 == 0:
                    print(f"  [index backfill] {stored} trading days stored (at {d})")
            time.sleep(0.6)
        d += timedelta(days=1)
    print(f"[bhavcopy] Index backfill complete: {stored} trading days of "
          f"{', '.join(INDEX_TRACK)} stored")


def update_today(client):
    """Called by the daily scheduled job, after market close."""
    d = date.today()
    prices = extract_prices_for_date(d)
    prices.update(fetch_index_closes(d))   # benchmark index: daily only
    if prices:
        store_prices(client, d, prices)
        print(f"[bhavcopy] {d}: stored {len(prices)}/{len(SME_STOCKS)} prices "
              f"({', '.join(prices.keys())})")
    else:
        print(f"[bhavcopy] {d}: no data found (holiday, weekend, or not yet published)")

    # SPLIT/BONUS WATCHDOG (21-Jul-2026): scan the raw table for the >25%
    # overnight step that means an UNADJUSTED corporate action. Catches the
    # next split/bonus the day it lands, instead of it silently corrupting
    # EMA states for months (as CWD's 4:1 bonus did). Best-effort: never let
    # this diagnostic break the daily price job.
    try:
        import corporate_actions
        findings = corporate_actions.scan_supabase(client)
        corporate_actions.report(findings)
    except Exception as e:
        print(f"  [bhavcopy] corporate-action scan skipped: {type(e).__name__}: {e}")


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
    mode = sys.argv[1] if len(sys.argv) > 1 else "today"
    if mode == "check":
        # Fast diagnostic: no Supabase needed, just prints whether each
        # tracked symbol is found in the MOST RECENT trading day's file.
        # Answers "is the symbol right?" in seconds instead of a 30-min backfill.
        d = date.today() - timedelta(days=1)   # last COMPLETED trading day:
        while d.weekday() >= 5:                # today's file doesn't exist
            d -= timedelta(days=1)             # until evening, so morning
        print(f"[check] Testing against {d} (last completed trading day)...")
        prices = extract_prices_for_date(d)
        for ticker in SME_STOCKS:
            status = f"FOUND close={prices[ticker]['close']}" if ticker in prices else "NOT FOUND"
            print(f"  {ticker:16s} {status}")
        sys.exit(0)
    from supabase import create_client
    client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    if mode == "backfill":
        backfill(client)
    elif mode == "index-backfill":
        index_backfill(client)
    else:
        update_today(client)
