"""
fundamentals.py — Sprint 3 fast-follow: Market Cap / P/E / P/B without Yahoo.

WHY THIS EXISTS
Yahoo's company-info endpoint (.info / fast_info) is heavily rate-limited
from datacenter IPs, and on 15-Jul-2026 it went from "sometimes fails" to
"100% blank for every stock, including large caps" -- almost certainly a
wholesale block on Render's IP from a day of heavy yfinance load. Throttling
harder doesn't fix a block. So: stop depending on Yahoo for fundamentals
at all. Same architecture as bhavcopy.py -- a scheduled job hits a source
that actually answers Indian retail requests, stores results once, the
dashboard just reads the table. No live Yahoo call in the request path.

SOURCE: screener.in's free company page (e.g. screener.in/company/GLAND/).
Public, no auth, no API key. Gives Market Cap, Stock P/E, Book Value
(P/B is derived here as CMP / Book Value using the ticker's live CMP).
EV/EBITDA is NOT reliably present on the free page across companies --
left NULL rather than faked; the dashboard shows "--" for it same as any
other missing field, and this is a known, disclosed limitation.

SCOPE: all NON-SME-tracked NSE/BSE holdings across all portfolios.
SME-tracked tickers (the ones in bhavcopy.py's SME_STOCKS) are skipped --
they're thinly-traded and rarely have a screener.in page worth trusting;
if wanted later that's a separate small add, same pattern as everything
else tonight.

Usage:  python fundamentals.py today
Env:    SUPABASE_URL, SUPABASE_SERVICE_KEY
"""
import re
import os
import sys
import time

import requests
from supabase import create_client

import screener_data

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
}
BASE = "https://www.screener.in/company/{sym}/{view}/"
TIMEOUT = 15

# Manually verified screener.in slugs where the trading symbol is NOT the
# right URL (BSE-SME stocks resolve by scrip code; add more here as the
# job's "no data / identity mismatch" log lines identify them).
SLUG_OVERRIDES = {
    "CWD-MS.BO": "543378",       # CWD Ltd
    "HSIL-MT.BO": "543916",      # Hemant Surgical
    "TRUECOLORS.BO": "544531",   # True Colors
    "LEHAR.BO": "532829",        # Lehar Footwears — BSE-only, slug = scrip code
    "SGRL.BO": "540737",         # Shree Ganesh Remedies — BSE-only, slug = scrip code
}


def _client():
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


def tracked_tickers(client) -> dict:
    """{screener_symbol: dashboard_ticker} for every non-SME holding.
    screener.in's URL slug is almost always the NSE trading symbol (even
    for BSE-only stocks it's usually the same root), so we strip the
    .NS/.BO suffix and try that first."""
    res = client.table("holdings").select("stock_name").execute()
    out = {}
    for row in (res.data or []):
        name = str(row.get("stock_name") or "")
        m = re.search(r"\((?:XNSE|XBOM):([^)]+)\)", name)
        if not m:
            continue
        sym = m.group(1).strip()
        ticker = f"{sym}.NS" if "XNSE:" in name else f"{sym}.BO"
        display = name[:name.rfind("(")].strip()
        # v3: attempt every ticker. Slug priority: manual override (verified),
        # else the trading symbol. Numeric BSE codes are valid slugs as-is.
        slug = SLUG_OVERRIDES.get(ticker, sym)
        out[ticker] = {"slug": slug, "display": display}
    return out


def fetch_one(symbol: str, expected_name: str = "") -> dict:
    """Market Cap / PE / Book Value / Sector — PLUS ROCE, ROE, the screener CMP
    and TTM revenue/EBITDA/OPM. {} on failure.

    Delegates to screener_data, which parses the same page far more thoroughly
    and carries the identity gate (whole-word company-name match, so a colliding
    slug can never store another company's numbers). One parser, one set of
    rules — the alternative is two scrapers of the same page drifting apart,
    which is the failure this project keeps re-learning.

    NOTE it returns `cmp` straight off the page. That is what makes P/B possible
    without Yahoo — see store()."""
    snap = screener_data.snapshot(symbol, expected_name)
    if not snap:
        return {}
    r = snap.get("ratios") or {}
    out = {
        "market_cap_cr": r.get("market_cap_cr"),
        "pe": r.get("pe"),
        "book_value": r.get("book_value"),
        "sector": r.get("sector") or r.get("industry"),
        "roce": r.get("roce"),
        "roe": r.get("roe"),
        "cmp": r.get("cmp"),
    }
    out.update(screener_data.ttm_metrics(snap))
    return out


def store(client, ticker: str, data: dict, cmp_price: float = None):
    # P/B needs a price. It used to come from Yahoo's fast_info, which House Rule
    # #1 says is exactly what gets blocked from datacenter IPs — so on Render it
    # returned nothing and `pb` was stored NULL for EVERY ticker (0 of 112 when
    # audited 01-Sep-2026). screener's own page carries the Current Price, so we
    # take it from the page we have already fetched and drop the Yahoo call.
    price = data.get("cmp") or cmp_price
    pb = None
    if data.get("book_value") and price:
        try:
            pb = round(float(price) / float(data["book_value"]), 2)
        except (ZeroDivisionError, TypeError, ValueError):
            pb = None
    payload = {
        "ticker": ticker,
        "market_cap_cr": data.get("market_cap_cr"),
        "pe": data.get("pe"),
        "book_value": data.get("book_value"),
        "pb": pb,
        "sector": data.get("sector"),
        "roce": data.get("roce"),
        "roe": data.get("roe"),
        "revenue_ttm_cr": data.get("revenue_ttm_cr"),
        "ebitda_ttm_cr": data.get("ebitda_ttm_cr"),
        "opm_ttm_pct": data.get("opm_ttm_pct"),
    }
    try:
        client.table("fundamentals_daily").upsert(payload, on_conflict="ticker").execute()
    except Exception as e:
        print(f"  [fundamentals] store failed for {ticker}: {e}")


def update_all(client):
    tracked = tracked_tickers(client)
    if not tracked:
        print("[fundamentals] no non-SME holdings found — nothing to fetch")
        return

    # No Yahoo call here any more: the price used for P/B comes off the same
    # screener page we already fetch (see store()). That removes the one
    # datacenter-IP-blocked dependency this job had.
    ok, failed = 0, []
    for ticker, meta in tracked.items():
        data = fetch_one(meta["slug"], expected_name=meta["display"])
        if not data:
            print(f"  [fundamentals] no data for {ticker} via slug '{meta['slug']}' — "
                  f"empty, unreachable, or identity mismatch (see above)")
            failed.append(ticker)
            time.sleep(1.5)
            continue
        store(client, ticker, data)
        ok += 1
        time.sleep(1.5)   # polite pacing on screener.in (429s at 0.5s)
    print(f"[fundamentals] stored {ok}/{len(tracked)} tickers"
          + (f" — failed: {failed}" if failed else ""))


if __name__ == "__main__":
    client = _client()
    update_all(client)
