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
    """Market Cap / PE / Book Value / Sector for one company. Tries
    consolidated first, falls back to standalone. Returns {} on failure.

    IDENTITY CHECK (v3, 15-Jul-2026): slugs can collide -- /company/TCL/
    might be a different company than Thaai Castings. If expected_name is
    given, the page's <h1> company name must share at least one
    significant word with it, else the fetch is REJECTED and logged.
    Storing another company's numbers against our stock is the
    wrong-instrument bug all over again; a blank beats a lie."""
    for view in ("consolidated", ""):
        url = BASE.format(sym=symbol, view=view).replace("//", "/").replace("https:/", "https://")
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code != 200:
                continue
            text = r.text
            # The top-ratios block is a <li> list: "<span class="name">Market Cap</span> ... <span class="number">41,599</span>"
            def grab(label):
                # Tempered pattern: the number must appear BEFORE the next
                # </li>. Without this, a stock whose P/E is blank on screener
                # made the regex run into the NEXT ratio and grab its number
                # (caught live 15-Jul-2026: Cockerill "PE 0.07" was actually
                # the following ratio's value). Wrong number > missing number
                # is the worst failure mode for financial data.
                pat = rf'"name"[^>]*>\s*{re.escape(label)}\s*</span>(?:(?!</li>).)*?"number"[^>]*>\s*([\d,\.]+)'
                m = re.search(pat, text, re.DOTALL)
                return float(m.group(1).replace(",", "")) if m else None

            # Identity gate BEFORE trusting any numbers
            if expected_name:
                hm = re.search(r"<h1[^>]*>([^<]+)</h1>", text)
                page_name = (hm.group(1) if hm else "").upper()
                stop = {"LIMITED", "LTD", "INDIA", "INDUSTRIES", "THE", "AND", "&"}
                want = {w for w in re.split(r"[^A-Z0-9]+", expected_name.upper())
                        if len(w) >= 3 and w not in stop}
                if want and not any(w in page_name for w in want):
                    print(f"  [fundamentals] identity MISMATCH for slug '{symbol}': "
                          f"page is '{page_name.title().strip()}', expected ~'{expected_name}'. "
                          f"Rejected -- add correct slug to SLUG_OVERRIDES.")
                    return {}
            mcap = grab("Market Cap")
            pe = grab("Stock P/E")
            bv = grab("Book Value")
            if mcap is None and pe is None and bv is None:
                continue  # this view had nothing usable, try the other
            # Sector: screener shows it as a breadcrumb link near "Peer comparison"
            sector = None
            sm = re.search(r'"Broad Sector"[^>]*>([^<]+)<', text)
            if sm:
                sector = sm.group(1).strip()
            return {"market_cap_cr": mcap, "pe": pe, "book_value": bv, "sector": sector}
        except Exception:
            continue
    return {}


def store(client, ticker: str, data: dict, cmp_price: float = None):
    pb = None
    if data.get("book_value") and cmp_price:
        try:
            pb = round(cmp_price / data["book_value"], 2)
        except (ZeroDivisionError, TypeError):
            pb = None
    payload = {
        "ticker": ticker,
        "market_cap_cr": data.get("market_cap_cr"),
        "pe": data.get("pe"),
        "book_value": data.get("book_value"),
        "pb": pb,
        "sector": data.get("sector"),
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

    # Need live CMP to derive P/B — reuse whatever's already in sme_daily_prices
    # is irrelevant here (these are non-SME); pull a quick Yahoo quote per
    # ticker just for the price used in the P/B division. Best-effort: if it
    # fails, P/B is simply left null rather than guessed.
    import yfinance as yf
    ok, failed = 0, []
    for ticker, meta in tracked.items():
        data = fetch_one(meta["slug"], expected_name=meta["display"])
        if not data:
            print(f"  [fundamentals] no data for {ticker} via slug '{meta['slug']}' — "
                  f"empty, unreachable, or identity mismatch (see above)")
            failed.append(ticker)
            time.sleep(0.5)
            continue
        cmp_price = None
        try:
            fi = yf.Ticker(ticker).fast_info
            cmp_price = fi.get("last_price") if hasattr(fi, "get") else getattr(fi, "last_price", None)
        except Exception:
            pass
        store(client, ticker, data, cmp_price)
        ok += 1
        time.sleep(0.5)   # polite pacing on screener.in
    print(f"[fundamentals] stored {ok}/{len(tracked)} tickers"
          + (f" — failed: {failed}" if failed else ""))


if __name__ == "__main__":
    client = _client()
    update_all(client)
