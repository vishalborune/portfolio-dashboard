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
    "PARMESHWAR.BO": "544330",   # Parmeshwar Metal — screener 404s on the symbol
}


def _client():
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


def tracked_tickers(client) -> dict:
    """{screener_symbol: dashboard_ticker} for every Indian holding AND watchlist
    name. screener.in's URL slug is almost always the NSE trading symbol (even
    for BSE-only stocks it's usually the same root), so we strip the
    .NS/.BO suffix and try that first.

    Watchlist names joined 12-Oct-2026 for the pledge early warning: a pledge on
    a stock you are ABOUT to buy matters more than one you already hold. US names
    never match the XNSE/XBOM pattern, so they stay out (usfundamentals owns them)."""
    rows = list(client.table("holdings").select("stock_name").execute().data or [])
    try:
        rows += client.table("watchlist").select("stock_name").execute().data or []
    except Exception as e:
        print(f"  [fundamentals] watchlist read failed ({type(e).__name__}: {e}) "
              f"— holdings only this run")
    out = {}
    for row in rows:
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
    # Promoter / FII / DII / pledge trend — off the SAME page, no extra request.
    out["shareholding"] = snap.get("shareholding") or {}

    # A company that STARTED consolidated reporting recently (E2E, caught
    # 02-Sep-2026) can have a consolidated page whose quarterly table is live
    # but whose ratio strip is still BLANK — screener hasn't computed the
    # consolidated ratios yet. The standalone strip has real values, so when
    # the winning view gives us NONE of BV/ROCE/ROE, spend one extra request
    # on the standalone strip and fill only the missing keys. Basis stays
    # honest: only ratio-strip fields (point-in-time) are merged, never the
    # quarterly/TTM tables.
    if (snap.get("basis") == "consolidated"
            and out["book_value"] is None and out["roce"] is None
            and out["roe"] is None):
        try:
            import requests as _rq
            slug = screener_data.slug_for(symbol)
            resp = _rq.get(f"https://www.screener.in/company/{slug}/",
                           headers=screener_data.HEADERS, timeout=20)
            if resp.status_code == 200 and screener_data._identity_ok(
                    resp.text, expected_name):
                alt = screener_data.top_ratios(resp.text)
                for k in ("book_value", "roce", "roe"):
                    if out[k] is None and alt.get(k) is not None:
                        out[k] = alt[k]
                if any(alt.get(k) is not None for k in ("book_value", "roce", "roe")):
                    print(f"  [fundamentals] {symbol}: consolidated ratio strip "
                          f"blank — BV/ROCE/ROE taken from standalone view")
        except Exception as e:
            print(f"  [fundamentals] {symbol}: standalone ratio fallback failed "
                  f"({type(e).__name__}: {e}) — leaving blanks")
    return out


# Shareholding columns (12-Oct-2026). Kept separate so a database that has not
# had the migration yet still stores everything it stored before.
SHP_COLS = ("pledge_pct", "promoter_pct", "fii_pct", "dii_pct", "shp_period", "shp_trend")
_SHP_MISSING = False      # set once the table says the columns do not exist


def _num_list(xs):
    """Plain floats/None for a JSON column (House Rule #6: no numpy types)."""
    out = []
    for x in xs or []:
        try:
            out.append(None if x is None else round(float(x), 2))
        except (TypeError, ValueError):
            out.append(None)
    return out


def _last(xs):
    vals = [x for x in (xs or []) if x is not None]
    return vals[-1] if vals else None


def shareholding_payload(sh: dict, prev_trend: dict = None, today: str = None) -> dict:
    """Latest promoter/FII/DII/pledge plus the quarterly trend.

    pledge_pct stays NULL when screener does not flag a pledge — 'absent' means
    'not flagged here', not a verified 0% (House Rule #2). The alert treats NULL
    that way too.

    Screener now gives pledge as ONE current value (no quarterly row), so we keep
    our own history in shp_trend['pledge_hist'] = [[date, pct|None], ...], adding
    a point only when the value changes. That is what lets the alert see 'pledge
    went 20% -> 35%' within a day of screener updating, instead of a quarter."""
    if not sh or not sh.get("periods"):
        return {}
    from datetime import date as _date
    pledge_now = _last(_num_list(sh.get("pledge_pct")))
    hist = [list(x) for x in ((prev_trend or {}).get("pledge_hist") or [])]
    if not hist or hist[-1][1] != pledge_now:
        hist.append([today or _date.today().isoformat(), pledge_now])
    trend = {"periods": [str(p) for p in sh.get("periods") or []],
             "promoters": _num_list(sh.get("promoters")),
             "fiis": _num_list(sh.get("fiis")),
             "diis": _num_list(sh.get("diis")),
             "pledge_hist": hist[-12:]}
    return {"pledge_pct": pledge_now,
            "promoter_pct": _last(trend["promoters"]),
            "fii_pct": _last(trend["fiis"]),
            "dii_pct": _last(trend["diis"]),
            "shp_period": trend["periods"][-1] if trend["periods"] else None,
            "shp_trend": trend}


def store(client, ticker: str, data: dict, cmp_price: float = None,
          prev_trend: dict = None):
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
    global _SHP_MISSING
    shp = {} if _SHP_MISSING else shareholding_payload(data.get("shareholding"), prev_trend)
    try:
        client.table("fundamentals_daily").upsert({**payload, **shp},
                                                  on_conflict="ticker").execute()
    except Exception as e:
        if shp and any(c in str(e) for c in SHP_COLS):
            # Migration not run yet: say so ONCE, keep storing what we always did.
            _SHP_MISSING = True
            print("  [fundamentals] shareholding columns missing in fundamentals_daily "
                  "— run the 12-Oct-2026 migration in CLAUDE.md; storing the rest")
            try:
                client.table("fundamentals_daily").upsert(payload, on_conflict="ticker").execute()
            except Exception as e2:
                print(f"  [fundamentals] store failed for {ticker}: {e2}")
        else:
            print(f"  [fundamentals] store failed for {ticker}: {e}")


def update_all(client):
    tracked = tracked_tickers(client)
    if not tracked:
        print("[fundamentals] no Indian holdings or watchlist names found — nothing to fetch")
        return

    # No Yahoo call here any more: the price used for P/B comes off the same
    # screener page we already fetch (see store()). That removes the one
    # datacenter-IP-blocked dependency this job had.
    # Yesterday's shareholding trend for every ticker, in ONE query, so the pledge
    # history can be extended (see shareholding_payload). Missing column (no
    # migration yet) just means no history — never a failed run.
    prev = {}
    try:
        for r in client.table("fundamentals_daily").select("ticker, shp_trend").execute().data or []:
            if r.get("shp_trend"):
                prev[r["ticker"]] = r["shp_trend"]
    except Exception as e:
        print(f"  [fundamentals] previous shareholding not readable ({type(e).__name__}) "
              f"— pledge history starts fresh")

    ok, failed = 0, []
    for ticker, meta in tracked.items():
        data = fetch_one(meta["slug"], expected_name=meta["display"])
        if not data:
            print(f"  [fundamentals] no data for {ticker} via slug '{meta['slug']}' — "
                  f"empty, unreachable, or identity mismatch (see above)")
            failed.append(ticker)
            time.sleep(1.5)
            continue
        store(client, ticker, data, prev_trend=prev.get(ticker))
        ok += 1
        time.sleep(1.5)   # polite pacing on screener.in (429s at 0.5s)
    print(f"[fundamentals] stored {ok}/{len(tracked)} tickers"
          + (f" — failed: {failed}" if failed else ""))
    return ok, len(tracked)


# Below this share of tickers stored, the run is broken, not unlucky. Screener's
# Oct-2026 header redesign made EVERY identity check fail and the job printed
# 'stored 0/55' night after night while exiting green — nobody saw it, and the
# dashboard quietly kept stale fundamentals. A failed exit makes GitHub mark the
# job red and email the repo owner.
MIN_STORED_SHARE = 0.5

if __name__ == "__main__":
    client = _client()
    stored, total = update_all(client) or (0, 0)
    if total and stored < max(1, int(total * MIN_STORED_SHARE)):
        print(f"🚨 [fundamentals] only {stored}/{total} tickers stored — the screener "
              f"parse is broken (layout change? identity check? rate limit?). Exiting "
              f"with an error so this run shows as FAILED.")
        sys.exit(1)
