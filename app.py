"""
Portfolio Dashboard (v2) — Supabase-backed
==========================================

Real-time NSE/BSE portfolio tracker. Holdings, watchlist, and notes all live
in Supabase. Two-role login: owner (full edit) and friend (watchlist + notes only).

Run locally: streamlit run app.py
Deploy:      Push to GitHub → Streamlit Cloud auto-redeploys.
"""

from __future__ import annotations

import re
from datetime import datetime, date
from urllib.parse import quote

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import yfinance as yf

import db
import signals
import xirr

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

# Tenant-aware page title/tab name. Read directly from os.environ here
# (not via _get_secret, which isn't defined yet at module-load time) --
# same env-first convention used everywhere else in this file.
import os as _os
_tenant = _os.environ.get("APP_TENANT", "vishal").strip().lower()
_PAGE_TITLE = "Lakshmi's Portfolio" if _tenant == "lakshmi" else "Vishal's Portfolio"

st.set_page_config(
    page_title=_PAGE_TITLE,
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

PRICE_CACHE_TTL = 300
INFO_CACHE_TTL = 60 * 60 * 6


# ---------------------------------------------------------------------------
# AUTH — two passwords, two roles
# ---------------------------------------------------------------------------

def _get_secret(key: str, default=None):
    # Read from os.environ FIRST: Streamlit Cloud injects secrets as env vars
    # too, and env reads are plain dict lookups with no file parsing or
    # watchers involved. The bisect showed a login flow reading os.environ
    # (stage 14) worked while the identical flow reading st.secrets crashed,
    # so environment is the primary source; st.secrets stays as fallback for
    # local runs.
    import os
    if key in os.environ:
        return os.environ[key]
    if hasattr(st, "secrets"):
        try:
            return st.secrets[key]
        except Exception:
            return default
    return default


def login_gate():
    """Block app until user enters a valid password.

    Each DEPLOYMENT serves exactly one tenant, set via APP_TENANT in that
    app's Streamlit secrets:
      - "vishal"  (default): Vishal's portfolio only; OWNER_PASSWORD (+ optional
                    FRIEND_PASSWORD read-only view)
      - "lakshmi": Lakshmi household only; LAKSHMI_PASSWORD; portfolios 2 & 3
    The two apps share code + database but neither can log into the other.

    The whole form lives inside an st.empty() placeholder so that on a
    successful login (which returns True and lets the SAME script run
    continue into the dashboard — no rerun, see the crash post-mortem)
    the form is erased before the dashboard renders below it.
    """
    tenant = _get_secret("APP_TENANT", "vishal").strip().lower()

    if st.session_state.get("role"):
        return True

    gate = st.empty()
    with gate.container():
        st.title("📈 Portfolio Dashboard")
        st.caption("Enter your password to continue")
        # st.form so the keyboard Enter key SUBMITS. With a bare st.button,
        # pressing Enter after typing only commits the text and reruns the
        # script -- the button reads as un-clicked and the login page
        # redraws, which users experience as "it bounced me back and I had
        # to enter the password twice, every time".
        with st.form("login_form"):
            pw = st.text_input("Password", type="password", key="pw_input")
            submitted = st.form_submit_button("Enter", type="primary")

        if tenant == "lakshmi":
            lakshmi_pw = _get_secret("LAKSHMI_PASSWORD")
            if not lakshmi_pw:
                st.error("⚠️ App not configured. Set LAKSHMI_PASSWORD in Streamlit secrets.")
                st.stop()
            if submitted:
                if pw == lakshmi_pw:
                    st.session_state.role = "lakshmi"
                    st.session_state.user = "Lakshmi"
                    st.session_state.portfolios = {2: "Lakshmi", 3: "Abinaya"}
                    st.session_state.portfolio_id = 2
                    gate.empty()   # wipe the login form before the dashboard draws
                    return True
                else:
                    st.error("Wrong password.")
            return False

        # Default tenant: Vishal's app
        owner_pw = _get_secret("OWNER_PASSWORD")
        friend_pw = _get_secret("FRIEND_PASSWORD")
        if not owner_pw:
            st.error("⚠️ App not configured. Set OWNER_PASSWORD in Streamlit secrets.")
            st.stop()
        if submitted:
            if pw == owner_pw:
                st.session_state.role = "owner"
                st.session_state.user = "Vishal"
                st.session_state.portfolios = {1: "Vishal"}
                st.session_state.portfolio_id = 1
                gate.empty()
                return True
            elif friend_pw and pw == friend_pw:
                st.session_state.role = "friend"
                st.session_state.user = _get_secret("FRIEND_NAME", "Friend")
                st.session_state.portfolios = {1: "Vishal"}
                st.session_state.portfolio_id = 1
                gate.empty()
                return True
            else:
                st.error("Wrong password.")
        return False


def portfolio_switcher():
    """Sidebar selector between the portfolios this login can access."""
    pfs = st.session_state.get("portfolios", {1: "Vishal"})
    if len(pfs) <= 1:
        return
    labels = list(pfs.values())
    ids = list(pfs.keys())
    current = st.session_state.get("portfolio_id", ids[0])
    idx = ids.index(current) if current in ids else 0
    choice = st.sidebar.radio("👤 Portfolio", labels, index=idx, key="pf_radio")
    new_id = ids[labels.index(choice)]
    if new_id != current:
        st.session_state.portfolio_id = new_id
        st.rerun()


def is_owner() -> bool:
    return st.session_state.get("role") == "owner"


def can_edit_holdings() -> bool:
    # Each login can edit its OWN portfolios; friend view stays read-only
    return st.session_state.get("role") in ("owner", "lakshmi")


def can_edit_watchlist() -> bool:
    return st.session_state.get("role") in ("owner", "friend", "lakshmi")


def can_edit_notes() -> bool:
    return st.session_state.get("role") in ("owner", "friend", "lakshmi")


# ---------------------------------------------------------------------------
# STOCK NAME / TICKER PARSING
# ---------------------------------------------------------------------------

def extract_yf_ticker(name: str):
    """'COMPANY (XNSE:SYMBOL)' -> 'SYMBOL.NS' for yfinance."""
    if not isinstance(name, str):
        return None
    m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", name)
    if not m:
        return None
    exch, sym = m.group(1), m.group(2).strip()
    return f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"


def short_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = re.sub(r"\s*\([^)]*\)\s*$", "", name)
    name = re.sub(r"\s+(LIMITED|LTD\.?|LTD)\s*$", "", name, flags=re.IGNORECASE)
    return name.strip().title()


def build_stock_name(company: str, exchange: str, symbol: str) -> str:
    """Combine user-entered fields into the canonical stock_name format."""
    company = company.strip().upper()
    if not company.endswith("LIMITED"):
        company = f"{company} LIMITED"
    exch_code = "XNSE" if exchange == "NSE" else "XBOM"
    return f"{company} ({exch_code}:{symbol.strip().upper()})"


def parse_stock_name(name: str):
    """Inverse of build_stock_name: 'COMPANY LIMITED (XNSE:SYMBOL)' ->
    (company, exchange, symbol), for pre-filling the edit form. Trailing
    'LIMITED' is stripped for a clean field (build_stock_name re-adds it)."""
    exchange, symbol = "NSE", ""
    m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", name or "")
    if m:
        exchange = "NSE" if m.group(1) == "XNSE" else "BSE"
        symbol = m.group(2).strip()
    company = re.sub(r"\s*\([^)]*\)\s*$", "", name or "").strip()
    company = re.sub(r"\s+LIMITED$", "", company, flags=re.IGNORECASE).strip()
    return company, exchange, symbol


# ---------------------------------------------------------------------------
# LIVE PRICES
# ---------------------------------------------------------------------------

from zoneinfo import ZoneInfo
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor

IST = ZoneInfo("Asia/Kolkata")


def now_ist():
    return datetime.now(IST)


def market_is_open() -> bool:
    """NSE/BSE regular session: 9:15–15:30 IST, Mon–Fri (holidays not tracked)."""
    n = now_ist()
    if n.weekday() >= 5:
        return False
    minutes = n.hour * 60 + n.minute
    return (9 * 60 + 15) <= minutes <= (15 * 60 + 30)


def last_expected_close_date():
    """The date whose closing prices we SHOULD be seeing right now."""
    n = now_ist()
    d = n.date()
    # today's close only exists if today is a weekday and the session has ended
    if n.weekday() < 5 and (n.hour * 60 + n.minute) >= (15 * 60 + 30):
        candidate = d
    else:
        candidate = d - timedelta(days=1)
    while candidate.weekday() >= 5:   # walk back over weekends
        candidate -= timedelta(days=1)
    return candidate


# Yahoo's BSE mirror is chronically stale for smallcaps. Dual-listed holdings
# get an NSE twin fallback (NSE data on Yahoo is reliable); BSE-only scrips
# get a direct BSE-API quote as last resort.
BSE_NSE_TWIN = {
    "532856.BO": "TIMETECHNO.NS",   # Time Technoplast
    "532365.BO": "DSSL.NS",         # Dynacons Systems & Solutions
}


def _fetch_bse_direct(scrip_code: str):
    """Last-traded price straight from BSE's own quote API. None on failure."""
    try:
        url = ("https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w"
               f"?Debtflag=&scripcode={scrip_code}&seriesid=")
        r = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bseindia.com/",
        })
        if r.status_code != 200:
            return None
        j = r.json()
        # Schema is loose; hunt for the LTP in the usual spots
        for path in (("CurrRate", "LTP"), ("Header", "LTP"), ("Cmpgn", "LTP")):
            node = j
            ok = True
            for k in path:
                node = node.get(k) if isinstance(node, dict) else None
                if node is None:
                    ok = False
                    break
            if ok:
                try:
                    v = float(str(node).replace(",", ""))
                    if v > 0:
                        return v
                except (TypeError, ValueError):
                    pass
        return None
    except Exception:
        return None


def _fetch_quote(t):
    """Yahoo quote endpoint — updates promptly at close, unlike daily bars."""
    try:
        fi = yf.Ticker(t).fast_info
        def _get(key, attr):
            try:
                v = fi[key]
            except Exception:
                v = getattr(fi, attr, None)
            try:
                v = float(v)
                return v if v > 0 else None
            except (TypeError, ValueError):
                return None
        lp = _get("last_price", "last_price")
        pc = _get("previous_close", "previous_close")
        return t, lp, pc
    except Exception:
        return t, None, None


@st.cache_data(ttl=PRICE_CACHE_TTL)
def fetch_live_prices(tickers: tuple) -> pd.DataFrame:
    fetched_at = now_ist().strftime("%H:%M:%S")
    if not tickers:
        df = pd.DataFrame(columns=["Ticker", "CMP", "Prev Close", "Day Change %",
                                    "Price Stale", "_fetched_at"])
        return df

    # 0) SME-tracked tickers (Sprint 3, hardened 13 Jul 2026): our own
    # bhavcopy table is authoritative for these -- so we don't ask Yahoo
    # about them AT ALL. Previously we asked, Yahoo failed with 12+ noisy
    # retries per refresh (404s, "possibly delisted", rate-limits), and
    # only then did we override with our own data. That retry storm was
    # hammering yfinance's cache ("database is locked"), slowing first
    # load by 30-60s, and spiking memory/CPU enough to crash the free-tier
    # instance mid-login (exit 139). Skip Yahoo for them entirely.
    try:
        bhav = db.get_sme_daily_prices(tuple(sorted(tickers)))
    except Exception:
        bhav = pd.DataFrame()
    sme_tracked = set(bhav["ticker"].unique()) if not bhav.empty else set()
    yahoo_tickers = tuple(t for t in tickers if t not in sme_tracked)

    # 1) Daily bars: prev-close reference + fallback + bar-date for staleness
    bar_close, bar_prev, bar_date = {}, {}, {}
    if yahoo_tickers:
        try:
            _end = now_ist().date() + timedelta(days=1)
            _start = _end - timedelta(days=14)
            data = yf.download(
                list(yahoo_tickers), start=str(_start), end=str(_end), interval="1d",
                progress=False, auto_adjust=False, group_by="ticker", threads=True,
            )
            for t in yahoo_tickers:
                try:
                    close = data["Close"].dropna() if len(yahoo_tickers) == 1 else data[t]["Close"].dropna()
                    if len(close):
                        bar_close[t] = float(close.iloc[-1])
                        bar_prev[t] = float(close.iloc[-2]) if len(close) >= 2 else float(close.iloc[-1])
                        bar_date[t] = pd.Timestamp(close.index[-1]).date()
                except Exception:
                    pass
        except Exception:
            pass

    # 2) Quote endpoint in parallel — the fresher source. max_workers kept
    # modest: 8 threads plus retries was part of the cache-contention storm.
    quotes = {}
    if yahoo_tickers:
        try:
            with ThreadPoolExecutor(max_workers=4) as ex:
                for t, lp, pc in ex.map(_fetch_quote, yahoo_tickers):
                    quotes[t] = (lp, pc)
        except Exception:
            pass

    expected = last_expected_close_date()
    MAX_PLAUSIBLE_MOVE = 0.25   # smallcap daily circuit ~20%; beyond this per
                                 # missing day, the quote is garbage, not a move
    rows = []
    for t in tickers:
        # SME-tracked: build directly from our own table, Yahoo never consulted
        if t in sme_tracked:
            sub = bhav[bhav["ticker"] == t].sort_values("price_date")
            latest = sub.iloc[-1]
            prev_row = sub.iloc[-2] if len(sub) >= 2 else latest
            rows.append({"Ticker": t, "CMP": float(latest["close"]),
                         "Prev Close": float(prev_row["close"]),
                         "Price Stale": False,
                         "Price Source": "bhavcopy (EOD, official NSE/BSE)",
                         "Price Date": str(latest["price_date"].date())})
            continue

        q_lp, q_pc = quotes.get(t, (None, None))
        b_close = bar_close.get(t)
        b_prev = bar_prev.get(t)
        bdate = bar_date.get(t)
        bar_is_fresh = bool(bdate and bdate >= expected)

        # Yahoo's quote endpoint is unreliable for BSE scrips (serves ancient
        # cached prices — seen twice: Kwality, Lehar). Never trust it for .BO;
        # the rescue pass gets those from BSE's own API or the NSE twin.
        quote_ok = bool(q_lp and q_lp > 0) and not t.endswith(".BO")
        if quote_ok and b_close:
            # Sanity band: reject quotes wildly off the last known bar
            # (Yahoo's quote endpoint serves ancient prices for some BSE scrips)
            days_gap = max(1, (expected - bdate).days if bdate else 1)
            band = MAX_PLAUSIBLE_MOVE * days_gap
            if abs(q_lp / b_close - 1) > band:
                quote_ok = False

        if bar_is_fresh and b_close:
            # After close with a same-date bar: the settled close is authoritative
            cmp_, prev, stale, source = b_close, b_prev, False, "bar (settled close)"
        elif quote_ok:
            cmp_ = q_lp
            prev = q_pc if (q_pc and q_pc > 0) else b_prev
            stale, source = False, "quote"
        else:
            cmp_, prev = b_close if b_close else np.nan, b_prev if b_prev else np.nan
            stale = bool(bdate and bdate < expected)
            source = "bar (STALE)" if stale else "bar"

        rows.append({"Ticker": t, "CMP": cmp_, "Prev Close": prev,
                     "Price Stale": stale, "Price Source": source,
                     "Price Date": str(bdate) if bdate else "—"})

    # --- Rescue pass for stale BSE scrips ---
    for row in rows:
        if not row["Price Stale"]:
            continue
        t = row["Ticker"]
        old_ref = row["CMP"]   # the stale-but-real bar price, our sanity anchor
        rescue = None

        # 1) BSE-only or dual-listed: BSE's own API first (native exchange price)
        if t.endswith(".BO"):
            lp = _fetch_bse_direct(t.split(".")[0])
            if lp:
                rescue = (lp, None)

        # 2) Dual-listed fallback: the NSE twin's quote (Yahoo NSE is reliable)
        twin = BSE_NSE_TWIN.get(t)
        if rescue is None and twin:
            _, lp, pc = _fetch_quote(twin)
            if lp and lp > 0:
                rescue = (lp, pc)

        # Accept only if sane vs the last known real price
        if rescue and old_ref and old_ref > 0:
            lp, pc = rescue
            if abs(lp / old_ref - 1) <= MAX_PLAUSIBLE_MOVE * 2:
                row["CMP"] = lp
                if pc and pc > 0:
                    row["Prev Close"] = pc
                row["Price Stale"] = False
                row["Price Source"] = ("BSE direct" if (t.endswith(".BO") and pc is None)
                                        else "NSE twin")
    out = pd.DataFrame(rows)
    out["Day Change %"] = ((out["CMP"] - out["Prev Close"]) / out["Prev Close"]) * 100
    out["_fetched_at"] = fetched_at
    return out


@st.cache_data(ttl=INFO_CACHE_TTL)
def fetch_fundamentals(tickers: tuple) -> pd.DataFrame:
    """Company fundamentals: Market Cap / PE / P/B / Sector.

    HARDENED 15-Jul-2026: no longer calls Yahoo live. Yahoo's .info /
    fast_info endpoints went from "sometimes fails" to "100% blank for
    every stock, including large caps" under today's load -- a wholesale
    block on Render's IP, not a per-ticker rate limit that more throttling
    could fix. Instead: read from fundamentals_daily, a table filled once
    daily by fundamentals.py (scrapes screener.in, a source that answers
    India-based requests fine). Instant, no live dependency, no crash risk.

    EV/EBITDA isn't in this table (unreliable on screener's free page
    across companies) -- shows as "--" same as before, now honestly
    rather than silently. Industry isn't tracked either (only Sector).
    """
    row_defaults = {"Sector": "Unknown", "Industry": "Unknown",
                    "Market Cap (Cr)": np.nan, "PE (live)": np.nan,
                    "P/B": np.nan, "EV/EBITDA": np.nan, "Book Value": np.nan}
    if not tickers:
        return pd.DataFrame(columns=["Ticker", *row_defaults])
    try:
        stored = db.get_fundamentals(tickers)
    except Exception:
        stored = pd.DataFrame()

    rows = []
    for t in tickers:
        row = {"Ticker": t, **row_defaults}
        if not stored.empty:
            match = stored[stored["ticker"] == t]
            if not match.empty:
                r = match.iloc[0]
                if pd.notna(r.get("market_cap_cr")):
                    row["Market Cap (Cr)"] = r["market_cap_cr"]
                if pd.notna(r.get("pe")):
                    row["PE (live)"] = r["pe"]
                if pd.notna(r.get("pb")):
                    row["P/B"] = r["pb"]
                if pd.notna(r.get("book_value")):
                    row["Book Value"] = r["book_value"]
                if r.get("sector"):
                    row["Sector"] = r["sector"]
        rows.append(row)
    return pd.DataFrame(rows)


@st.cache_data(ttl=INFO_CACHE_TTL)
def lookup_company(symbol: str, exchange: str) -> str:
    """Try to fetch company name from yfinance for a given symbol."""
    suffix = ".NS" if exchange == "NSE" else ".BO"
    try:
        info = yf.Ticker(f"{symbol.upper()}{suffix}").info or {}
        return info.get("longName") or info.get("shortName") or ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# COMPUTATION
# ---------------------------------------------------------------------------

ENTRY_ZONE_CACHE_TTL = 60 * 20   # daily 10/21-EMA barely moves intraday; a 20-min
                                 # cache keeps the zones "instant" on load without
                                 # a fresh per-ticker fetch every rerun (Render
                                 # free-tier / Yahoo-storm protection, rule #4)


@st.cache_data(ttl=ENTRY_ZONE_CACHE_TTL)
def fetch_entry_zones(tickers: tuple) -> pd.DataFrame:
    """Daily 10/21-DMA entry-tranche table for a set of tickers, computed on
    load (no button). SME names resolve via bhavcopy inside signals, mainboard
    via Yahoo. Empty df on failure — callers degrade gracefully."""
    if not tickers:
        return pd.DataFrame()
    try:
        return signals.entry_states_for_watchlist(tickers)
    except Exception:
        return pd.DataFrame()


def enrich_holdings(holdings_df: pd.DataFrame) -> pd.DataFrame:
    """Add Ticker, Short Name, live price columns + computed P&L."""
    if holdings_df is None or holdings_df.empty:
        # Brand-new portfolio: return an empty frame with the columns the UI reads
        return pd.DataFrame(columns=[
            "id", "stock_name", "Short Name", "Ticker", "quantity", "purchase_cost",
            "CMP", "Prev Close", "Day Change %", "Invested", "Current Value",
            "P&L", "P&L %", "Allocation %", "State", "State Display", "State Reason",
            "State Priority", "% from 10wEMA", "Vol vs 10wk", "10DMA", "21DMA",
            "% vs 21DMA",
        ])
    if holdings_df.empty:
        return pd.DataFrame()
    df = holdings_df.copy()
    df["Ticker"] = df["stock_name"].apply(extract_yf_ticker)
    df["Short Name"] = df["stock_name"].apply(short_name)
    df = df.dropna(subset=["Ticker"])
    if df.empty:
        return df

    tickers = tuple(sorted(df["Ticker"].unique()))
    prices = fetch_live_prices(tickers)
    fundamentals = fetch_fundamentals(tickers)

    df = df.merge(prices, on="Ticker", how="left")
    df = df.merge(fundamentals, on="Ticker", how="left")

    # P/B computed HERE from the dashboard's own live CMP / stored Book
    # Value (15-Jul-2026). The fetch job originally derived P/B using a
    # Yahoo price -- which is exactly the blocked dependency this whole
    # table exists to remove, so every P/B came back null. The dashboard
    # always has a CMP; Book Value changes quarterly. CMP/BV is the same
    # arithmetic every screener site uses, and it's always current.
    if "Book Value" in df.columns:
        computed_pb = df["CMP"] / df["Book Value"]
        df["P/B"] = df["P/B"].fillna(computed_pb).round(2)

    # Flowchart states (Lakshmi's TheWrap TA rules) — one per ticker
    with st.spinner("Computing flowchart states (weekly TA)..."):
        states = signals.states_for_holdings(tickers)
    df = df.merge(states, on="Ticker", how="left")

    # Daily entry/add tranches (10/21-DMA) — Lakshmi wants the 21-DMA add level
    # visible on holdings too, instantly (21-Jul-2026). Cached; SME-aware.
    with st.spinner("Computing entry tranches (10/21 DMA)..."):
        entry = fetch_entry_zones(tickers)
    if not entry.empty:
        keep = [c for c in ["Ticker", "10DMA", "21DMA", "% vs 21DMA",
                            "Entry Zone", "Entry Advice"] if c in entry.columns]
        df = df.merge(entry[keep], on="Ticker", how="left")

    # Delivery % (Sprint 3 fast-follow) — context column only, never gates
    # any state or alert. Empty/failed fetch = column simply shows "—".
    deliv = db.get_delivery_pct(tickers)
    if not deliv.empty:
        df = df.merge(deliv, on="Ticker", how="left")
        # force numeric dtype: stocks without delivery data must be true
        # NaN (renders as "—" via na_rep), never a printed "None"
        for c in ("Deliv % (last)", "Deliv % (4wk)"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    # % distance of LIVE price from the 10-week EMA (add-timing helper):
    # +20% = price 20% above the 10wEMA (extended); -10% = 10% below it.
    if "EMA10" in df.columns:
        df["% from 10wEMA"] = (df["CMP"] - df["EMA10"]) / df["EMA10"] * 100

    df["quantity"] = df["quantity"].astype(float)
    df["purchase_cost"] = df["purchase_cost"].astype(float)
    df["Invested"] = df["purchase_cost"] * df["quantity"]

    # IMPORTANT: when a stock has no live price (CMP is NaN — e.g. the SME
    # names Yahoo doesn't cover), do NOT let Current Value become NaN.
    # pandas .sum() silently treats NaN as 0, which was making those
    # holdings' entire market value vanish from portfolio totals while
    # their invested cost stayed counted — understating unrealised P&L by
    # the full amount invested in every no-price stock. Fallback: value
    # unknown-price holdings at cost (flat, P&L=0 for that row) instead of
    # erasing them. This is the honest "we don't know, so assume no gain
    # or loss" convention, not a fabricated number.
    has_price = df["CMP"].notna()
    df["Current Value"] = df["Invested"].where(~has_price, df["CMP"] * df["quantity"])
    df["P&L"] = df["Current Value"] - df["Invested"]
    df["P&L %"] = (df["P&L"] / df["Invested"]).replace([float("inf"), -float("inf")], 0) * 100
    df["Day P&L"] = ((df["CMP"] - df["Prev Close"]) * df["quantity"]).fillna(0)
    total = df["Current Value"].sum()
    df["Allocation %"] = (df["Current Value"] / total * 100) if total else 0
    return df


def compute_kpis(enriched: pd.DataFrame, realised: pd.DataFrame) -> dict:
    invested = enriched["Invested"].sum() if not enriched.empty else 0
    current = enriched["Current Value"].sum() if not enriched.empty else 0
    unrealised = current - invested
    unrealised_pct = (unrealised / invested * 100) if invested else 0
    day_pnl = enriched["Day P&L"].sum() if not enriched.empty else 0
    realised_total = realised["gain_loss"].sum() if (not realised.empty and "gain_loss" in realised.columns) else 0
    return {
        "invested": float(invested),
        "current": float(current),
        "unrealised": float(unrealised),
        "unrealised_pct": float(unrealised_pct),
        "day_pnl": float(day_pnl),
        "realised": float(realised_total),
        "total_pnl": float(unrealised + realised_total),
        "n_holdings": int(len(enriched)),
    }


# ---------------------------------------------------------------------------
# UI HELPERS
# ---------------------------------------------------------------------------

def fmt_inr(x, decimals=0):
    if pd.isna(x) or x is None:
        return "—"
    return f"₹{x:,.{decimals}f}"


def fmt_inr_compact(x):
    """Indian Cr/Lakh formatting for KPI cards -- keeps values short enough
    that Streamlit's metric widget never ellipsis-truncates them on narrow
    (mobile/tablet) screens. Full precision stays available via fmt_inr()
    everywhere else (tables, tooltips, exports).
    Sign is preserved for P&L figures that can be negative.
    """
    if pd.isna(x) or x is None:
        return "—"
    sign = "-" if x < 0 else ""
    ax = abs(x)
    if ax >= 1_00_00_000:      # >= 1 crore
        return f"{sign}₹{ax/1_00_00_000:.2f} Cr"
    if ax >= 1_00_000:          # >= 1 lakh
        return f"{sign}₹{ax/1_00_000:.2f} L"
    return f"{sign}₹{ax:,.0f}"


def fmt_pct(x, decimals=2):
    if pd.isna(x) or x is None:
        return "—"
    return f"{x:+.{decimals}f}%"


def color_pnl(val):
    if pd.isna(val) or val == 0:
        return "color: #888;"
    return "color: #16a34a;" if val > 0 else "color: #dc2626;"


# ---------------------------------------------------------------------------
# TAB: HOLDINGS
# ---------------------------------------------------------------------------

def tab_holdings(enriched: pd.DataFrame):
    if enriched.empty:
        st.info("No holdings yet. Add your first one below 👇")
    else:
        # --- Action-needed banner (states that demand attention) ---
        if "State" in enriched.columns:
            urgent = enriched[enriched["State"].isin(["EXIT", "BE CAUTIOUS"])]
            fading = enriched[enriched["State"] == "MOMENTUM FADING"]
            if not urgent.empty:
                names = ", ".join(urgent["Short Name"].tolist())
                st.error(f"🚨 **Action needed** — {names} "
                         f"({len(urgent)} holding{'s' if len(urgent) > 1 else ''} in EXIT / BE CAUTIOUS state)")
            if not fading.empty:
                names = ", ".join(fading["Short Name"].tolist())
                st.warning(f"🟣 **Momentum fading** — {names}")

        # ---- Column groups: pick the view for the job ----
        # 📊 Decision view = Lakshmi's three decision fields (State, 10wEMA dist,
        # volume) + price basics. Default view.
        COLUMN_VIEWS = {
            "📊 Decision view": [
                "Short Name", "State Display", "% from 10wEMA", "% vs 21DMA", "Vol vs 10wk",
                "Deliv % (4wk)", "CMP", "purchase_cost", "quantity",
            ],
            "💰 P&L view": [
                "Short Name", "State Display", "quantity", "purchase_cost", "Invested",
                "CMP", "Day Change %", "Current Value", "P&L", "P&L %", "Allocation %",
            ],
            "🔬 Fundamentals view": [
                "Short Name", "Ticker", "CMP", "Sector", "Market Cap (Cr)",
                "PE (live)", "P/B", "EV/EBITDA", "Allocation %",
            ],
            "🗂 Everything": [
                "Short Name", "Ticker", "State Display", "% from 10wEMA",
                "% vs 21DMA", "Vol vs 10wk",
                "Deliv % (last)", "Deliv % (4wk)",
                "quantity", "purchase_cost", "Invested", "CMP",
                "Day Change %", "Current Value", "P&L", "P&L %", "Allocation %",
                "Sector", "Market Cap (Cr)", "PE (live)", "P/B", "EV/EBITDA",
            ],
        }

        vc1, vc2 = st.columns([2, 1])
        with vc1:
            view_choice = st.radio(
                "Columns", list(COLUMN_VIEWS.keys()),
                horizontal=True, key="holdings_view",
                label_visibility="collapsed",
            )
        with vc2:
            sort_by = st.selectbox(
                "Sort by",
                ["State (urgent first)", "% from 10wEMA", "Vol vs 10wk", "Deliv % (4wk)",
                 "Invested",
                 "Day Change %", "P&L %", "P&L", "Allocation %", "Current Value", "Short Name"],
                index=0, key="holdings_sort",
            )

        view_cols = COLUMN_VIEWS[view_choice]
        view_cols = [c for c in view_cols if c in enriched.columns or c in ("State Display",)]
        sort_col_map = {"State (urgent first)": "State Priority"}
        actual_sort = sort_col_map.get(sort_by, sort_by)
        ascending = sort_by in ("Short Name", "State (urgent first)")

        view = enriched[[c for c in view_cols if c in enriched.columns]
                        + (["State Priority"] if "State Priority" in enriched.columns else [])].copy()
        view = view.rename(columns={"quantity": "Qty", "purchase_cost": "Avg Cost",
                                     "State Display": "State"})
        if actual_sort in ("State Priority",) and "State Priority" in view.columns:
            view = view.sort_values("State Priority", ascending=True, na_position="last")
        elif actual_sort in view.columns:
            view = view.sort_values(actual_sort, ascending=ascending, na_position="last")
        view = view.drop(columns=[c for c in ["State Priority"] if c in view.columns])

        def color_ema_distance(val):
            """Near/below the 10wEMA = add zone (green); far above = extended (amber/red)."""
            # Uniform favourable→unfavourable spectrum:
            # dark green → green → red → dark red
            if pd.isna(val):
                return "color: #888;"
            if val >= 10:
                return "color: #15803d; font-weight: 700;"   # strong momentum — most favourable
            if val >= 0:
                return "color: #22c55e;"                      # healthy, above the EMA
            if val > -5:
                return "color: #ef4444;"                      # slipped below the EMA
            return "color: #b91c1c; font-weight: 700;"        # well below — most unfavourable

        # Only style/format columns that actually exist — signals data can be
        # partially unavailable (yfinance hiccup, stale cache), and a styler
        # subset referencing a missing column raises KeyError.
        pnl_cols = [c for c in ["Day Change %", "P&L", "P&L %"] if c in view.columns]
        ema_cols = [c for c in ["% from 10wEMA"] if c in view.columns]
        deliv_cols = [c for c in ["Deliv % (last)", "Deliv % (4wk)"] if c in view.columns]

        def color_delivery(val):
            """High delivery = genuine hands taking stock home; low = intraday churn."""
            if pd.isna(val):
                return "color: #888;"
            if val >= 60:
                return "color: #15803d; font-weight: 700;"   # strong accumulation
            if val >= 40:
                return "color: #22c55e;"                      # healthy
            if val >= 25:
                return "color: #f59e0b;"                      # mixed
            return "color: #ef4444;"                          # mostly speculative churn

        styled = view.style.format({
            "Qty": "{:,.0f}", "Avg Cost": "₹{:,.2f}", "Invested": "₹{:,.0f}",
            "CMP": "₹{:,.2f}",
            "% from 10wEMA": "{:+.1f}%", "Vol vs 10wk": "{:.1f}x",
            "10DMA": "₹{:,.2f}", "21DMA": "₹{:,.2f}", "% vs 21DMA": "{:+.1f}%",
            "Deliv % (last)": "{:.0f}%", "Deliv % (4wk)": "{:.0f}%",
            "Day Change %": "{:+.2f}%", "Current Value": "₹{:,.0f}",
            "P&L": "₹{:,.0f}", "P&L %": "{:+.2f}%",
            "Allocation %": "{:.1f}%", "Market Cap (Cr)": "{:,.0f}",
            "PE (live)": "{:.2f}", "P/B": "{:.2f}", "EV/EBITDA": "{:.2f}",
        }, na_rep="—")
        if pnl_cols:
            styled = styled.map(color_pnl, subset=pnl_cols)
        if ema_cols:
            styled = styled.map(color_ema_distance, subset=ema_cols)
        if deliv_cols:
            styled = styled.map(color_delivery, subset=deliv_cols)
        st.dataframe(styled, width="stretch", height=520, hide_index=True)
        if deliv_cols:
            st.caption("Deliv % = share of traded quantity actually taken as delivery "
                       "(4wk = rolling average). High = genuine accumulation, low = intraday "
                       "churn. Context only — it never changes any state or alert. "
                       "Sourced from NSE/BSE official EOD files; — means no data yet for that stock.")

        # --- State detail expander: why is each stock in its state? ---
        if "State Reason" in enriched.columns:
            with st.expander("🔍 Why is each stock in its state? (flowchart detail)"):
                detail = enriched[["Short Name", "State Display", "State Reason"]].copy()
                detail = detail.rename(columns={"State Display": "State", "State Reason": "Reason"})
                if "State Priority" in enriched.columns:
                    detail = detail.loc[enriched.sort_values("State Priority").index]
                detail = detail.reset_index(drop=True)
                # st.table wraps long text; st.dataframe clips it inside expanders
                st.table(detail)

    if not can_edit_holdings():
        st.caption("🔒 Read-only view (logged in as friend)")
        return

    st.divider()
    st.subheader("Manage holdings")

    # Row 1: Buy actions
    cc1, cc2 = st.columns(2)
    with cc1:
        with st.expander("➕ Add new stock", expanded=False):
            _form_add_holding()
    with cc2:
        with st.expander("🔁 Buy more of existing", expanded=False):
            _form_buy_more(enriched)

    # Row 2: Sell / edit actions
    cc3, cc4 = st.columns(2)
    with cc3:
        with st.expander("💰 Mark as sold", expanded=False):
            _form_mark_as_sold(enriched)
    with cc4:
        with st.expander("✏️ Edit / 🗑️ Delete", expanded=False):
            _form_edit_delete_holding(enriched)


def _form_add_holding():
    with st.form("add_holding", clear_on_submit=True):
        c1, c2 = st.columns([2, 1])
        with c1:
            company = st.text_input("Company name", placeholder="e.g. Lincoln Pharmaceuticals")
        with c2:
            exchange = st.radio("Exchange", ["NSE", "BSE"], horizontal=True)
        symbol = st.text_input(
            "Symbol",
            placeholder="LINCOLN (NSE) or 524000 (BSE numeric code)",
            help="NSE symbol like LINCOLN, FINCABLES, KPEL. For BSE use the numeric code.",
        )
        c3, c4, c5 = st.columns(3)
        with c3:
            qty = st.number_input("Quantity", min_value=0.0, step=1.0, format="%.2f")
        with c4:
            cost = st.number_input("Purchase cost (avg)", min_value=0.0, step=0.01, format="%.2f")
        with c5:
            buy_dt = st.date_input("Buy date", value=date.today())

        notes = st.text_input("Notes (optional)", placeholder="Any context — thesis, source, etc.")
        submitted = st.form_submit_button("Add", type="primary")

    if submitted:
        if not company or not symbol or qty <= 0 or cost <= 0:
            st.error("Fill in company, symbol, quantity (>0), and cost (>0).")
            return
        try:
            stock_name = build_stock_name(company, exchange, symbol)
            db.add_holding(stock_name, qty, cost, buy_date=buy_dt, notes=notes or None)
            if db.graduate_from_watchlist(stock_name):
                st.info("🎓 Removed from watchlist — it's a holding now.")
            st.success(f"✅ Added {qty:g} × {short_name(stock_name)} @ ₹{cost:,.2f}")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to add: {e}")


def _form_buy_more(enriched: pd.DataFrame):
    """Form to add more shares to an existing holding. Recalcs weighted avg."""
    if enriched.empty:
        st.caption("No existing holdings to add to.")
        return

    options = enriched[["id", "Short Name", "quantity", "purchase_cost", "CMP"]].copy()
    options["label"] = options.apply(
        lambda r: f"{r['Short Name']} (have {r['quantity']:.0f} @ avg ₹{r['purchase_cost']:,.2f})",
        axis=1,
    )
    pick = st.selectbox("Select holding to add to", options["label"].tolist(), key="buymore_pick")
    if not pick:
        return
    row = options[options["label"] == pick].iloc[0]

    with st.form("buy_more_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            add_qty = st.number_input("Additional quantity", min_value=0.0, step=1.0, format="%.2f")
        with c2:
            add_price = st.number_input(
                "Buy price per share", min_value=0.0, step=0.01, format="%.2f",
                value=float(row["CMP"]) if pd.notna(row["CMP"]) else 0.0,
            )
        with c3:
            buy_dt = st.date_input("Transaction date", value=date.today(), key="buymore_date")

        notes = st.text_input(
            "Notes (optional)",
            placeholder="Why are you adding here? Thesis, dip, etc.",
        )

        # Live preview of new weighted average
        if add_qty > 0 and add_price > 0:
            old_qty = float(row["quantity"])
            old_cost = float(row["purchase_cost"])
            new_qty = old_qty + add_qty
            new_avg = ((old_qty * old_cost) + (add_qty * add_price)) / new_qty
            st.info(
                f"📊 **Preview:** {old_qty:.0f} @ ₹{old_cost:,.2f}  +  "
                f"{add_qty:.0f} @ ₹{add_price:,.2f}  →  "
                f"**{new_qty:.0f} @ ₹{new_avg:,.2f}**  "
                f"(invested ₹{new_qty * new_avg:,.0f})"
            )

        submitted = st.form_submit_button("🔁 Buy more", type="primary")

    if submitted:
        if add_qty <= 0 or add_price <= 0:
            st.error("Quantity and price must both be greater than 0.")
            return
        try:
            result = db.buy_more(
                holding_id=int(row["id"]),
                additional_qty=add_qty,
                price=add_price,
                transaction_date=buy_dt,
                notes=notes or None,
            )
            st.success(
                f"✅ Added {add_qty:.0f} more shares of {row['Short Name']}. "
                f"New position: {result['new_qty']:.0f} units @ avg ₹{result['new_avg']:,.2f}"
            )
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


def _form_mark_as_sold(enriched: pd.DataFrame):
    if enriched.empty:
        st.caption("No holdings to sell.")
        return
    options = enriched[["id", "Short Name", "quantity", "CMP"]].copy()
    options["label"] = options.apply(
        lambda r: f"{r['Short Name']} ({r['quantity']:.0f} units @ live ₹{r['CMP']:,.2f})", axis=1
    )
    pick = st.selectbox("Select holding", options["label"].tolist(), key="sell_pick")
    if not pick:
        return
    row = options[options["label"] == pick].iloc[0]

    with st.form("sell_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            sell_price = st.number_input(
                "Selling price", min_value=0.0, step=0.01, format="%.2f",
                value=float(row["CMP"]) if pd.notna(row["CMP"]) else 0.0,
            )
        with c2:
            sell_dt = st.date_input("Sale date", value=date.today())
        # Quantity is always editable (pre-filled with the full holding).
        # Reducing it = partial sell; leaving it = full sell. NOTE: the old
        # "Partial sell?" checkbox could never work here -- widgets inside an
        # st.form don't rerun until submit, so ticking it couldn't un-disable
        # the quantity field. Discovered by Lakshmi mid-sell, 13 Jul 2026.
        qty_sold = st.number_input(
            "Quantity sold", min_value=1.0,
            max_value=float(row["quantity"]), value=float(row["quantity"]),
            step=1.0,
            help="Pre-filled with your full holding — reduce it for a partial sell.",
        )
        # Sprint 3: trade journal — WHY is this exit happening?
        c5, c6 = st.columns(2)
        with c5:
            sell_reason = st.selectbox(
                "Why are you selling?", db.JOURNAL_REASONS,
                help="Logged in the trade journal. The audit engine checks back "
                     "30/60/90 days later to see what the stock did after the exit.",
            )
        with c6:
            sell_notes = st.text_input("Notes (optional)", max_chars=200,
                                       placeholder="e.g. Lakshmi's call after weekly close")
        submitted = st.form_submit_button("Mark as sold", type="primary")

    if submitted:
        if sell_price <= 0:
            st.error("Selling price must be > 0")
            return
        try:
            db.mark_as_sold(
                holding_id=int(row["id"]),
                selling_price=sell_price,
                sale_date=sell_dt,
                partial_quantity=qty_sold,   # == full holding -> full sell (db handles it)
                reason=sell_reason,
                notes=sell_notes.strip() or None,
            )
            st.success("✅ Trade closed — moved to Realised P&L")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


def _form_edit_delete_holding(enriched: pd.DataFrame):
    if enriched.empty:
        st.caption("No holdings to edit.")
        return
    options = enriched[["id", "Short Name", "quantity", "purchase_cost"]].copy()
    options["label"] = options.apply(
        lambda r: f"{r['Short Name']} ({r['quantity']:.0f} units @ ₹{r['purchase_cost']:,.2f})", axis=1
    )
    pick = st.selectbox("Select holding", options["label"].tolist(), key="edit_pick")
    if not pick:
        return
    row = options[options["label"] == pick].iloc[0]

    with st.form("edit_form"):
        c1, c2 = st.columns(2)
        with c1:
            new_qty = st.number_input("Quantity", value=float(row["quantity"]), min_value=0.0, step=1.0)
        with c2:
            new_cost = st.number_input("Avg purchase cost", value=float(row["purchase_cost"]),
                                        min_value=0.0, step=0.01, format="%.2f")
        c3, c4 = st.columns(2)
        save = c3.form_submit_button("💾 Save changes", type="primary")
        delete = c4.form_submit_button("🗑️ Delete this holding")

    if save:
        try:
            db.update_holding(int(row["id"]), quantity=new_qty, purchase_cost=new_cost,
                              amount_invested=round(new_qty * new_cost, 2))
            st.success("✅ Updated")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")
    if delete:
        try:
            db.delete_holding(int(row["id"]))
            st.success("🗑️ Deleted")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


# ---------------------------------------------------------------------------
# TAB: ALLOCATION
# ---------------------------------------------------------------------------

def tab_allocation(enriched: pd.DataFrame, k: dict):
    if enriched.empty:
        st.info("Add holdings to see allocation.")
        return
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("By Stock")
        fig = px.pie(enriched, values="Current Value", names="Short Name", hole=0.45)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False, height=480, margin=dict(t=10, b=10))
        st.plotly_chart(fig, width="stretch")
    with c2:
        st.subheader("By Sector")
        sec = enriched.groupby("Sector", dropna=False)["Current Value"].sum().reset_index()
        sec = sec.sort_values("Current Value", ascending=True)
        fig2 = px.bar(sec, x="Current Value", y="Sector", orientation="h",
                      text=sec["Current Value"].apply(lambda x: f"₹{x/1000:,.0f}k"))
        fig2.update_layout(height=480, margin=dict(t=10, b=10), xaxis_title="", yaxis_title="")
        st.plotly_chart(fig2, width="stretch")

    st.subheader("Concentration")
    top3 = enriched.nlargest(3, "Current Value")["Allocation %"].sum()
    top5 = enriched.nlargest(5, "Current Value")["Allocation %"].sum()
    cc1, cc2, cc3 = st.columns(3)
    cc1.metric("Top 3 holdings", f"{top3:.1f}%")
    cc2.metric("Top 5 holdings", f"{top5:.1f}%")
    cc3.metric("# of holdings", k["n_holdings"])


# ---------------------------------------------------------------------------
# TAB: WATCHLIST
# ---------------------------------------------------------------------------

def tab_watchlist():
    wl = db.get_watchlist()
    if wl.empty:
        st.info("Watchlist is empty. Add stocks to track below.")
    else:
        wl_view = wl.copy()
        wl_view["Ticker"] = wl_view["stock_name"].apply(extract_yf_ticker)
        wl_view["Short Name"] = wl_view["stock_name"].apply(short_name)
        tickers = tuple(t for t in wl_view["Ticker"].dropna().unique())
        if tickers:
            prices = fetch_live_prices(tickers)
            wl_view = wl_view.merge(prices, on="Ticker", how="left")
            # Lakshmi's staged-entry system: 10DMA = 1st tranche, 21DMA = final.
            # Computed INSTANTLY on load now (21-Jul-2026, Lakshmi's request) —
            # cached (fetch_entry_zones) so it stays light, and SME-aware so the
            # bhavcopy names resolve too (the old Yahoo-only path skipped them).
            with st.spinner("Computing entry zones (10/21 DMA)…"):
                entries = fetch_entry_zones(tickers)
            if entries is not None and not entries.empty:
                wl_view = wl_view.merge(
                    entries.drop(columns=["CMP (d)"], errors="ignore"),
                    on="Ticker", how="left")
            if "target_buy_price" in wl_view.columns:
                wl_view["Distance to Target %"] = (
                    (wl_view["CMP"] - wl_view["target_buy_price"]) / wl_view["target_buy_price"] * 100
                )
        # Lakshmi 22-Jul-2026: show the % DISTANCE to the DMAs, not the ₹ levels —
        # "how far am I from the entry zone" is the decision, the rupee value isn't.
        cols = ["Short Name", "Ticker", "CMP", "Entry Advice",
                "% vs 10DMA", "% vs 21DMA", "Day Change %",
                "target_buy_price", "Distance to Target %", "notes", "added_by"]
        cols = [c for c in cols if c in wl_view.columns]
        styled = (
            wl_view[cols].rename(columns={
                "target_buy_price": "Target Buy", "notes": "Notes", "added_by": "Added By",
            }).style.format({
                "CMP": "₹{:,.2f}", "Day Change %": "{:+.2f}%",
                "Target Buy": "₹{:,.2f}", "Distance to Target %": "{:+.2f}%",
                "% vs 10DMA": "{:+.1f}%", "% vs 21DMA": "{:+.1f}%",
            }, na_rep="—").map(color_pnl, subset=["Day Change %"])
        )
        st.dataframe(styled, width="stretch", hide_index=True, height=420)

    if not can_edit_watchlist():
        return

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        with st.expander("➕ Add to watchlist", expanded=False):
            _form_add_watchlist()
    with c2:
        with st.expander("🗑️ Remove from watchlist", expanded=False):
            _form_remove_watchlist(wl)
    with st.expander("✏️ Edit a watchlist item", expanded=False):
        _form_edit_watchlist(wl)


def _form_add_watchlist():
    with st.form("add_wl", clear_on_submit=True):
        c1, c2 = st.columns([2, 1])
        with c1:
            company = st.text_input("Company name", placeholder="e.g. Lincoln Pharmaceuticals")
        with c2:
            exchange = st.radio("Exchange", ["NSE", "BSE"], horizontal=True, key="wl_exch")
        symbol = st.text_input("Symbol", placeholder="LINCOLN")
        c3, c4 = st.columns(2)
        with c3:
            target = st.number_input("Target buy price (optional)", min_value=0.0, step=0.01,
                                      format="%.2f", value=0.0)
        with c4:
            st.write("")
            st.write("")
        note = st.text_area("Notes / thesis", placeholder="Why are we watching this?", height=80)
        submitted = st.form_submit_button("Add", type="primary")

    if submitted:
        if not company or not symbol:
            st.error("Company and symbol are required.")
            return
        try:
            stock_name = build_stock_name(company, exchange, symbol)
            db.add_watchlist(
                stock_name, target_buy_price=target if target > 0 else None,
                notes=note or None, added_by=st.session_state.get("user", "Unknown"),
            )
            st.success("✅ Added to watchlist")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


def _form_remove_watchlist(wl: pd.DataFrame):
    if wl.empty:
        st.caption("Watchlist is empty.")
        return
    options = wl.copy()
    options["label"] = options["stock_name"].apply(short_name)
    pick = st.selectbox("Select to remove", options["label"].tolist(), key="rm_wl_pick")
    if st.button("🗑️ Remove", key="rm_wl_btn"):
        try:
            row = options[options["label"] == pick].iloc[0]
            db.delete_watchlist(int(row["id"]))
            st.success("Removed")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


def _form_edit_watchlist(wl: pd.DataFrame):
    """Edit an existing watchlist item: fix a mistyped symbol/company, switch
    exchange, or update the target price / thesis. The item picker sits OUTSIDE
    the form so changing the selection re-fills the fields; the form key is
    keyed to the row id so Streamlit re-applies the defaults on each switch."""
    if wl.empty:
        st.caption("Watchlist is empty.")
        return
    options = wl.copy()
    options["label"] = options["stock_name"].apply(short_name)
    pick = st.selectbox("Select item to edit", options["label"].tolist(), key="edit_wl_pick")
    row = options[options["label"] == pick].iloc[0]
    rid = int(row["id"])
    company0, exch0, sym0 = parse_stock_name(row["stock_name"])
    target0 = float(row["target_buy_price"]) if pd.notna(row.get("target_buy_price")) else 0.0
    notes0 = row.get("notes") or ""

    with st.form(f"edit_wl_form_{rid}"):
        c1, c2 = st.columns([2, 1])
        with c1:
            company = st.text_input("Company name", value=company0)
        with c2:
            exchange = st.radio("Exchange", ["NSE", "BSE"],
                                index=0 if exch0 == "NSE" else 1, horizontal=True)
        symbol = st.text_input("Symbol", value=sym0,
                               help="No spaces — e.g. JITFINFRA, not 'JITF INFRA'")
        target = st.number_input("Target buy price (0 = clear it)", min_value=0.0,
                                 step=0.01, format="%.2f", value=target0)
        note = st.text_area("Notes / thesis", value=notes0, height=80)
        submitted = st.form_submit_button("💾 Save changes", type="primary")

    if submitted:
        if not company or not symbol.strip():
            st.error("Company and symbol are required.")
            return
        # Guard the exact bug that made this feature necessary: a space in the
        # symbol breaks the price lookup (was 'JITF INFRA.NS' -> no CMP).
        if " " in symbol.strip():
            st.error("Symbol can't contain spaces — that breaks the price lookup. "
                     "e.g. use JITFINFRA, not 'JITF INFRA'.")
            return
        try:
            new_name = build_stock_name(company, exchange, symbol)
            db.update_watchlist(rid, stock_name=new_name,
                                target_buy_price=target if target > 0 else None,
                                notes=note or None)
            st.success("✅ Watchlist item updated")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


# ---------------------------------------------------------------------------
# TAB: REALISED
# ---------------------------------------------------------------------------

def tab_realised(realised: pd.DataFrame):
    if realised.empty:
        st.info("No closed trades yet.")
        return

    total = realised["gain_loss"].sum()
    wins = (realised["gain_loss"] > 0).sum()
    losses = (realised["gain_loss"] < 0).sum()
    win_rate = wins / max(wins + losses, 1) * 100

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total realised P&L", fmt_inr(total))
    c2.metric("Trades closed", len(realised))
    c3.metric("Win rate", f"{win_rate:.0f}%")
    c4.metric("Avg trade P&L", fmt_inr(realised["gain_loss"].mean()))

    view = realised.copy()
    view["Short Name"] = view["stock_name"].apply(short_name)
    cols_order = ["Short Name", "quantity", "purchase_cost", "selling_price",
                  "sale_consideration", "gain_loss", "pct_gain_loss",
                  "sale_date", "buy_date", "no_of_days"]
    cols_order = [c for c in cols_order if c in view.columns]
    styled = (
        view[cols_order].rename(columns={
            "quantity": "Qty", "purchase_cost": "Buy Price", "selling_price": "Sell Price",
            "sale_consideration": "Sale Amount", "gain_loss": "P&L",
            "pct_gain_loss": "P&L %", "sale_date": "Sale Date",
            "buy_date": "Buy Date", "no_of_days": "Days Held",
        }).style.format({
            "Qty": "{:,.0f}", "Buy Price": "₹{:,.2f}", "Sell Price": "₹{:,.2f}",
            "Sale Amount": "₹{:,.0f}", "P&L": "₹{:,.0f}", "P&L %": "{:+.2%}",
        }, na_rep="—").map(color_pnl, subset=["P&L", "P&L %"])
    )
    st.dataframe(styled, width="stretch", hide_index=True, height=460)

    cb, cw = st.columns(2)
    with cb:
        st.markdown("**🏆 Top 5 winners**")
        st.dataframe(realised.nlargest(5, "gain_loss")[["stock_name", "gain_loss", "pct_gain_loss"]],
                     width="stretch", hide_index=True)
    with cw:
        st.markdown("**💀 Top 5 losers**")
        st.dataframe(realised.nsmallest(5, "gain_loss")[["stock_name", "gain_loss", "pct_gain_loss"]],
                     width="stretch", hide_index=True)

    # --- Sprint 3: Trade Journal + 30/60/90-day exit audits ---
    st.markdown("---")
    st.markdown("**📓 Trade Journal — what happened after we sold**")
    journal = db.get_trade_journal()
    if journal.empty:
        st.caption("No journaled exits yet. Every sell from now on asks "
                   "'Why are you selling?' and lands here — then the audit engine "
                   "checks the price 30/60/90 days later.")
    else:
        jv = journal.copy()
        jv["Stock"] = jv["ticker"].apply(short_name)

        def _audit_cell(row, col):
            p = row.get(col)
            if p is None or pd.isna(p):
                return "⏳ pending"
            chg = (float(p) - float(row["exit_price"])) / float(row["exit_price"]) * 100
            # Stock fell after we sold → the exit saved money; rose → it cost us
            verdict = "saved" if chg < 0 else "cost"
            return f"₹{float(p):,.1f} ({verdict} {abs(chg):.1f}%)"

        for col in ("price_30d", "price_60d", "price_90d"):
            jv[col.replace("price_", "After ").replace("d", " days")] = jv.apply(
                lambda r, c=col: _audit_cell(r, c), axis=1)
        show = jv[["Stock", "exit_date", "exit_price", "qty_sold", "reason",
                   "After 30 days", "After 60 days", "After 90 days", "notes"]].rename(
            columns={"exit_date": "Exit Date", "exit_price": "Exit ₹",
                     "qty_sold": "Qty", "reason": "Reason", "notes": "Notes"})
        st.dataframe(show.style.format({"Exit ₹": "₹{:,.2f}", "Qty": "{:,.0f}"},
                                       na_rep="—"),
                     width="stretch", hide_index=True)
        st.caption("'saved X%' = the stock fell after the exit (the rule protected you). "
                   "'cost X%' = it kept rising (the exit left money on the table). "
                   "Audits fill in automatically as each window matures.")


# ---------------------------------------------------------------------------
# TAB: HISTORY
# ---------------------------------------------------------------------------

def tab_history(k: dict):
    snaps = db.get_snapshots()
    if is_owner():
        c1, c2 = st.columns([1, 3])
        with c1:
            if st.button("📸 Take snapshot now", type="primary"):
                try:
                    db.upsert_snapshot(k)
                    st.success(f"Snapshot saved for {date.today().isoformat()}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")

    if snaps.empty:
        st.info("No snapshots yet. Take your first one above to start the equity curve.")
        return

    st.caption(f"{len(snaps)} snapshots · "
               f"first: {snaps['snapshot_date'].min().date()} · "
               f"latest: {snaps['snapshot_date'].max().date()}")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=snaps["snapshot_date"], y=snaps["invested"], name="Invested",
        mode="lines", line=dict(color="#94a3b8", dash="dash"),
    ))
    fig.add_trace(go.Scatter(
        x=snaps["snapshot_date"], y=snaps["current_value"], name="Current Value",
        mode="lines+markers", line=dict(color="#0ea5e9", width=3),
        fill="tonexty", fillcolor="rgba(14,165,233,0.1)",
    ))
    fig.update_layout(title="Portfolio value over time", height=420,
                       margin=dict(t=50, b=10), hovermode="x unified",
                       yaxis_title="₹", xaxis_title="")
    st.plotly_chart(fig, width="stretch")

    st.subheader("Snapshot history")
    show = snaps.iloc[::-1].copy()
    show["snapshot_date"] = show["snapshot_date"].dt.strftime("%Y-%m-%d")
    st.dataframe(show, width="stretch", hide_index=True)


# ---------------------------------------------------------------------------
# TAB: NOTES
# ---------------------------------------------------------------------------

def tab_transactions():
    """Chronological log of every buy and sell, with filters + Excel export."""
    import io

    tx = db.get_transactions()
    if tx.empty:
        st.info("No transactions yet. They'll appear here every time you buy or sell.")
        return

    # ---- Top KPIs ----
    n_buy = (tx["transaction_type"] == "buy").sum()
    n_sell = (tx["transaction_type"] == "sell").sum()
    total_invested_ever = tx.loc[tx["transaction_type"] == "buy", "amount"].sum()
    total_realised_ever = tx.loc[tx["transaction_type"] == "sell", "amount"].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total transactions", len(tx))
    k2.metric("Buys", int(n_buy))
    k3.metric("Sells", int(n_sell))
    k4.metric("Cash deployed (lifetime)", fmt_inr(total_invested_ever))

    st.divider()

    # ---- Filters ----
    f1, f2, f3 = st.columns([2, 1, 2])
    with f1:
        stocks = ["All"] + sorted(tx["stock_name"].apply(short_name).unique().tolist())
        stock_filter = st.selectbox("Filter by stock", stocks, key="tx_stock_filter")
    with f2:
        type_filter = st.selectbox("Type", ["All", "Buy only", "Sell only"], key="tx_type_filter")
    with f3:
        min_date = tx["transaction_date"].min().date()
        max_date = tx["transaction_date"].max().date()
        date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date, max_value=max_date,
            key="tx_date_range",
        )

    # Apply filters
    filtered = tx.copy()
    if stock_filter != "All":
        filtered = filtered[filtered["stock_name"].apply(short_name) == stock_filter]
    if type_filter == "Buy only":
        filtered = filtered[filtered["transaction_type"] == "buy"]
    elif type_filter == "Sell only":
        filtered = filtered[filtered["transaction_type"] == "sell"]
    if isinstance(date_range, tuple) and len(date_range) == 2:
        d0, d1 = date_range
        filtered = filtered[
            (filtered["transaction_date"].dt.date >= d0) &
            (filtered["transaction_date"].dt.date <= d1)
        ]

    # ---- Display ----
    view = filtered.copy()
    view["Stock"] = view["stock_name"].apply(short_name)
    view["Date"] = view["transaction_date"].dt.strftime("%Y-%m-%d")
    view["Type"] = view["transaction_type"].str.upper()
    view = view.rename(columns={
        "quantity": "Qty", "price": "Price", "amount": "Amount", "notes": "Notes",
    })
    show_cols = ["Date", "Stock", "Type", "Qty", "Price", "Amount", "Notes"]

    st.caption(f"Showing {len(view)} of {len(tx)} transactions")

    def color_type(v):
        if v == "BUY":
            return "background-color: #ecfdf5; color: #065f46; font-weight: 600;"
        if v == "SELL":
            return "background-color: #fef2f2; color: #991b1b; font-weight: 600;"
        return ""

    styled = (
        view[show_cols].style.format({
            "Qty": "{:,.0f}", "Price": "₹{:,.2f}", "Amount": "₹{:,.2f}",
        }, na_rep="—").map(color_type, subset=["Type"])
    )
    st.dataframe(styled, width="stretch", hide_index=True, height=500)

    # ---- Excel export ----
    st.divider()
    c1, c2 = st.columns([3, 1])
    with c2:
        buf = io.BytesIO()
        export_df = view[show_cols].copy()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            export_df.to_excel(writer, sheet_name="Transactions", index=False)
            # Auto-size columns
            ws = writer.sheets["Transactions"]
            for col_cells in ws.columns:
                max_len = max(
                    (len(str(c.value)) for c in col_cells if c.value is not None),
                    default=10,
                )
                ws.column_dimensions[col_cells[0].column_letter].width = min(max(max_len + 2, 10), 50)

        fname = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.download_button(
            "⬇️ Download Excel",
            data=buf.getvalue(),
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            width="stretch",
        )
    with c1:
        st.caption(
            "💡 Tip: Use the filters above to narrow down what gets exported. "
            "Useful for monthly reconciliation with INDmoney, tax filing, or sharing with your CA."
        )


    # ---- Manage entries (owner/lakshmi only): delete mistaken rows ----
    if st.session_state.get("role") in ("owner", "lakshmi"):
        with st.expander("🗑 Delete a transaction entry"):
            st.caption(
                "For cleaning up mistaken or test entries only. Deleting a "
                "genuine buy/sell makes XIRR and history less accurate — "
                "when in doubt, leave it."
            )
            opts = {
                f"#{int(r['id'])} · {r['transaction_date'].strftime('%Y-%m-%d')} · "
                f"{r['transaction_type'].upper()} · {short_name(r['stock_name'])} · "
                f"qty {r['quantity']:g} @ ₹{r['price']:g}": int(r["id"])
                for _, r in tx.sort_values("transaction_date", ascending=False).iterrows()
            }
            sel = st.selectbox("Entry to delete", ["— select —"] + list(opts.keys()),
                                key="tx_del_select")
            confirm = st.checkbox("Yes, I'm sure — delete this entry permanently",
                                   key="tx_del_confirm")
            if st.button("Delete entry", type="primary", key="tx_del_btn",
                          disabled=(sel == "— select —" or not confirm)):
                db.delete_transaction(opts[sel])
                st.success("Entry deleted.")
                st.rerun()

def tab_notes():
    notes = db.get_notes()

    if can_edit_notes():
        with st.expander("✍️ Add a new note", expanded=False):
            with st.form("add_note", clear_on_submit=True):
                txt = st.text_area("Note", height=120,
                                    placeholder="Investment idea, market thoughts, action items…")
                submit = st.form_submit_button("Post", type="primary")
            if submit:
                if not txt.strip():
                    st.error("Note can't be empty.")
                else:
                    try:
                        db.add_note(author=st.session_state.get("user", "Unknown"),
                                     note=txt.strip())
                        st.success("✅ Posted")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

    if notes.empty:
        st.info("No notes yet.")
        return

    for _, n in notes.iterrows():
        with st.container(border=True):
            cols = st.columns([5, 1])
            with cols[0]:
                d = n.get("note_date", "")
                if d and not isinstance(d, str):
                    d = str(d)
                st.markdown(f"**{n.get('author','')}** · _{d}_")
                st.markdown(n.get("note", ""))
            with cols[1]:
                if can_edit_notes() and (is_owner() or n.get("author") == st.session_state.get("user")):
                    if st.button("🗑️", key=f"del_note_{n['id']}", help="Delete this note"):
                        try:
                            db.delete_note(int(n["id"]))
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

# (Bulk-import feature removed after onboarding — data now managed via SQL)


def main():
    if not login_gate():
        return

    # Sidebar
    st.sidebar.title("⚙️ Settings")
    st.sidebar.caption(f"👤 Logged in as: **{st.session_state.get('user','?')}**  "
                        f"({st.session_state.get('role','?')})")

    portfolio_switcher()

    if st.sidebar.button("🔄 Refresh prices"):
        st.cache_data.clear()
        st.rerun()

    # Auto-refresh: reruns the app every 5 minutes so prices stay current
    # during market hours without manual refreshing.
    auto = st.sidebar.toggle("⏱ Auto-refresh (5 min)", value=False,
                              help="Keeps prices updating while this tab is open. "
                                   "Yahoo data is ~15 min delayed regardless.")
    if auto:
        if hasattr(st, "fragment"):
            @st.fragment(run_every=PRICE_CACHE_TTL)
            def _auto_refresh_tick():
                st.rerun(scope="app")
            _auto_refresh_tick()
        else:
            st.sidebar.caption("Auto-refresh needs a newer Streamlit; use 🔄 instead.")

    if st.sidebar.button("Logout"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

    st.sidebar.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")
    st.sidebar.caption(f"Prices cached: {PRICE_CACHE_TTL // 60} min")
    st.sidebar.toggle("🔍 Price diagnostics", value=False, key="show_price_diag",
                       help="Per-stock price source and date — for debugging "
                            "when numbers look off vs INDmoney.")

    # Load data from Supabase
    try:
        holdings_raw = db.get_holdings()
        realised = db.get_realised()
    except Exception as e:
        st.error(f"❌ Database error: {e}")
        st.info("Check your Supabase credentials in Streamlit secrets and that the schema has been created.")
        return

    # Enrich with live prices
    with st.spinner("Loading live prices..."):
        enriched = enrich_holdings(holdings_raw)

    k = compute_kpis(enriched, realised)

    # Header
    st.title("📈 Portfolio Dashboard")

    # When were these prices actually fetched? (IST) + market status
    fetched_at = None
    if not enriched.empty and "_fetched_at" in enriched.columns:
        fetched_at = enriched["_fetched_at"].iloc[0]

    stale_names = []
    if not enriched.empty and "Price Stale" in enriched.columns:
        stale_names = enriched.loc[enriched["Price Stale"] == True, "Short Name"].tolist()

    if market_is_open():
        badge = "🟢 Market OPEN"
        note = "prices refresh every 5 min (Yahoo feed, ~15 min delayed)"
    else:
        badge = "🔴 Market CLOSED"
        note = ("last close loaded for all holdings — safe to compare with INDmoney"
                if not stale_names else
                "last close loaded, EXCEPT the stocks flagged below")
    pf_name = st.session_state.get("portfolios", {}).get(
        st.session_state.get("portfolio_id", 1), "")
    st.caption(f"**{pf_name}** · Tracking {k['n_holdings']} holdings · {badge} · "
                f"Prices as of **{fetched_at or '—'} IST** · {note}")

    if stale_names:
        st.warning(
            f"⚠️ **Yahoo is serving outdated prices for {len(stale_names)} stock"
            f"{'s' if len(stale_names) > 1 else ''}:** {', '.join(stale_names)}. "
            f"Portfolio totals will differ from INDmoney by these stocks' last-day moves. "
            f"Usually self-corrects within a few hours; try 🔄 Refresh prices later."
        )

    # Per-stock price provenance — hidden by default; enable from the sidebar
    # when something looks off (the ⚠️ banner above fires automatically anyway).
    if (st.session_state.get("show_price_diag")
            and not enriched.empty and "Price Source" in enriched.columns):
        with st.expander("🔍 Price data diagnostics (per stock)", expanded=True):
            diag = enriched[["Short Name", "CMP", "Price Source", "Price Date"]].copy()
            diag = diag.rename(columns={"Price Date": "Daily-bar date"})
            st.caption(f"Expected latest close date: **{last_expected_close_date()}** · "
                        "'quote'/'NSE twin'/'BSE direct' = fetched live, bar date not applicable")
            st.table(diag.reset_index(drop=True))

    # KPI cards
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Invested", fmt_inr_compact(k["invested"]), help=fmt_inr(k["invested"]))
    c2.metric("Current Value", fmt_inr_compact(k["current"]), fmt_pct(k["unrealised_pct"]),
               help=fmt_inr(k["current"]))
    c3.metric("Unrealised P&L", fmt_inr_compact(k["unrealised"]), fmt_pct(k["unrealised_pct"]),
               help=fmt_inr(k["unrealised"]))
    day_pct = (k["day_pnl"] / k["current"] * 100) if k["current"] else 0
    c4.metric("Today's P&L", fmt_inr_compact(k["day_pnl"]), fmt_pct(day_pct),
               help=fmt_inr(k["day_pnl"]))
    c5.metric("Realised P&L", fmt_inr_compact(k["realised"]), help=fmt_inr(k["realised"]))

    # Live XIRR from transactions log
    try:
        tx = db.get_transactions()
        xr = xirr.compute_xirr(tx, k["current"])
        if xr.get("xirr") is not None:
            c6.metric("XIRR (annualised)", f"{xr['xirr']*100:.2f}%",
                       help=f"Computed from {xr['n_flows']} cash flows since {xr['first_date']}")
        else:
            c6.metric("XIRR", "—", help=xr.get("reason", ""))
    except Exception:
        c6.metric("XIRR", "—", help="Transactions table unavailable")

    st.divider()

    tab_names = [
        "📊 Holdings", "🥧 Allocation", "👀 Watchlist",
        "💰 Realised P&L", "📈 History", "📜 Transactions", "📝 Notes",
    ]
    tabs = st.tabs(tab_names)
    with tabs[0]: tab_holdings(enriched)
    with tabs[1]: tab_allocation(enriched, k)
    with tabs[2]: tab_watchlist()
    with tabs[3]: tab_realised(realised)
    with tabs[4]: tab_history(k)
    with tabs[5]: tab_transactions()
    with tabs[6]: tab_notes()


if __name__ == "__main__":
    main()
