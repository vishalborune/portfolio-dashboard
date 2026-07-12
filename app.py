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

st.set_page_config(
    page_title="Vishal's Portfolio",
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
    """
    tenant = _get_secret("APP_TENANT", "vishal").strip().lower()

    if st.session_state.get("role"):
        return True

    st.title("📈 Portfolio Dashboard")
    st.caption("Enter your password to continue")
    pw = st.text_input("Password", type="password", key="pw_input")

    if tenant == "lakshmi":
        lakshmi_pw = _get_secret("LAKSHMI_PASSWORD")
        if not lakshmi_pw:
            st.error("⚠️ App not configured. Set LAKSHMI_PASSWORD in Streamlit secrets.")
            st.stop()
        if st.button("Enter", type="primary"):
            if pw == lakshmi_pw:
                st.session_state.role = "lakshmi"
                st.session_state.user = "Lakshmi"
                st.session_state.portfolios = {2: "Lakshmi", 3: "Abinaya"}
                st.session_state.portfolio_id = 2
                # No explicit st.rerun() here: the button click already
                # triggers Streamlit's own automatic rerun. Calling rerun()
                # AGAIN from inside that in-flight rerun caused an
                # intermittent native crash. Returning True lets THIS same
                # execution continue straight into the dashboard instead.
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
    if st.button("Enter", type="primary"):
        if pw == owner_pw:
            st.session_state.role = "owner"
            st.session_state.user = "Vishal"
            st.session_state.portfolios = {1: "Vishal"}
            st.session_state.portfolio_id = 1
            return True
        elif friend_pw and pw == friend_pw:
            st.session_state.role = "friend"
            st.session_state.user = _get_secret("FRIEND_NAME", "Friend")
            st.session_state.portfolios = {1: "Vishal"}
            st.session_state.portfolio_id = 1
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

    # 1) Daily bars: prev-close reference + fallback + bar-date for staleness
    bar_close, bar_prev, bar_date = {}, {}, {}
    try:
        _end = now_ist().date() + timedelta(days=1)
        _start = _end - timedelta(days=14)
        data = yf.download(
            list(tickers), start=str(_start), end=str(_end), interval="1d",
            progress=False, auto_adjust=False, group_by="ticker", threads=True,
        )
        for t in tickers:
            try:
                close = data["Close"].dropna() if len(tickers) == 1 else data[t]["Close"].dropna()
                if len(close):
                    bar_close[t] = float(close.iloc[-1])
                    bar_prev[t] = float(close.iloc[-2]) if len(close) >= 2 else float(close.iloc[-1])
                    bar_date[t] = pd.Timestamp(close.index[-1]).date()
            except Exception:
                pass
    except Exception:
        pass

    # 2) Quote endpoint in parallel — the fresher source
    quotes = {}
    try:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for t, lp, pc in ex.map(_fetch_quote, tickers):
                quotes[t] = (lp, pc)
    except Exception:
        pass

    expected = last_expected_close_date()
    MAX_PLAUSIBLE_MOVE = 0.25   # smallcap daily circuit ~20%; beyond this per
                                 # missing day, the quote is garbage, not a move
    rows = []
    for t in tickers:
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
    rows = []
    for t in tickers:
        try:
            info = yf.Ticker(t).info or {}
            rows.append({
                "Ticker": t,
                "Sector": info.get("sector") or "Unknown",
                "Industry": info.get("industry") or "Unknown",
                "Market Cap (Cr)": (info.get("marketCap") or 0) / 1e7,
                "PE (live)": info.get("trailingPE"),
                "P/B": info.get("priceToBook"),
                "EV/EBITDA": info.get("enterpriseToEbitda"),
                "PEG": info.get("trailingPegRatio") or info.get("pegRatio"),
            })
        except Exception:
            rows.append({"Ticker": t, "Sector": "Unknown", "Industry": "Unknown",
                         "Market Cap (Cr)": np.nan, "PE (live)": np.nan,
                         "P/B": np.nan, "EV/EBITDA": np.nan, "PEG": np.nan})
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

def enrich_holdings(holdings_df: pd.DataFrame) -> pd.DataFrame:
    """Add Ticker, Short Name, live price columns + computed P&L."""
    if holdings_df is None or holdings_df.empty:
        # Brand-new portfolio: return an empty frame with the columns the UI reads
        return pd.DataFrame(columns=[
            "id", "stock_name", "Short Name", "Ticker", "quantity", "purchase_cost",
            "CMP", "Prev Close", "Day Change %", "Invested", "Current Value",
            "P&L", "P&L %", "Allocation %", "State", "State Display", "State Reason",
            "State Priority", "% from 10wEMA", "Vol vs 10wk",
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

    # Flowchart states (Lakshmi's TheWrap TA rules) — one per ticker
    with st.spinner("Computing flowchart states (weekly TA)..."):
        states = signals.states_for_holdings(tickers)
    df = df.merge(states, on="Ticker", how="left")

    # % distance of LIVE price from the 10-week EMA (add-timing helper):
    # +20% = price 20% above the 10wEMA (extended); -10% = 10% below it.
    if "EMA10" in df.columns:
        df["% from 10wEMA"] = (df["CMP"] - df["EMA10"]) / df["EMA10"] * 100

    df["quantity"] = df["quantity"].astype(float)
    df["purchase_cost"] = df["purchase_cost"].astype(float)
    df["Current Value"] = df["CMP"] * df["quantity"]
    df["Invested"] = df["purchase_cost"] * df["quantity"]
    df["P&L"] = df["Current Value"] - df["Invested"]
    df["P&L %"] = (df["P&L"] / df["Invested"]) * 100
    df["Day P&L"] = (df["CMP"] - df["Prev Close"]) * df["quantity"]
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
                "Short Name", "State Display", "% from 10wEMA", "Vol vs 10wk",
                "CMP", "purchase_cost", "quantity",
            ],
            "💰 P&L view": [
                "Short Name", "State Display", "quantity", "purchase_cost", "Invested",
                "CMP", "Day Change %", "Current Value", "P&L", "P&L %", "Allocation %",
            ],
            "🔬 Fundamentals view": [
                "Short Name", "Ticker", "CMP", "Sector", "Market Cap (Cr)",
                "PE (live)", "P/B", "EV/EBITDA", "PEG", "Allocation %",
            ],
            "🗂 Everything": [
                "Short Name", "Ticker", "State Display", "% from 10wEMA", "Vol vs 10wk",
                "quantity", "purchase_cost", "Invested", "CMP",
                "Day Change %", "Current Value", "P&L", "P&L %", "Allocation %",
                "Sector", "Market Cap (Cr)", "PE (live)", "P/B", "EV/EBITDA", "PEG",
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
                ["State (urgent first)", "% from 10wEMA", "Vol vs 10wk", "Invested",
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

        styled = view.style.format({
            "Qty": "{:,.0f}", "Avg Cost": "₹{:,.2f}", "Invested": "₹{:,.0f}",
            "CMP": "₹{:,.2f}",
            "% from 10wEMA": "{:+.1f}%", "Vol vs 10wk": "{:.1f}x",
            "Day Change %": "{:+.2f}%", "Current Value": "₹{:,.0f}",
            "P&L": "₹{:,.0f}", "P&L %": "{:+.2f}%",
            "Allocation %": "{:.1f}%", "Market Cap (Cr)": "{:,.0f}",
            "PE (live)": "{:.2f}", "P/B": "{:.2f}", "EV/EBITDA": "{:.2f}", "PEG": "{:.2f}",
        }, na_rep="—")
        if pnl_cols:
            styled = styled.map(color_pnl, subset=pnl_cols)
        if ema_cols:
            styled = styled.map(color_ema_distance, subset=ema_cols)
        st.dataframe(styled, use_container_width=True, height=520, hide_index=True)

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
        c3, c4 = st.columns(2)
        with c3:
            partial = st.checkbox("Partial sell?")
        with c4:
            partial_qty = st.number_input(
                "Quantity sold", min_value=0.0,
                max_value=float(row["quantity"]), value=float(row["quantity"]),
                step=1.0, disabled=not partial,
            )
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
                partial_quantity=partial_qty if partial else None,
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
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("By Sector")
        sec = enriched.groupby("Sector", dropna=False)["Current Value"].sum().reset_index()
        sec = sec.sort_values("Current Value", ascending=True)
        fig2 = px.bar(sec, x="Current Value", y="Sector", orientation="h",
                      text=sec["Current Value"].apply(lambda x: f"₹{x/1000:,.0f}k"))
        fig2.update_layout(height=480, margin=dict(t=10, b=10), xaxis_title="", yaxis_title="")
        st.plotly_chart(fig2, use_container_width=True)

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
            # On-demand (button) rather than auto-run: keeps the tab light and
            # avoids heavy daily-bar fetches on every app load.
            if st.button("🎯 Check entry zones (10/21 DMA)", key="check_entry_zones"):
                try:
                    with st.spinner("Checking entry zones…"):
                        st.session_state["entry_zone_cache"] = (
                            signals.entry_states_for_watchlist(tickers))
                except Exception as e:
                    st.error(f"Entry-zone check failed: {e}")
            entries = st.session_state.get("entry_zone_cache")
            if entries is not None and not entries.empty:
                wl_view = wl_view.merge(
                    entries.drop(columns=["CMP (d)"], errors="ignore"),
                    on="Ticker", how="left")
            if "target_buy_price" in wl_view.columns:
                wl_view["Distance to Target %"] = (
                    (wl_view["CMP"] - wl_view["target_buy_price"]) / wl_view["target_buy_price"] * 100
                )
        cols = ["Short Name", "Ticker", "CMP", "Entry Advice", "10DMA", "21DMA",
                "% vs 10DMA", "Day Change %",
                "target_buy_price", "Distance to Target %", "notes", "added_by"]
        cols = [c for c in cols if c in wl_view.columns]
        styled = (
            wl_view[cols].rename(columns={
                "target_buy_price": "Target Buy", "notes": "Notes", "added_by": "Added By",
            }).style.format({
                "CMP": "₹{:,.2f}", "Day Change %": "{:+.2f}%",
                "Target Buy": "₹{:,.2f}", "Distance to Target %": "{:+.2f}%",
            }, na_rep="—").map(color_pnl, subset=["Day Change %"])
        )
        st.dataframe(styled, use_container_width=True, hide_index=True, height=420)

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
    st.dataframe(styled, use_container_width=True, hide_index=True, height=460)

    cb, cw = st.columns(2)
    with cb:
        st.markdown("**🏆 Top 5 winners**")
        st.dataframe(realised.nlargest(5, "gain_loss")[["stock_name", "gain_loss", "pct_gain_loss"]],
                     use_container_width=True, hide_index=True)
    with cw:
        st.markdown("**💀 Top 5 losers**")
        st.dataframe(realised.nsmallest(5, "gain_loss")[["stock_name", "gain_loss", "pct_gain_loss"]],
                     use_container_width=True, hide_index=True)


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
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Snapshot history")
    show = snaps.iloc[::-1].copy()
    show["snapshot_date"] = show["snapshot_date"].dt.strftime("%Y-%m-%d")
    st.dataframe(show, use_container_width=True, hide_index=True)


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
    st.dataframe(styled, use_container_width=True, hide_index=True, height=500)

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
            use_container_width=True,
        )
    with c1:
        st.caption(
            "💡 Tip: Use the filters above to narrow down what gets exported. "
            "Useful for monthly reconciliation with INDmoney, tax filing, or sharing with your CA."
        )


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

REQUIRED_IMPORT_COLS = {"stock name", "exchange", "symbol", "quantity", "avg buy price"}


def _norm_header(c) -> str:
    # Kill non-breaking spaces / stray whitespace / case differences
    return " ".join(str(c).replace("\xa0", " ").strip().lower().split())


def _load_import_template(file):
    """Find the header row anywhere in any sheet (row 1-12), any sheet name.
    Returns (DataFrame, None) on success or (None, diagnostic_message)."""
    try:
        xls = pd.ExcelFile(file)
    except Exception as e:
        return None, f"Couldn't read the file: {e}"
    seen = []
    for sheet in xls.sheet_names:
        try:
            probe = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=12)
        except Exception:
            continue
        for i in range(len(probe)):
            row_vals = {_norm_header(v) for v in probe.iloc[i].tolist()}
            hits = REQUIRED_IMPORT_COLS & row_vals
            if len(hits) >= 4:      # found the header row
                df = pd.read_excel(xls, sheet_name=sheet, header=i)
                df.columns = [_norm_header(c) for c in df.columns]
                missing = REQUIRED_IMPORT_COLS - set(df.columns)
                if missing:
                    return None, (f"Found the header row in sheet '{sheet}' but "
                                   f"column(s) still missing: {', '.join(sorted(missing))}")
                return df, None
            seen.extend(v for v in row_vals if v and v != "nan")
    return None, ("Couldn't find the header row in any sheet. The file must contain "
                   "a row with: Stock Name · Exchange · Symbol · Quantity · Avg Buy Price. "
                   f"(Headers seen in the file: {', '.join(sorted(set(seen))[:12]) or 'none'})")


def _normalize_import_row(name, exch, sym):
    """Map broker-style exchange/symbol values to our canonical ticker format.
    Returns (canonical_stock_name, warning_or_None)."""
    exch = exch.upper().replace(" ", "")
    sym = sym.upper().strip()
    warn = None

    # Known fixes: broker symbols that differ from Yahoo/our pipeline
    SPECIAL = {
        ("BSE", "KPL"): ("XBOM", "539997"),          # Kwality Pharmaceuticals
        ("BSE", "BMW-X"): ("XBOM", "542669"),        # BMW Industries
        ("BSE", "BMW"): ("XBOM", "542669"),
        ("BSE", "BANSWRAS"): ("XNSE", "BANSWRAS"),   # dual-listed: NSE feed is better
        ("BSE", "FAZE3Q"): ("XNSE", "FAZE3Q"),       # dual-listed: NSE symbol
    }
    base_exch = "BSE" if exch.startswith("BSE") else "NSE"
    if (base_exch, sym) in SPECIAL:
        prefix, sym = SPECIAL[(base_exch, sym)]
    elif base_exch == "NSE":
        prefix = "XNSE"
        # strip series suffixes brokers append (-T T2T, -SM/-ST SME, -BE etc.)
        stripped = re.sub(r"-(SM|ST|T|BE|BZ|MT|MS)$", "", sym)
        if stripped != sym:
            sym = stripped
        if exch == "NSE-SME":
            warn = "SME stock — price data may be unavailable on Yahoo"
    else:  # BSE / BSE-SME
        prefix = "XBOM"
        if not sym.isdigit():
            # Alphabetic BSE symbol without a known numeric code: keep as-is,
            # price fetch will flag it rather than risk fetching the wrong company
            warn = "BSE symbol is not a numeric scrip code — price may not resolve"
    canonical = f"{name.upper()} ({prefix}:{sym})"
    return canonical, warn


def tab_import_holdings():
    """Setup-phase bulk import from the standard Excel template.
    Handles transaction-level files: multiple buy lots per stock are grouped,
    the first lot creates the holding and later lots go through buy-more so
    every purchase date lands in the transactions table (accurate XIRR).
    Imports into the CURRENTLY SELECTED portfolio. Removable after onboarding."""
    st.subheader("📥 Import Holdings (one-time setup)")
    pf_name = st.session_state.get("portfolios", {}).get(
        st.session_state.get("portfolio_id"), "?")
    st.info(f"Rows will be imported into: **{pf_name}** "
            f"(switch portfolio in the sidebar to import the other one).")
    st.markdown(
        "Columns needed: **Stock Name · Exchange · Symbol · Quantity · Avg Buy Price** "
        "· optional **Buy Date**. Multiple rows per stock (one per purchase) are fine — "
        "they'll be combined automatically with the full buy history preserved."
    )

    up = st.file_uploader("Upload the filled template (.xlsx)", type=["xlsx"],
                           key="import_upload")
    if not up:
        return
    raw, err = _load_import_template(up)
    if err:
        st.error(err)
        return

    # Parse rows -> lots
    lots, problems = [], []
    for i, r in raw.iterrows():
        try:
            name = str(r["stock name"]).strip()
            exch = str(r["exchange"]).strip().upper()
            sym = str(r["symbol"]).strip().upper()
            qty = float(r["quantity"])
            cost = float(r["avg buy price"])
            if (not name or name.lower() == "nan" or not sym or sym == "NAN"
                    or exch.replace("-", "").replace(" ", "") not in
                    ("NSE", "BSE", "NSESME", "BSESME") or qty <= 0 or cost < 0):
                raise ValueError
            bdate = pd.to_datetime(r.get("buy date"), errors="coerce")
            bdate = bdate.date() if pd.notna(bdate) else date.today()
            canonical, warn = _normalize_import_row(name, exch, sym)
            lots.append({"stock_name": canonical, "qty": qty, "price": cost,
                          "date": bdate, "warn": warn})
        except Exception:
            # skip blank/total/invalid rows silently unless partially filled
            vals = [r.get(c) for c in ("stock name", "symbol", "quantity")]
            if any(pd.notna(v) for v in vals):
                problems.append(i + 2)

    if problems:
        st.warning(f"Skipping {len(problems)} row(s) with invalid data "
                    f"(rows: {problems[:10]}{'…' if len(problems) > 10 else ''})")
    if not lots:
        st.error("No valid rows found.")
        return

    # Group lots per stock. Zero-price lots = bonus shares; a paid lot must
    # open the position, so sort by (date, price==0 last within same date).
    stocks = {}
    for l in sorted(lots, key=lambda x: (x["date"], x["price"] == 0)):
        s = stocks.setdefault(l["stock_name"], {"lots": [], "warn": l["warn"]})
        s["lots"].append(l)
    for cname in list(stocks):
        ls = stocks[cname]["lots"]
        if all(l["price"] == 0 for l in ls):
            st.warning(f"{cname}: only zero-price (bonus) lots found — skipping, "
                        "needs at least one paid lot.")
            del stocks[cname]
            continue
        if ls[0]["price"] == 0:   # bonus dated before first paid buy: reorder
            paid = next(i for i, l in enumerate(ls) if l["price"] > 0)
            ls.insert(0, ls.pop(paid))

    prev_rows = []
    for cname, s in stocks.items():
        tq = sum(l["qty"] for l in s["lots"])
        inv = sum(l["qty"] * l["price"] for l in s["lots"])
        prev_rows.append({"Stock": cname, "Lots": len(s["lots"]), "Total Qty": tq,
                           "Weighted Avg": round(inv / tq, 2), "Invested": round(inv, 2),
                           "⚠️": s["warn"] or ""})
    prev = pd.DataFrame(prev_rows)
    st.dataframe(prev, use_container_width=True, hide_index=True)
    st.markdown(f"**{len(stocks)} stocks · {len(lots)} buy lots · "
                f"₹{prev['Invested'].sum():,.0f} invested** → portfolio **{pf_name}**")
    warns = prev[prev["⚠️"] != ""]
    if not warns.empty:
        st.warning(f"{len(warns)} stock(s) flagged — they'll import fine, but live "
                    f"prices/signals may not resolve for them (SME / non-standard symbols).")

    existing = db.get_holdings()
    existing_names = set(existing["stock_name"]) if not existing.empty else set()
    dupes = [c for c in stocks if c in existing_names]
    if dupes:
        st.warning(f"⚠️ Already in this portfolio (will be skipped): "
                    f"{', '.join(d.split(' (')[0] for d in dupes[:8])}")

    n_new = len(stocks) - len(dupes)
    if st.button(f"✅ Import {n_new} stocks ({len(lots)} buy lots) into {pf_name}",
                  type="primary", key="do_import"):
        done, prog = 0, st.progress(0.0, text="Importing…")
        todo = [c for c in stocks if c not in existing_names]
        for k, cname in enumerate(todo):
            ls = stocks[cname]["lots"]
            try:
                first = ls[0]
                hid = db.add_holding(cname, first["qty"], first["price"],
                                      buy_date=first["date"])
                for extra in ls[1:]:
                    db.buy_more(hid, extra["qty"], extra["price"],
                                 transaction_date=extra["date"])
                db.graduate_from_watchlist(cname)
                done += 1
            except Exception as e:
                st.error(f"{cname}: {e}")
            prog.progress((k + 1) / max(len(todo), 1),
                           text=f"Importing… {k + 1}/{len(todo)}")
        prog.empty()
        st.success(f"Imported {done} stocks with full buy history into {pf_name}. "
                    "Switch portfolio in the sidebar to import the next file.")
        st.balloons()


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
    c1.metric("Invested", fmt_inr(k["invested"]))
    c2.metric("Current Value", fmt_inr(k["current"]), fmt_pct(k["unrealised_pct"]))
    c3.metric("Unrealised P&L", fmt_inr(k["unrealised"]), fmt_pct(k["unrealised_pct"]))
    day_pct = (k["day_pnl"] / k["current"] * 100) if k["current"] else 0
    c4.metric("Today's P&L", fmt_inr(k["day_pnl"]), fmt_pct(day_pct))
    c5.metric("Realised P&L", fmt_inr(k["realised"]))

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
    # Setup-phase importer for Lakshmi's household — removable after onboarding
    show_import = st.session_state.get("role") == "lakshmi"
    if show_import:
        tab_names.append("📥 Import Holdings")
    tabs = st.tabs(tab_names)
    with tabs[0]: tab_holdings(enriched)
    with tabs[1]: tab_allocation(enriched, k)
    with tabs[2]: tab_watchlist()
    with tabs[3]: tab_realised(realised)
    with tabs[4]: tab_history(k)
    with tabs[5]: tab_transactions()
    with tabs[6]: tab_notes()
    if show_import:
        with tabs[7]: tab_import_holdings()


if __name__ == "__main__":
    main()
