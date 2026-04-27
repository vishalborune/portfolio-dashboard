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
import streamlit as st
import yfinance as yf

import db

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
    """Block app until user enters a valid password. Sets st.session_state.role."""
    owner_pw = _get_secret("OWNER_PASSWORD")
    friend_pw = _get_secret("FRIEND_PASSWORD")

    if not owner_pw:
        st.error("⚠️ App not configured. Set OWNER_PASSWORD in Streamlit secrets.")
        st.stop()

    if st.session_state.get("role"):
        return True

    st.title("📈 Portfolio Dashboard")
    st.caption("Enter your password to continue")
    pw = st.text_input("Password", type="password", key="pw_input")
    if st.button("Enter", type="primary"):
        if pw == owner_pw:
            st.session_state.role = "owner"
            st.session_state.user = "Vishal"
            st.rerun()
        elif friend_pw and pw == friend_pw:
            st.session_state.role = "friend"
            st.session_state.user = _get_secret("FRIEND_NAME", "Friend")
            st.rerun()
        else:
            st.error("Wrong password.")
    return False


def is_owner() -> bool:
    return st.session_state.get("role") == "owner"


def can_edit_holdings() -> bool:
    return is_owner()


def can_edit_watchlist() -> bool:
    return st.session_state.get("role") in ("owner", "friend")


def can_edit_notes() -> bool:
    return st.session_state.get("role") in ("owner", "friend")


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

@st.cache_data(ttl=PRICE_CACHE_TTL)
def fetch_live_prices(tickers: tuple) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "CMP", "Prev Close", "Day Change %"])
    data = yf.download(
        list(tickers), period="5d", interval="1d",
        progress=False, auto_adjust=False, group_by="ticker", threads=True,
    )
    rows = []
    for t in tickers:
        try:
            close = data["Close"].dropna() if len(tickers) == 1 else data[t]["Close"].dropna()
            if len(close) == 0:
                rows.append({"Ticker": t, "CMP": np.nan, "Prev Close": np.nan})
                continue
            cmp_ = float(close.iloc[-1])
            prev = float(close.iloc[-2]) if len(close) >= 2 else cmp_
            rows.append({"Ticker": t, "CMP": cmp_, "Prev Close": prev})
        except Exception:
            rows.append({"Ticker": t, "CMP": np.nan, "Prev Close": np.nan})
    out = pd.DataFrame(rows)
    out["Day Change %"] = ((out["CMP"] - out["Prev Close"]) / out["Prev Close"]) * 100
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
            })
        except Exception:
            rows.append({"Ticker": t, "Sector": "Unknown", "Industry": "Unknown",
                         "Market Cap (Cr)": np.nan, "PE (live)": np.nan})
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
        sort_by = st.selectbox(
            "Sort by",
            ["Day Change %", "P&L %", "P&L", "Allocation %", "Current Value", "Short Name"],
            index=0, key="holdings_sort",
        )
        ascending = sort_by == "Short Name"
        view = enriched[[
            "Short Name", "Ticker", "quantity", "purchase_cost", "CMP",
            "Day Change %", "Current Value", "P&L", "P&L %", "Allocation %",
            "Sector", "Market Cap (Cr)", "PE (live)",
        ]].rename(columns={"quantity": "Qty", "purchase_cost": "Avg Cost"})
        view = view.sort_values(sort_by, ascending=ascending, na_position="last")
        styled = (
            view.style.format({
                "Qty": "{:,.0f}", "Avg Cost": "₹{:,.2f}", "CMP": "₹{:,.2f}",
                "Day Change %": "{:+.2f}%", "Current Value": "₹{:,.0f}",
                "P&L": "₹{:,.0f}", "P&L %": "{:+.2f}%",
                "Allocation %": "{:.1f}%", "Market Cap (Cr)": "{:,.0f}",
                "PE (live)": "{:.2f}",
            }, na_rep="—").map(color_pnl, subset=["Day Change %", "P&L", "P&L %"])
        )
        st.dataframe(styled, use_container_width=True, height=520, hide_index=True)

    if not can_edit_holdings():
        st.caption("🔒 Read-only view (logged in as friend)")
        return

    st.divider()
    st.subheader("Manage holdings")

    cc1, cc2, cc3 = st.columns(3)
    with cc1:
        with st.expander("➕ Add holding", expanded=False):
            _form_add_holding()
    with cc2:
        with st.expander("💰 Mark as sold", expanded=False):
            _form_mark_as_sold(enriched)
    with cc3:
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
            st.success(f"✅ Added {qty:g} × {short_name(stock_name)} @ ₹{cost:,.2f}")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to add: {e}")


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
            if "target_buy_price" in wl_view.columns:
                wl_view["Distance to Target %"] = (
                    (wl_view["CMP"] - wl_view["target_buy_price"]) / wl_view["target_buy_price"] * 100
                )
        cols = ["Short Name", "Ticker", "CMP", "Day Change %",
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

def main():
    if not login_gate():
        return

    # Sidebar
    st.sidebar.title("⚙️ Settings")
    st.sidebar.caption(f"👤 Logged in as: **{st.session_state.get('user','?')}**  "
                        f"({st.session_state.get('role','?')})")

    if st.sidebar.button("🔄 Refresh prices"):
        st.cache_data.clear()
        st.rerun()

    if st.sidebar.button("Logout"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

    st.sidebar.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")
    st.sidebar.caption(f"Prices cached: {PRICE_CACHE_TTL // 60} min")

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
    st.caption(f"Tracking {k['n_holdings']} holdings · "
                f"Updated {datetime.now().strftime('%I:%M %p, %d %b %Y')}")

    # KPI cards
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Invested", fmt_inr(k["invested"]))
    c2.metric("Current Value", fmt_inr(k["current"]), fmt_pct(k["unrealised_pct"]))
    c3.metric("Unrealised P&L", fmt_inr(k["unrealised"]), fmt_pct(k["unrealised_pct"]))
    day_pct = (k["day_pnl"] / k["current"] * 100) if k["current"] else 0
    c4.metric("Today's P&L", fmt_inr(k["day_pnl"]), fmt_pct(day_pct))
    c5.metric("Realised P&L", fmt_inr(k["realised"]))

    st.divider()

    tabs = st.tabs([
        "📊 Holdings", "🥧 Allocation", "👀 Watchlist",
        "💰 Realised P&L", "📈 History", "📝 Notes",
    ])
    with tabs[0]: tab_holdings(enriched)
    with tabs[1]: tab_allocation(enriched, k)
    with tabs[2]: tab_watchlist()
    with tabs[3]: tab_realised(realised)
    with tabs[4]: tab_history(k)
    with tabs[5]: tab_notes()


if __name__ == "__main__":
    main()
