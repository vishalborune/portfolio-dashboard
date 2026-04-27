"""
Vishal's Portfolio Dashboard
----------------------------
Live NSE/BSE portfolio tracker reading from Google Sheets, with collaborative
watchlist + notes for buy/sell recommendations.

Run locally:    streamlit run app.py
Deploy:         Push to GitHub -> connect on share.streamlit.io
"""

import io
import os
import re
import json
import time
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import streamlit as st
import yfinance as yf
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Vishal's Portfolio",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Optional password gate. Set PORTFOLIO_PASSWORD in Streamlit secrets or env.
APP_PASSWORD = st.secrets.get("PORTFOLIO_PASSWORD") if hasattr(st, "secrets") else None
if APP_PASSWORD is None:
    APP_PASSWORD = os.environ.get("PORTFOLIO_PASSWORD")

SNAPSHOT_FILE = Path("snapshots.csv")
NOTES_FILE = Path("notes_local.json")

PRICE_CACHE_TTL = 300        # 5 minutes between live-price refreshes
INFO_CACHE_TTL = 60 * 60 * 6 # 6 hours for fundamentals (sector, market cap)


# ---------------------------------------------------------------------------
# AUTH (optional)
# ---------------------------------------------------------------------------

def password_gate():
    if not APP_PASSWORD:
        return True
    if st.session_state.get("authed"):
        return True
    st.title("📈 Portfolio Dashboard")
    pw = st.text_input("Password", type="password")
    if st.button("Enter") and pw == APP_PASSWORD:
        st.session_state.authed = True
        st.rerun()
    if pw and pw != APP_PASSWORD:
        st.error("Wrong password.")
    return False


# ---------------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------------

def gsheet_csv_url(sheet_url: str, gid: str = "0") -> str:
    """Convert a Google Sheets share URL into a CSV export URL."""
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", sheet_url)
    if not m:
        return sheet_url
    sheet_id = m.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


@st.cache_data(ttl=120)
def load_csv(url: str) -> pd.DataFrame:
    return pd.read_csv(url)


def _is_sheet_url(source) -> bool:
    return isinstance(source, str) and source.startswith("http")


def _read_excel_tab(source, idx: int) -> pd.DataFrame:
    """Read a sheet by index from either a file path string or uploaded file."""
    return pd.read_excel(source, sheet_name=idx)


def load_holdings(source) -> pd.DataFrame:
    """Load holdings from Google Sheet URL, file path, or uploaded Excel."""
    if _is_sheet_url(source):
        df = load_csv(gsheet_csv_url(source, gid="0"))
    else:
        df = _read_excel_tab(source, 0)
    return clean_holdings(df)


def load_realised(source) -> pd.DataFrame:
    try:
        if _is_sheet_url(source):
            df = load_csv(gsheet_csv_url(source, gid="1"))
        else:
            df = _read_excel_tab(source, 1)
    except Exception:
        return pd.DataFrame()
    return clean_realised(df)


def load_watchlist(source) -> pd.DataFrame:
    """Load watchlist (gid=2 in Google Sheet, 3rd tab in Excel)."""
    empty = pd.DataFrame(columns=["Stock Name", "Target Buy Price", "Notes", "Added By"])
    try:
        if _is_sheet_url(source):
            df = load_csv(gsheet_csv_url(source, gid="2"))
        else:
            df = _read_excel_tab(source, 2)
        if "Stock Name" in df.columns:
            df = df.dropna(subset=["Stock Name"])
        return df
    except Exception:
        return empty


def load_notes(source) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["Date", "Author", "Note"])
    try:
        if _is_sheet_url(source):
            df = load_csv(gsheet_csv_url(source, gid="3"))
        else:
            df = _read_excel_tab(source, 3)
        if "Note" in df.columns:
            df = df.dropna(subset=["Note"])
        return df
    except Exception:
        return empty


# ---------------------------------------------------------------------------
# CLEANING
# ---------------------------------------------------------------------------

def _to_number(x):
    """Strip rupee symbols, commas, spaces -> float. Returns NaN on failure."""
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = re.sub(r"[₹$,?\s]", "", s)
    if s in ("", "-", "—"):
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def extract_yf_ticker(name: str):
    """'COMPANY (XNSE:SYMBOL)' -> 'SYMBOL.NS'. 'XBOM:123456' -> '123456.BO'."""
    if not isinstance(name, str):
        return None
    m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", name)
    if not m:
        return None
    exch, sym = m.group(1), m.group(2).strip()
    return f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"


def short_name(name: str) -> str:
    """Strip the ticker suffix and 'LIMITED' for cleaner display."""
    if not isinstance(name, str):
        return ""
    name = re.sub(r"\s*\([^)]*\)\s*$", "", name)
    name = re.sub(r"\s+(LIMITED|LTD\.?|LTD)\s*$", "", name, flags=re.IGNORECASE)
    return name.strip().title()


def clean_holdings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(subset=["Stock Name"]) if "Stock Name" in df.columns else df

    needed = ["Stock Name", "Quantity", "Purchase Cost", "Amount Invested"]
    for col in needed:
        if col not in df.columns:
            raise ValueError(f"Holdings sheet missing required column: '{col}'")

    df["Ticker"] = df["Stock Name"].apply(extract_yf_ticker)
    df["Short Name"] = df["Stock Name"].apply(short_name)

    for col in ["Quantity", "Purchase Cost", "Amount Invested"]:
        df[col] = df[col].apply(_to_number)

    # Drop computed columns from source — we recompute these from live prices.
    drop_cols = ["CMP", "Current Value", "Gain/Loss", "%Gain/Loss",
                 "Allocation %", "PE Ratio", "Market cap (In Cr)", "S.No"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    df = df.dropna(subset=["Ticker", "Quantity", "Purchase Cost"])
    df = df[df["Quantity"] > 0].reset_index(drop=True)
    return df


def clean_realised(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if "Stock Name" not in df.columns:
        # Header is on row 2 in original sheet; detect and skip the title row
        df.columns = df.iloc[0].astype(str).str.strip()
        df = df.iloc[1:].reset_index(drop=True)
    df = df.dropna(subset=["Stock Name"]) if "Stock Name" in df.columns else df
    for col in ["Quantity", "Purchase Cost", "Amount Invested",
                "Selling Price", "Sale consideration", "Gain/Loss"]:
        if col in df.columns:
            df[col] = df[col].apply(_to_number)
    if "Stock Name" in df.columns:
        df["Short Name"] = df["Stock Name"].apply(short_name)
    return df


# ---------------------------------------------------------------------------
# LIVE PRICES
# ---------------------------------------------------------------------------

@st.cache_data(ttl=PRICE_CACHE_TTL)
def fetch_live_prices(tickers: tuple) -> pd.DataFrame:
    """Batch-fetch last close + previous close for all tickers in one call."""
    if not tickers:
        return pd.DataFrame(columns=["CMP", "Prev Close", "Day Change %"])

    data = yf.download(
        list(tickers),
        period="5d",
        interval="1d",
        progress=False,
        auto_adjust=False,
        group_by="ticker",
        threads=True,
    )

    rows = []
    for t in tickers:
        try:
            if len(tickers) == 1:
                close = data["Close"].dropna()
            else:
                close = data[t]["Close"].dropna()
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
    """Sector + market cap + PE. Slower, so cached for 6 hours."""
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


# ---------------------------------------------------------------------------
# COMPUTATION
# ---------------------------------------------------------------------------

def enrich_holdings(holdings: pd.DataFrame, prices: pd.DataFrame,
                    fundamentals: pd.DataFrame) -> pd.DataFrame:
    df = holdings.merge(prices, on="Ticker", how="left")
    df = df.merge(fundamentals, on="Ticker", how="left")
    df["Current Value"] = df["CMP"] * df["Quantity"]
    df["Invested"] = df["Purchase Cost"] * df["Quantity"]
    df["P&L"] = df["Current Value"] - df["Invested"]
    df["P&L %"] = (df["P&L"] / df["Invested"]) * 100
    df["Day P&L"] = (df["CMP"] - df["Prev Close"]) * df["Quantity"]
    total_value = df["Current Value"].sum()
    df["Allocation %"] = (df["Current Value"] / total_value) * 100 if total_value else 0
    return df


def kpis(enriched: pd.DataFrame, realised: pd.DataFrame) -> dict:
    invested = enriched["Invested"].sum()
    current = enriched["Current Value"].sum()
    unrealised = current - invested
    unrealised_pct = (unrealised / invested * 100) if invested else 0
    day_pnl = enriched["Day P&L"].sum()
    realised_pnl = realised["Gain/Loss"].sum() if "Gain/Loss" in realised.columns else 0
    return {
        "invested": invested,
        "current": current,
        "unrealised": unrealised,
        "unrealised_pct": unrealised_pct,
        "day_pnl": day_pnl,
        "realised": realised_pnl,
        "total_pnl": unrealised + realised_pnl,
        "n_holdings": len(enriched),
    }


# ---------------------------------------------------------------------------
# SNAPSHOTS (long-run P&L)
# ---------------------------------------------------------------------------

def take_snapshot(k: dict):
    today = date.today().isoformat()
    row = {
        "date": today,
        "invested": round(k["invested"], 2),
        "current_value": round(k["current"], 2),
        "unrealised_pnl": round(k["unrealised"], 2),
        "realised_pnl": round(k["realised"], 2),
        "total_pnl": round(k["total_pnl"], 2),
        "n_holdings": k["n_holdings"],
    }
    if SNAPSHOT_FILE.exists():
        df = pd.read_csv(SNAPSHOT_FILE)
        df = df[df["date"] != today]   # overwrite today's snapshot
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])
    df.sort_values("date").to_csv(SNAPSHOT_FILE, index=False)
    return df


def load_snapshots() -> pd.DataFrame:
    if SNAPSHOT_FILE.exists():
        df = pd.read_csv(SNAPSHOT_FILE)
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# UI HELPERS
# ---------------------------------------------------------------------------

def fmt_inr(x, decimals=0):
    if pd.isna(x):
        return "—"
    return f"₹{x:,.{decimals}f}"


def fmt_pct(x, decimals=2):
    if pd.isna(x):
        return "—"
    return f"{x:+.{decimals}f}%"


def color_pnl(val):
    if pd.isna(val) or val == 0:
        return "color: #888;"
    return "color: #16a34a;" if val > 0 else "color: #dc2626;"


# ---------------------------------------------------------------------------
# MAIN APP
# ---------------------------------------------------------------------------

def main():
    if not password_gate():
        return

    # ---------- SIDEBAR ----------
    st.sidebar.title("⚙️ Settings")

    default_url = st.secrets.get("DEFAULT_SHEET_URL", "") if hasattr(st, "secrets") else ""
    default_url = default_url or os.environ.get("DEFAULT_SHEET_URL", "")

    source_type = st.sidebar.radio("Data source", ["Google Sheet", "Upload Excel"], index=0)

    if source_type == "Google Sheet":
        sheet_url = st.sidebar.text_input(
            "Google Sheet URL",
            value=default_url,
            help="Paste the share URL of your portfolio sheet. Set sharing to 'Anyone with link can view'.",
        )
        source = sheet_url if sheet_url else None
    else:
        upload = st.sidebar.file_uploader("Upload portfolio Excel", type=["xlsx"])
        source = upload

    if not source:
        st.title("📈 Portfolio Dashboard")
        st.info("👈 Paste your Google Sheet URL or upload your portfolio Excel to begin.")
        st.markdown(
            "**Required columns in Sheet 1 (Holdings):** "
            "`Stock Name`, `Quantity`, `Purchase Cost`, `Amount Invested`"
        )
        return

    if st.sidebar.button("🔄 Force refresh prices"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")
    st.sidebar.caption(f"Prices cached: {PRICE_CACHE_TTL // 60} min")

    # ---------- LOAD DATA ----------
    try:
        holdings_raw = load_holdings(source)
    except Exception as e:
        st.error(f"Could not load holdings: {e}")
        return

    realised = load_realised(source)
    watchlist = load_watchlist(source)
    notes = load_notes(source)

    tickers = tuple(sorted(holdings_raw["Ticker"].unique()))

    with st.spinner("Fetching live prices..."):
        prices = fetch_live_prices(tickers)
    with st.spinner("Loading fundamentals..."):
        fundamentals = fetch_fundamentals(tickers)

    enriched = enrich_holdings(holdings_raw, prices, fundamentals)
    k = kpis(enriched, realised)

    # ---------- HEADER ----------
    st.title("📈 Portfolio Dashboard")
    st.caption(f"Tracking {k['n_holdings']} holdings · Updated {datetime.now().strftime('%I:%M %p, %d %b %Y')}")

    # ---------- KPI CARDS ----------
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Invested", fmt_inr(k["invested"]))
    c2.metric("Current Value", fmt_inr(k["current"]),
              fmt_pct(k["unrealised_pct"]))
    c3.metric("Unrealised P&L", fmt_inr(k["unrealised"]),
              fmt_pct(k["unrealised_pct"]))
    c4.metric("Today's P&L", fmt_inr(k["day_pnl"]),
              fmt_pct(k["day_pnl"] / k["current"] * 100 if k["current"] else 0))
    c5.metric("Realised P&L (lifetime)", fmt_inr(k["realised"]))

    st.divider()

    # ---------- TABS ----------
    tabs = st.tabs([
        "📊 Holdings",
        "🥧 Allocation",
        "👀 Watchlist",
        "💰 Realised P&L",
        "📈 History",
        "📝 Notes",
    ])

    # ============ HOLDINGS ============
    with tabs[0]:
        col_a, col_b = st.columns([3, 1])
        with col_b:
            sort_by = st.selectbox(
                "Sort by",
                ["Day Change %", "P&L %", "P&L", "Allocation %", "Current Value", "Short Name"],
                index=0,
            )

        view = enriched[[
            "Short Name", "Ticker", "Quantity", "Purchase Cost", "CMP",
            "Day Change %", "Current Value", "P&L", "P&L %", "Allocation %",
            "Sector", "Market Cap (Cr)", "PE (live)",
        ]].copy()

        ascending = sort_by == "Short Name"
        view = view.sort_values(sort_by, ascending=ascending, na_position="last")

        styled = (
            view.style
            .format({
                "Quantity": "{:,.0f}",
                "Purchase Cost": "₹{:,.2f}",
                "CMP": "₹{:,.2f}",
                "Day Change %": "{:+.2f}%",
                "Current Value": "₹{:,.0f}",
                "P&L": "₹{:,.0f}",
                "P&L %": "{:+.2f}%",
                "Allocation %": "{:.1f}%",
                "Market Cap (Cr)": "{:,.0f}",
                "PE (live)": "{:.2f}",
            }, na_rep="—")
            .map(color_pnl, subset=["Day Change %", "P&L", "P&L %"])
        )
        st.dataframe(styled, use_container_width=True, height=620, hide_index=True)

        # Day's movers
        with st.expander("🚀 Today's biggest movers"):
            movers = enriched.sort_values("Day Change %", ascending=False)
            cm1, cm2 = st.columns(2)
            with cm1:
                st.markdown("**Top gainers**")
                for _, r in movers.head(3).iterrows():
                    if pd.notna(r["Day Change %"]):
                        st.markdown(f"🟢 **{r['Short Name']}** &nbsp; {r['Day Change %']:+.2f}% &nbsp; ({fmt_inr(r['Day P&L'])})")
            with cm2:
                st.markdown("**Top losers**")
                for _, r in movers.tail(3).iloc[::-1].iterrows():
                    if pd.notna(r["Day Change %"]):
                        st.markdown(f"🔴 **{r['Short Name']}** &nbsp; {r['Day Change %']:+.2f}% &nbsp; ({fmt_inr(r['Day P&L'])})")

    # ============ ALLOCATION ============
    with tabs[1]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("By Stock")
            fig = px.pie(
                enriched, values="Current Value", names="Short Name",
                hole=0.45,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=False, height=480, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("By Sector")
            sec = enriched.groupby("Sector", dropna=False)["Current Value"].sum().reset_index()
            sec = sec.sort_values("Current Value", ascending=True)
            fig2 = px.bar(
                sec, x="Current Value", y="Sector", orientation="h",
                text=sec["Current Value"].apply(lambda x: f"₹{x/1000:,.0f}k"),
            )
            fig2.update_layout(height=480, margin=dict(t=10, b=10),
                               xaxis_title="", yaxis_title="")
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Concentration")
        top5 = enriched.nlargest(5, "Current Value")["Allocation %"].sum()
        top3 = enriched.nlargest(3, "Current Value")["Allocation %"].sum()
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Top 3 holdings", f"{top3:.1f}%")
        cc2.metric("Top 5 holdings", f"{top5:.1f}%")
        cc3.metric("# of holdings", k["n_holdings"])

    # ============ WATCHLIST ============
    with tabs[2]:
        st.markdown(
            "_To add stocks: edit the **Watchlist** tab in your Google Sheet. "
            "Both you and your friend can edit it. Changes appear here within ~2 minutes._"
        )
        if watchlist.empty:
            st.info("No watchlist items yet. Add them in the Watchlist tab of your Google Sheet.")
        else:
            wl = watchlist.copy()
            wl["Ticker"] = wl["Stock Name"].apply(extract_yf_ticker)
            wl_tickers = tuple(t for t in wl["Ticker"].dropna().unique())
            if wl_tickers:
                wl_prices = fetch_live_prices(wl_tickers)
                wl = wl.merge(wl_prices, on="Ticker", how="left")
                if "Target Buy Price" in wl.columns:
                    wl["Target Buy Price"] = wl["Target Buy Price"].apply(_to_number)
                    wl["Distance to Target %"] = ((wl["CMP"] - wl["Target Buy Price"])
                                                   / wl["Target Buy Price"]) * 100
            wl["Short Name"] = wl["Stock Name"].apply(short_name)
            cols = ["Short Name", "Ticker", "CMP", "Day Change %"]
            for c in ["Target Buy Price", "Distance to Target %", "Notes", "Added By"]:
                if c in wl.columns:
                    cols.append(c)
            st.dataframe(
                wl[cols].style.format({
                    "CMP": "₹{:,.2f}",
                    "Day Change %": "{:+.2f}%",
                    "Target Buy Price": "₹{:,.2f}",
                    "Distance to Target %": "{:+.2f}%",
                }, na_rep="—").map(color_pnl, subset=["Day Change %"]),
                use_container_width=True, hide_index=True, height=520,
            )

    # ============ REALISED P&L ============
    with tabs[3]:
        if realised.empty:
            st.info("No realised trades found.")
        else:
            r1, r2, r3, r4 = st.columns(4)
            total_realised = realised["Gain/Loss"].sum()
            wins = (realised["Gain/Loss"] > 0).sum()
            losses = (realised["Gain/Loss"] < 0).sum()
            win_rate = wins / max(wins + losses, 1) * 100
            r1.metric("Total realised P&L", fmt_inr(total_realised))
            r2.metric("Trades closed", len(realised))
            r3.metric("Win rate", f"{win_rate:.0f}%")
            r4.metric("Avg trade P&L", fmt_inr(realised["Gain/Loss"].mean()))

            display_cols = [c for c in ["Short Name", "Quantity", "Purchase Cost",
                                         "Selling Price", "Sale consideration",
                                         "Gain/Loss", "%Gain/Loss",
                                         "Sale Date", "Buy date", "No of days"]
                            if c in realised.columns]
            view_r = realised[display_cols].copy().sort_values("Gain/Loss", ascending=False)
            st.dataframe(
                view_r.style.format({
                    "Quantity": "{:,.0f}",
                    "Purchase Cost": "₹{:,.2f}",
                    "Selling Price": "₹{:,.2f}",
                    "Sale consideration": "₹{:,.0f}",
                    "Gain/Loss": "₹{:,.0f}",
                    "%Gain/Loss": "{:+.2%}",
                }, na_rep="—").map(color_pnl, subset=["Gain/Loss", "%Gain/Loss"]),
                use_container_width=True, hide_index=True, height=520,
            )

            st.subheader("Best & worst closed trades")
            best = realised.nlargest(5, "Gain/Loss")[["Short Name", "Gain/Loss", "%Gain/Loss"]]
            worst = realised.nsmallest(5, "Gain/Loss")[["Short Name", "Gain/Loss", "%Gain/Loss"]]
            cb, cw = st.columns(2)
            with cb:
                st.markdown("**🏆 Top 5 winners**")
                st.dataframe(best, use_container_width=True, hide_index=True)
            with cw:
                st.markdown("**💀 Top 5 losers**")
                st.dataframe(worst, use_container_width=True, hide_index=True)

    # ============ HISTORY ============
    with tabs[4]:
        st.markdown(
            "_Snapshots track portfolio value over time. Click the button below to record today's "
            "value — over weeks/months you'll get a real equity curve._"
        )

        cs1, cs2 = st.columns([1, 3])
        with cs1:
            if st.button("📸 Take snapshot now", type="primary"):
                take_snapshot(k)
                st.success(f"Snapshot saved for {date.today().isoformat()}")
                st.rerun()

        snaps = load_snapshots()
        if snaps.empty:
            st.info("No snapshots yet. Take your first one above.")
        else:
            cs2.caption(f"{len(snaps)} snapshots · "
                        f"first: {snaps['date'].min().date()} · "
                        f"latest: {snaps['date'].max().date()}")

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=snaps["date"], y=snaps["invested"],
                name="Invested", mode="lines",
                line=dict(color="#94a3b8", dash="dash"),
            ))
            fig.add_trace(go.Scatter(
                x=snaps["date"], y=snaps["current_value"],
                name="Current Value", mode="lines+markers",
                line=dict(color="#0ea5e9", width=3),
                fill="tonexty", fillcolor="rgba(14,165,233,0.1)",
            ))
            fig.update_layout(
                title="Portfolio value over time",
                height=420, margin=dict(t=50, b=10),
                hovermode="x unified",
                yaxis_title="₹", xaxis_title="",
            )
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Snapshot history")
            st.dataframe(snaps.iloc[::-1], use_container_width=True, hide_index=True)

            with open(SNAPSHOT_FILE, "rb") as f:
                st.download_button("⬇️ Download snapshots.csv", f.read(),
                                   file_name="snapshots.csv", mime="text/csv")

    # ============ NOTES ============
    with tabs[5]:
        st.markdown(
            "_Notes from your investment partner. To add a note, edit the **Notes** tab in the Google Sheet._"
        )
        if notes.empty:
            st.info("No notes yet. Add them in the Notes tab of your Google Sheet.")
        else:
            for _, n in notes.iloc[::-1].iterrows():
                d = n.get("Date", "")
                a = n.get("Author", "")
                txt = n.get("Note", "")
                with st.container(border=True):
                    st.markdown(f"**{a}** · _{d}_")
                    st.markdown(txt)


if __name__ == "__main__":
    main()
