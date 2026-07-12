STAGE = 10

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

import os
os.environ["APP_TENANT"] = "lakshmi"
st.session_state["portfolio_id"] = 2
st.session_state["portfolios"] = {2: "Lakshmi", 3: "Abinaya"}

import db
wl = db.get_watchlist()
st.write(f"Watchlist has {len(wl)} row(s)")
st.dataframe(wl)

import app as appmod
tickers = tuple(t for t in wl["Ticker"].dropna().unique()) if not wl.empty and "Ticker" in wl.columns else ()
st.write(f"Tickers to fetch: {tickers}")

if tickers:
    st.write("Calling fetch_live_prices() — the threaded quote fetcher...")
    prices = appmod.fetch_live_prices(tickers)
    st.write("OK — fetch_live_prices survived!")
    st.dataframe(prices)
else:
    st.write("No tickers in watchlist — nothing to fetch, trying a synthetic one instead")
    prices = appmod.fetch_live_prices(("RELIANCE.NS", "TCS.NS"))
    st.write("OK — fetch_live_prices survived with synthetic tickers!")
    st.dataframe(prices)

st.write("STAGE 10 CLEAR")
