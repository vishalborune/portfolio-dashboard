STAGE = 13

import streamlit as st
st.title("Bisect stage 13")

import os
os.environ["APP_TENANT"] = "lakshmi"
st.session_state["role"] = "lakshmi"
st.session_state["user"] = "Lakshmi"
st.session_state["portfolio_id"] = 2
st.session_state["portfolios"] = {2: "Lakshmi", 3: "Abinaya"}

import db
import app as appmod

holdings = db.get_holdings()
enriched = appmod.enrich_holdings(holdings)
realised = db.get_realised()
k = appmod.compute_kpis(enriched, realised)

st.write("STEP 1: Sidebar portfolio_switcher")
appmod.portfolio_switcher()
st.write("STEP 1 done")

st.write("STEP 2: Sidebar refresh and auto-refresh toggle")
if st.sidebar.button("Refresh prices"):
    st.cache_data.clear()
auto = st.sidebar.toggle("Auto-refresh 5 min", value=False, key="check_auto")
st.write("STEP 2 done")

st.write("STEP 3: Sidebar price diagnostics toggle")
st.sidebar.toggle("Price diagnostics", value=False, key="show_price_diag")
st.write("STEP 3 done")

st.write("STEP 4: KPI title caption block")
st.title("Portfolio Dashboard")
badge = "Market OPEN" if appmod.market_is_open() else "Market CLOSED"
st.caption(f"Tracking {k['n_holdings']} holdings - {badge}")
st.write("STEP 4 done")

st.write("STEP 5: Real st.tabs with context managers")
tab_names = ["Holdings", "Allocation", "Watchlist", "Realised PL",
             "History", "Transactions", "Notes", "Import Holdings"]
tabs = st.tabs(tab_names)

with tabs[0]:
    appmod.tab_holdings(enriched)

with tabs[1]:
    appmod.tab_allocation(enriched, k)

with tabs[2]:
    appmod.tab_watchlist()

with tabs[3]:
    appmod.tab_realised(realised)

with tabs[4]:
    appmod.tab_history(k)

with tabs[5]:
    appmod.tab_transactions()

with tabs[6]:
    appmod.tab_notes()

with tabs[7]:
    appmod.tab_import_holdings()

st.write("STEP 5 done")
st.write("STAGE 13 CLEAR - this is now an exact replica of main")
