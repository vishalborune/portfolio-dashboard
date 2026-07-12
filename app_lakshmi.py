STAGE = 13

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")

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

st.write("① Sidebar: portfolio_switcher...")
appmod.portfolio_switcher()
st.write("✅① done")

st.write("② Sidebar: refresh button + auto-refresh toggle...")
if st.sidebar.button("🔄 Refresh prices"):
    st.cache_data.clear()
auto = st.sidebar.toggle("⏱ Auto-refresh (5 min)", value=False, key="check_auto")
st.write("✅② done")

st.write("③ Sidebar: price diagnostics toggle...")
st.sidebar.toggle("🔍 Price diagnostics", value=False, key="show_price_diag")
st.write("✅③ done")

st.write("④ KPI
