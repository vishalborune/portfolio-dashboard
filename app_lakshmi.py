STAGE = 12

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
st.write("✅ Data ready, now rendering each tab's content directly...")

st.write("① tab_holdings...")
appmod.tab_holdings(enriched)
st.write("✅① done")

st.write("② tab_allocation...")
appmod.tab_allocation(enriched, k)
st.write("✅② done")

st.write("③ tab_watchlist...")
appmod.tab_watchlist()
st.write("✅③ done")

st.write("④ tab_realised...")
appmod.tab_realised(realised)
st.write("✅④ done")

st.write("⑤ tab_history...")
appmod.tab_history(k)
st.write("✅⑤ done")

st.write("⑥ tab_transactions...")
appmod.tab_transactions()
st.write("✅⑥ done")

st.write("⑦ tab_notes...")
appmod.tab_notes()
st.write("✅⑦ done")

st.write("⑧ tab_import_holdings...")
appmod.tab_import_holdings()
st.write("✅⑧ done")

st.write("🎉 STAGE 12 CLEAR — every tab renders fine")
