STAGE = 11

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")

import os
os.environ["APP_TENANT"] = "lakshmi"
st.session_state["role"] = "lakshmi"
st.session_state["user"] = "Lakshmi"
st.session_state["portfolio_id"] = 2
st.session_state["portfolios"] = {2: "Lakshmi", 3: "Abinaya"}

import db, signals
import app as appmod

st.write("① Fetching holdings...")
holdings = db.get_holdings()
st.write(f"✅ {len(holdings)} holdings")

st.write("② Enriching...")
enriched = appmod.enrich_holdings(holdings)
st.write(f"✅ enriched shape {enriched.shape}")

st.write("③ Computing KPIs...")
k = appmod.compute_kpis(enriched)
st.write(f"✅ KPIs: {k}")

st.write("④ Fetching realised P&L...")
realised = db.get_realised()
st.write(f"✅ {len(realised)} realised rows")

st.write("⑤ Fetching transactions...")
txns = db.get_transactions()
st.write(f"✅ {len(txns)} transactions")

st.write("⑥ Fetching watchlist...")
wl = db.get_watchlist()
st.write(f"✅ {len(wl)} watchlist rows")

st.write("⑦ Fetching notes...")
notes = db.get_notes()
st.write(f"✅ {len(notes)} notes")

st.write("🎉 STAGE 11 CLEAR — all data functions survive in sequence")
