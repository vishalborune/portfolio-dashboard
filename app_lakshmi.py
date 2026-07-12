STAGE = 9

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

import os
os.environ["APP_TENANT"] = "lakshmi"

import db
st.session_state["portfolio_id"] = 2
st.session_state["portfolios"] = {2: "Lakshmi", 3: "Abinaya"}

st.write("Calling db.get_holdings()...")
holdings = db.get_holdings()
st.write(f"OK — got {len(holdings)} holdings")
st.dataframe(holdings)

import app as appmod
st.write("Calling app.enrich_holdings()...")
enriched = appmod.enrich_holdings(holdings)
st.write(f"OK — enriched shape {enriched.shape}")
st.dataframe(enriched)

st.write("STAGE 9 CLEAR")
