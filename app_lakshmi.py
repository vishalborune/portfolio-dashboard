STAGE = 8

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")
st.write("About to call app.main()...")

import app
app.main()
