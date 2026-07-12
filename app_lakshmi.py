STAGE = 7

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

st.write("About to import app.py (module-level code only, NOT calling main() yet)...")
import app
st.write("STAGE 7 CLEAR — app.py imported fine, crash must be inside main()")
