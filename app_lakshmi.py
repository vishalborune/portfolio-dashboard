import streamlit as st

st.title("Bisect stage 15 retry - real login_gate, repeat test")

import app as appmod

st.write("About to call appmod.login_gate()...")
result = appmod.login_gate()
st.write(f"login_gate() returned: {result}")

if result:
    st.write("SUCCESS - real login_gate returned True, rerun survived this time")
