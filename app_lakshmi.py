import streamlit as st

st.title("Bisect stage 16 - testing st.secrets access alone")

st.write("About to check hasattr(st, 'secrets')...")
has_secrets = hasattr(st, "secrets")
st.write(f"hasattr(st, 'secrets') = {has_secrets}")

st.write("About to access st.secrets['APP_TENANT']...")
try:
    val = st.secrets["APP_TENANT"]
    st.write(f"SUCCESS - APP_TENANT = {val}")
except Exception as e:
    st.write(f"Exception (not a crash): {type(e).__name__}: {e}")

st.write("About to access st.secrets['LAKSHMI_PASSWORD']...")
try:
    val2 = st.secrets["LAKSHMI_PASSWORD"]
    st.write(f"SUCCESS - LAKSHMI_PASSWORD length = {len(val2)}")
except Exception as e:
    st.write(f"Exception (not a crash): {type(e).__name__}: {e}")

st.write("STAGE 16 CLEAR - st.secrets access survived")
