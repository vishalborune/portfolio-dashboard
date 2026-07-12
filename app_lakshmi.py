import streamlit as st

st.title("Bisect stage 14 - real login and rerun sequence")

if "role" not in st.session_state:
    st.write("BEFORE LOGIN: showing password box")
    pw = st.text_input("Password", type="password", key="pw14")
    if st.button("Enter"):
        st.write("Password submitted, checking...")
        import os
        expected = os.environ.get("LAKSHMI_PASSWORD", "")
        if pw == expected:
            st.write("Password correct. About to set session_state...")
            st.session_state.role = "lakshmi"
            st.session_state.user = "Lakshmi"
            st.session_state.portfolios = {2: "Lakshmi", 3: "Abinaya"}
            st.session_state.portfolio_id = 2
            st.write("session_state set. About to call st.rerun() NOW...")
            st.rerun()
        else:
            st.write("Password did not match.")
    st.stop()

st.write("AFTER RERUN: role is set, we made it past st.rerun()")
st.write(f"role = {st.session_state.get('role')}")
st.write("STAGE 14 CLEAR - rerun survived, now loading real app.main()")

import app
app.main()
