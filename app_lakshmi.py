STAGE = 3

import streamlit as st

st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

import numpy as np
import pandas as pd
st.write(f"Stage 1 OK — pandas {pd.__version__}, numpy {np.__version__}")
df = pd.DataFrame({"a": [1.5, 2.5], "b": ["x", "y"]})
st.dataframe(df)
st.write("Stage 1b OK — dataframe rendered (pyarrow serialization works)")

import plotly.express as px
fig = px.line(x=[1, 2, 3], y=[1, 4, 9])
st.plotly_chart(fig)
st.write("Stage 2 OK — plotly rendered")

from supabase import create_client
st.write("Stage 3 OK — supabase imported")
