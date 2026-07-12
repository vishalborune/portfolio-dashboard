STAGE = 1

import streamlit as st

st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

import numpy as np
import pandas as pd
st.write(f"Stage 1 OK — pandas {pd.__version__}, numpy {np.__version__}")
df = pd.DataFrame({"a": [1.5, 2.5], "b": ["x", "y"]})
st.dataframe(df)
st.write("Stage 1b OK — dataframe rendered (pyarrow serialization works)")
