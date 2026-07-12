STAGE = "4b"

import streamlit as st

st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

import numpy as np
import pandas as pd
st.write(f"Stage 1 OK — pandas {pd.__version__}, numpy {np.__version__}")

import plotly.express as px
st.write("Stage 2 OK — plotly imported")

from supabase import create_client
st.write("Stage 3 OK — supabase imported")

import yfinance as yf
st.write("Stage 4 OK — yfinance imported")

st.write("Now making a REAL Yahoo Finance network call...")
data = yf.download("RELIANCE.NS", period="5d", progress=False)
st.write(f"Stage 4b OK — fetched {len(data)} rows from Yahoo")
st.dataframe(data.tail(3))
