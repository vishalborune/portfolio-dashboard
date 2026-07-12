STAGE = 5

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

import numpy as np
import pandas as pd
import plotly.express as px
from supabase import create_client
import yfinance as yf
st.write("Stages 1-4 imports OK")

import scipy
st.write(f"scipy {scipy.__version__} imported OK")
from scipy.optimize import brentq
result = brentq(lambda x: x**2 - 4, 0, 5)
st.write(f"scipy brentq OK — result {result}")

import openpyxl
st.write(f"openpyxl {openpyxl.__version__} imported OK")

st.write("ALL LIBRARIES CLEAR — bug is in our own app.py code")
