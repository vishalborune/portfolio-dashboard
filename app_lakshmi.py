STAGE = 6

import streamlit as st
st.title(f"🔬 Bisect stage {STAGE}")
st.write("If you can read this, this stage is ALIVE.")

import numpy as np
import pandas as pd
import plotly.express as px
from supabase import create_client
import yfinance as yf
import scipy
import openpyxl
st.write("All external libraries OK")

import db
st.write("db.py imported OK")

import signals
st.write("signals.py imported OK")

st.write("STAGE 6 CLEAR — the crash is inside app.py itself")
