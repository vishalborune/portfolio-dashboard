"""
signals.py — Lakshmi's TheWrap TA Rules flowchart, as code.

Locked definitions (July 2026):
  - EMAs: 10, 20, 40-week (weekly closes)
  - Convergence: max(EMA10,20,40) - min(EMA10,20,40) <= 2% of their mean
  - Support/Resistance: most recent confirmed swing low/high on weekly closes
    (5-week centered pivot)
  - Six states: EXIT | BULLISH SIGNAL | WAIT/WATCH | BE CAUTIOUS |
                MOMENTUM FADING | MAINTAIN/ADD

Flowchart:
  Are EMAs converging?
    YES -> broken support?    YES -> EXIT
                              NO  -> broken resistance? YES -> BULLISH SIGNAL
                                                        NO  -> WAIT/WATCH
    NO  -> broken 40W EMA?    YES -> EXIT
                              NO  -> broken 20W EMA?  YES -> BE CAUTIOUS
                                                      NO  -> broken 10W EMA?
                                                              YES -> MOMENTUM FADING
                                                              NO  -> MAINTAIN/ADD
"""

from __future__ import annotations
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

CONVERGENCE_BAND_PCT = 2.0   # Lakshmi's locked number
SWING_WINDOW = 5             # centered pivot window (weeks)
MIN_WEEKS_REQUIRED = 45      # need enough history for a meaningful 40W EMA

# EXIT confirmation buffer (Lakshmi's option (a), July 2026):
# a single weekly close below the 40W EMA is NOT an exit — it takes either
# two consecutive closes below, or one close this far below in one week.
EXIT_CONFIRM_WEEKS = 2       # consecutive closes below 40W EMA required
EXIT_HARD_BREAK_PCT = 3.0    # ...unless a single close is 3%+ below the 40W EMA

STATE_EMOJI = {
    "EXIT": "🔴 EXIT",
    "BULLISH SIGNAL": "🟢 BULLISH SIGNAL",
    "WAIT/WATCH": "🔵 WAIT/WATCH",
    "BE CAUTIOUS": "🟠 BE CAUTIOUS",
    "MOMENTUM FADING": "🟣 MOMENTUM FADING",
    "MAINTAIN/ADD": "🟢 MAINTAIN/ADD",
    "INSUFFICIENT DATA": "⚪ INSUFFICIENT DATA",
    "NO DATA": "⚪ NO DATA",
}

# Sort priority for the Holdings table (most urgent first)
STATE_PRIORITY = {
    "EXIT": 0,
    "BE CAUTIOUS": 1,
    "MOMENTUM FADING": 2,
    "WAIT/WATCH": 3,
    "BULLISH SIGNAL": 4,
    "MAINTAIN/ADD": 5,
    "INSUFFICIENT DATA": 6,
    "NO DATA": 7,
}


# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60 * 60 * 4)  # weekly bars barely change intraday; 4h cache
def fetch_weekly(ticker: str, period: str = "3y") -> pd.DataFrame:
    """Weekly OHLCV for one ticker. Empty df on failure."""
    try:
        df = yf.download(ticker, period=period, interval="1wk",
                         progress=False, auto_adjust=False)
        if df.empty:
            return pd.DataFrame()
        # yfinance returns MultiIndex columns for single ticker sometimes
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.reset_index()
        df = df.rename(columns={"Date": "date", "Close": "close",
                                 "High": "high", "Low": "low",
                                 "Open": "open", "Volume": "volume"})
        df = df.dropna(subset=["close"]).reset_index(drop=True)
        return df[["date", "open", "high", "low", "close", "volume"]]
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Indicator computation
# ---------------------------------------------------------------------------

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add EMAs, convergence flag, swing S/R to a weekly OHLCV frame."""
    out = df.copy()
    out["ema10"] = out["close"].ewm(span=10, adjust=False).mean()
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema40"] = out["close"].ewm(span=40, adjust=False).mean()

    ema_stack = out[["ema10", "ema20", "ema40"]]
    out["ema_spread_pct"] = (
        (ema_stack.max(axis=1) - ema_stack.min(axis=1))
        / ema_stack.mean(axis=1) * 100
    )
    out["converging"] = out["ema_spread_pct"] <= CONVERGENCE_BAND_PCT

    # Swing pivots: a bar whose close is the min/max of the centered window
    w = SWING_WINDOW
    roll_min = out["close"].rolling(w, center=True).min()
    roll_max = out["close"].rolling(w, center=True).max()
    is_swing_low = out["close"] == roll_min
    is_swing_high = out["close"] == roll_max

    support, resistance = [], []
    cur_sup, cur_res = np.nan, np.nan
    for i in range(len(out)):
        # Note: centered window means the last w//2 bars can't confirm a pivot
        # yet — support/resistance carries forward from the last confirmed one,
        # which is exactly the behaviour we want.
        if bool(is_swing_low.iloc[i]) and pd.notna(roll_min.iloc[i]):
            cur_sup = out["close"].iloc[i]
        if bool(is_swing_high.iloc[i]) and pd.notna(roll_max.iloc[i]):
            cur_res = out["close"].iloc[i]
        support.append(cur_sup)
        resistance.append(cur_res)
    out["support"] = support
    out["resistance"] = resistance
    return out


# ---------------------------------------------------------------------------
# The flowchart itself
# ---------------------------------------------------------------------------

def classify_row(row, prev_row=None) -> dict:
    """Run one weekly bar through the flowchart. Returns state + reasons.

    prev_row enables the EXIT confirmation buffer (option (a)):
    in the trending branch, EXIT fires only if this is the SECOND consecutive
    close below the 40W EMA, or a single close 3%+ below it. A first, mild
    close below lands in BE CAUTIOUS instead (warning week).
    """
    detail = {
        "converging": bool(row["converging"]),
        "ema_spread_pct": round(float(row["ema_spread_pct"]), 2),
        "close": round(float(row["close"]), 2),
        "ema10": round(float(row["ema10"]), 2),
        "ema20": round(float(row["ema20"]), 2),
        "ema40": round(float(row["ema40"]), 2),
        "support": round(float(row["support"]), 2) if pd.notna(row["support"]) else None,
        "resistance": round(float(row["resistance"]), 2) if pd.notna(row["resistance"]) else None,
    }

    if pd.isna(row["support"]) or pd.isna(row["resistance"]):
        detail["state"] = "INSUFFICIENT DATA"
        detail["reason"] = "Not enough history to confirm swing support/resistance yet"
        return detail

    close = row["close"]

    if row["converging"]:
        # LEFT BRANCH — consolidation
        if close < row["support"]:
            detail["state"] = "EXIT"
            detail["reason"] = (f"EMAs converged ({detail['ema_spread_pct']}% spread) and close "
                                f"₹{detail['close']} broke below swing support ₹{detail['support']}")
        elif close > row["resistance"]:
            detail["state"] = "BULLISH SIGNAL"
            detail["reason"] = (f"EMAs converged and close ₹{detail['close']} broke above "
                                f"swing resistance ₹{detail['resistance']}")
        else:
            detail["state"] = "WAIT/WATCH"
            detail["reason"] = (f"EMAs converged ({detail['ema_spread_pct']}% spread); price inside "
                                f"₹{detail['support']}–₹{detail['resistance']} range")
    else:
        # RIGHT BRANCH — trending
        below_40 = close < row["ema40"]
        hard_break = close < row["ema40"] * (1 - EXIT_HARD_BREAK_PCT / 100)
        prev_below_40 = (prev_row is not None
                         and pd.notna(prev_row.get("ema40"))
                         and prev_row["close"] < prev_row["ema40"])

        if below_40 and (hard_break or prev_below_40):
            detail["state"] = "EXIT"
            if hard_break:
                detail["reason"] = (f"Close ₹{detail['close']} is {EXIT_HARD_BREAK_PCT}%+ below the "
                                    f"40-wk EMA ₹{detail['ema40']} — hard break, exit confirmed")
            else:
                detail["reason"] = (f"Second consecutive weekly close below the 40-wk EMA "
                                    f"(₹{detail['close']} vs ₹{detail['ema40']}) — exit confirmed")
        elif below_40:
            # First mild close below 40W — warning week, not exit yet
            detail["state"] = "BE CAUTIOUS"
            detail["reason"] = (f"⚠️ First weekly close below the 40-wk EMA "
                                f"(₹{detail['close']} vs ₹{detail['ema40']}) — one more weekly close "
                                f"below, or a 3%+ break, confirms EXIT")
        elif close < row["ema20"]:
            detail["state"] = "BE CAUTIOUS"
            detail["reason"] = (f"Close ₹{detail['close']} below 20-wk EMA ₹{detail['ema20']} "
                                f"but holding 40-wk ₹{detail['ema40']}")
        elif close < row["ema10"]:
            detail["state"] = "MOMENTUM FADING"
            detail["reason"] = (f"Close ₹{detail['close']} below 10-wk EMA ₹{detail['ema10']} "
                                f"but holding 20-wk ₹{detail['ema20']}")
        else:
            detail["state"] = "MAINTAIN/ADD"
            detail["reason"] = (f"Close ₹{detail['close']} above all EMAs "
                                f"(10wk ₹{detail['ema10']} / 20wk ₹{detail['ema20']} / "
                                f"40wk ₹{detail['ema40']}) — trend healthy")
    return detail


def classify_series(df: pd.DataFrame) -> pd.DataFrame:
    """Classify every bar in a weekly frame. Adds 'state' and 'reason' columns."""
    ind = compute_indicators(df)
    states, reasons = [], []
    prev = None
    for _, row in ind.iterrows():
        d = classify_row(row, prev_row=prev)
        states.append(d["state"])
        reasons.append(d["reason"])
        prev = row
    ind["state"] = states
    ind["reason"] = reasons
    return ind


def current_state(ticker: str) -> dict:
    """Fetch weekly data for one ticker and return the latest state + detail."""
    df = fetch_weekly(ticker)
    if df.empty or len(df) < MIN_WEEKS_REQUIRED:
        return {"state": "NO DATA" if df.empty else "INSUFFICIENT DATA",
                "reason": "Could not fetch weekly data" if df.empty
                          else f"Only {len(df)} weeks of history (need {MIN_WEEKS_REQUIRED}+)",
                "ticker": ticker}
    ind = compute_indicators(df)
    prev = ind.iloc[-2] if len(ind) >= 2 else None
    d = classify_row(ind.iloc[-1], prev_row=prev)
    d["ticker"] = ticker
    d["as_of"] = str(ind["date"].iloc[-1].date()) if hasattr(ind["date"].iloc[-1], "date") else str(ind["date"].iloc[-1])
    return d


@st.cache_data(ttl=60 * 60 * 4)
def states_for_holdings(tickers: tuple) -> pd.DataFrame:
    """Compute the current flowchart state for every holding. One row per ticker."""
    rows = []
    for t in tickers:
        d = current_state(t)
        rows.append({
            "Ticker": t,
            "State": d["state"],
            "State Display": STATE_EMOJI.get(d["state"], d["state"]),
            "State Reason": d.get("reason", ""),
            "State Priority": STATE_PRIORITY.get(d["state"], 9),
        })
    return pd.DataFrame(rows)
