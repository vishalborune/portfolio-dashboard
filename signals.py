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
import yfinance as yf

# signals.py is shared by the Streamlit dashboard AND the headless alert
# engine (GitHub Actions). Streamlit may not be installed in the latter.
try:
    import streamlit as st
    _cache = st.cache_data
except ImportError:
    def _cache(**kwargs):
        def deco(fn):
            return fn
        return deco

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

def _fetch_weekly_from_bhavcopy(ticker: str) -> pd.DataFrame:
    """Fallback source for stocks Yahoo doesn't carry at all (NSE Emerge /
    BSE SME). Builds weekly bars by resampling our own bhavcopy-derived
    daily table (see bhavcopy.py, Sprint 3). Empty df if this ticker has
    no bhavcopy history either (e.g. backfill hasn't run yet) -- caller
    treats that exactly like any other missing-data case."""
    try:
        import db
        daily = db.get_sme_daily_prices((ticker,))
        if daily.empty:
            return pd.DataFrame()
        daily = daily.set_index("price_date").sort_index()
        weekly = daily.resample("W-FRI").agg({
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum",
        }).dropna(subset=["close"]).reset_index()
        return weekly.rename(columns={"price_date": "date"})[
            ["date", "open", "high", "low", "close", "volume"]]
    except Exception:
        return pd.DataFrame()


@_cache(ttl=60 * 60 * 4)  # weekly bars barely change intraday; 4h cache
def fetch_weekly(ticker: str, period: str = "3y") -> pd.DataFrame:
    """Weekly OHLCV for one ticker. Empty df on failure.

    Order matters (hardened 12 Jul 2026): our own bhavcopy table is checked
    FIRST. For the SME tickers it tracks, the official NSE/BSE EOD data is
    authoritative — Yahoo's SME coverage, when it answers at all, can serve
    stale or wrong-instrument bars, which was silently corrupting the
    flowchart states for those stocks. Non-SME tickers aren't in the table,
    return empty here, and proceed to Yahoo exactly as before."""
    bhav = _fetch_weekly_from_bhavcopy(ticker)
    if not bhav.empty:
        return bhav
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
        return _fetch_weekly_from_bhavcopy(ticker)


# ---------------------------------------------------------------------------
# Indicator computation
# ---------------------------------------------------------------------------

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add EMAs, convergence flag, swing S/R, volume ratio to a weekly OHLCV frame."""
    out = df.copy()
    out["ema10"] = out["close"].ewm(span=10, adjust=False).mean()
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema40"] = out["close"].ewm(span=40, adjust=False).mean()

    # Volume layer: this week's volume vs its 10-week average
    # (10wk chosen by Lakshmi — decisions anchor on the 10-week timeframe)
    if "volume" in out.columns:
        out["vol_avg10"] = out["volume"].rolling(10).mean()
        out["vol_ratio"] = out["volume"] / out["vol_avg10"]
    else:
        out["vol_avg10"] = np.nan
        out["vol_ratio"] = np.nan

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
        "vol_ratio": round(float(row["vol_ratio"]), 2) if ("vol_ratio" in row and pd.notna(row["vol_ratio"])) else None,
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


@_cache(ttl=60 * 60 * 4)
def states_for_holdings(tickers: tuple) -> pd.DataFrame:
    """Compute the current flowchart state for every holding. One row per ticker."""
    _cols = ["Ticker", "State", "State Display", "State Reason", "State Priority",
             "EMA10", "EMA20", "EMA40", "Vol vs 10wk"]
    if not tickers:
        return pd.DataFrame(columns=_cols)
    rows = []
    for t in tickers:
        d = current_state(t)
        rows.append({
            "Ticker": t,
            "State": d["state"],
            "State Display": STATE_EMOJI.get(d["state"], d["state"]),
            "State Reason": d.get("reason", ""),
            "State Priority": STATE_PRIORITY.get(d["state"], 9),
            "EMA10": d.get("ema10"),
            "EMA20": d.get("ema20"),
            "EMA40": d.get("ema40"),
            "Vol vs 10wk": d.get("vol_ratio"),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# DAILY ENTRY TRANCHES (Lakshmi's staged-entry system for WATCHLIST stocks)
# 1st tranche: price pulls back to the 10-day EMA
# 2nd (final) tranche: price pulls back to the 21-day EMA
# After entry the position moves to the portfolio, where the weekly system
# (10wEMA ≈ 50DMA) governs holding and exits.
# ---------------------------------------------------------------------------

TOUCH_BAND = 0.005   # close within ±0.5% of the EMA, or intraday low pierces it


def _fetch_daily(ticker: str, lookback: int = 260) -> pd.DataFrame:
    """Recent daily close+low for entry-tranche math. Source order mirrors
    fetch_weekly (house rule #1 — own the data for SME): our bhavcopy table
    FIRST (authoritative + now split/bonus-adjusted on read), Yahoo only for
    mainboard names it doesn't track. Empty df on failure. Returns a plain
    2-column frame {close, low}."""
    # 1) bhavcopy (SME / Emerge / BSE-only). Was the silent gap: the old
    #    Yahoo-only path returned nothing for these, so their watchlist/holding
    #    entry zones never computed at all.
    try:
        import db
        d = db.get_sme_daily_prices((ticker,))
        if not d.empty and "close" in d.columns and "low" in d.columns:
            d = d.sort_values("price_date").tail(lookback)
            return pd.DataFrame({"close": d["close"].astype(float).to_numpy(),
                                 "low": d["low"].astype(float).to_numpy()})
    except Exception:
        pass
    # 2) Yahoo (mainboard NSE names)
    try:
        df = yf.download(ticker, period="6mo", interval="1d",
                         progress=False, auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()
        close = df["Close"].dropna()
        if hasattr(close, "columns"):        # multi-index from yf
            close = close.iloc[:, 0]
        low = df["Low"].dropna()
        if hasattr(low, "columns"):
            low = low.iloc[:, 0]
        return pd.DataFrame({"close": close.astype(float).to_numpy(),
                             "low": low.astype(float).to_numpy()})
    except Exception:
        return pd.DataFrame()


def daily_entry_state(ticker: str) -> dict | None:
    """Entry-tranche status off daily bars. None if data unavailable."""
    try:
        d = _fetch_daily(ticker)
        if d.empty or len(d) < 30:
            return None
        close = d["close"]
        low = d["low"]
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        ema21 = close.ewm(span=21, adjust=False).mean().iloc[-1]
        cmp_ = float(close.iloc[-1])
        day_low = float(low.iloc[-1])

        def touching(ema):
            # intraday low pierced the EMA, or close sits within the band
            return day_low <= ema or abs(cmp_ / ema - 1) <= TOUCH_BAND

        pct10 = (cmp_ / ema10 - 1) * 100
        pct21 = (cmp_ / ema21 - 1) * 100

        if cmp_ < ema21 * (1 - TOUCH_BAND):
            zone, advice = "BELOW 21DMA", "🔴 Below both — no add, wait for repair"
        elif touching(ema21):
            zone, advice = "TRANCHE 2", "🎯 At 21DMA — 2nd & FINAL tranche zone"
        elif touching(ema10):
            zone, advice = "TRANCHE 1", "🟢 At 10DMA — 1st tranche zone"
        else:
            zone, advice = "EXTENDED", f"⏳ {pct10:+.1f}% above 10DMA — wait for pullback"

        return {"Ticker": ticker, "CMP (d)": cmp_, "10DMA": round(float(ema10), 2),
                "21DMA": round(float(ema21), 2), "% vs 10DMA": round(pct10, 1),
                "% vs 21DMA": round(pct21, 1), "Entry Zone": zone, "Entry Advice": advice}
    except Exception:
        return None


def entry_states_for_watchlist(tickers: tuple) -> "pd.DataFrame":
    _cols = ["Ticker", "CMP (d)", "10DMA", "21DMA", "% vs 10DMA", "% vs 21DMA",
             "Entry Zone", "Entry Advice"]
    if not tickers:
        return pd.DataFrame(columns=_cols)
    rows = [r for t in tickers if (r := daily_entry_state(t))]
    return pd.DataFrame(rows, columns=_cols) if rows else pd.DataFrame(columns=_cols)
