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
import re
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

def fy_bounds(today=None):
    """Indian financial year (1 Apr – 31 Mar) containing `today`. Returns
    (start_date, end_date, label) e.g. (2026-04-01, 2027-03-31, 'FY26-27').
    Used for YTD realised P&L, which resets every 1 April (Lakshmi 23-Jul-2026)."""
    from datetime import date
    d = today or date.today()
    sy = d.year if d.month >= 4 else d.year - 1
    return date(sy, 4, 1), date(sy + 1, 3, 31), f"FY{str(sy)[2:]}-{str(sy + 1)[2:]}"


CONVERGENCE_BAND_PCT = 2.0   # Lakshmi's locked number
SWING_WINDOW = 5             # centered pivot window (weeks)
MIN_WEEKS_REQUIRED = 45      # need enough history for a meaningful 40W EMA
# "Non-empty isn't usable" thresholds (house rule #7): a newly-tracked ticker has
# only a few bhavcopy rows until the backfill runs. Below these, fall back to
# Yahoo rather than compute EMAs/peaks off a handful of bars. Set well under what
# the long-tracked SME names carry (~500 days / ~100 weeks) so they're unaffected.
MIN_BHAV_DAILY_ROWS = 60
MIN_BHAV_WEEKS = 20

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
    # Same "non-empty != usable" guard as the daily path: a just-added ticker has
    # a couple of weekly bars until the backfill runs, which would show as
    # INSUFFICIENT DATA instead of its real state. Yahoo carries the history for
    # these mainboard-BSE names in the meantime.
    if not bhav.empty and len(bhav) >= MIN_BHAV_WEEKS:
        return bhav
    if not bhav.empty:
        print(f"  [signals] {ticker}: only {len(bhav)} bhavcopy week(s) "
              f"(<{MIN_BHAV_WEEKS}) — trying Yahoo for deeper history")
    try:
        df = yf.download(ticker, period=period, interval="1wk",
                         progress=False, auto_adjust=False)
        if df.empty:
            # Yahoo gave nothing. Return whatever bhavcopy has rather than an
            # empty frame: SOME real history beats none, and the caller labels a
            # short series honestly as "BUILDING n/45w" instead of the misleading
            # "NO PRICE DATA — fetch failed" this used to produce. Indo-Mim
            # (listed 30-Jul-2026, 6 weeks) read as a fetch failure on Render
            # purely because Yahoo is refused there (Vishal, 02-Sep-2026).
            return bhav
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


def _reconcile_last_week(ticker: str, df: pd.DataFrame) -> pd.DataFrame:
    """Guard against a STALE weekly close (House Rule #7 — "non-empty != usable").

    Yahoo's 1-week bar can silently MISS the latest session, so the
    just-completed week's close comes back stale-LOW. That once fired a FALSE
    'MOMENTUM FADING' on HFCL (03-Aug-2026): the weekly bar showed Thursday's
    close ₹184.69 — below the 10-wk EMA — instead of Friday's true close
    ₹193.92, which was ABOVE it (state should have stayed MAINTAIN/ADD). Same
    family as the Today's-P&L bug: Yahoo's endpoint dropping the most recent bar.

    Fix: cross-check the last weekly close against a FRESH daily fetch for the
    SAME ISO week and correct it if they disagree. The daily fetch is only
    trusted when it is at least as fresh as the weekly frame, so this can never
    move the close in the STALER direction. SME names resolve both sides from the
    same bhavcopy table, so they agree and nothing changes."""
    try:
        wk_ts = pd.Timestamp(df["date"].iloc[-1])
        wk_close = float(df["close"].iloc[-1])
        if not wk_close or pd.isna(wk_ts):
            return df
        dd = _fetch_daily(ticker)
        if dd.empty or not pd.api.types.is_datetime64_any_dtype(dd.index):
            return df
        # Only trust the daily OVER the weekly if the daily actually reaches that
        # week's FRIDAY (the weekly bar's close is the week's LAST trading day, not
        # its Monday label). Otherwise the WEEKLY may hold the fresher Friday close
        # while the DAILY is the stale side (Yahoo's daily feed missing Friday), and
        # correcting toward the daily would WRONGLY move a good weekly close down.
        # (PGIL 08-Aug-2026: daily missing Fri sat at ₹2221; the real Fri close was
        # ₹2464 in the weekly bar — the old `< wk_ts` (Monday) guard over-corrected
        # it ~10%, knocking ₹2L off Lakshmi's book. Verified vs TickerTape/ICICI.)
        wk_friday = wk_ts + pd.Timedelta(days=(4 - wk_ts.weekday()))
        if dd.index.max() < wk_friday:
            return df
        wk_iso = wk_ts.isocalendar()
        ic = dd.index.isocalendar()
        mask = (ic["year"] == wk_iso[0]) & (ic["week"] == wk_iso[1])
        same = dd[mask.values]
        if same.empty:
            return df
        true_close = float(same["close"].iloc[-1])
        if true_close and abs(true_close - wk_close) / wk_close > 0.005:
            out = df.copy()
            out.iloc[-1, out.columns.get_loc("close")] = true_close
            # keep the bar internally consistent (close must sit within high/low)
            if "high" in out.columns:
                out.iloc[-1, out.columns.get_loc("high")] = max(float(out["high"].iloc[-1]), true_close)
            if "low" in out.columns:
                out.iloc[-1, out.columns.get_loc("low")] = min(float(out["low"].iloc[-1]), true_close)
            print(f"  [signals] {ticker}: stale weekly close reconciled "
                  f"{wk_close:.2f} -> {true_close:.2f} (House Rule #7)")
            return out
    except Exception as e:
        print(f"  [signals] {ticker}: weekly reconcile skipped ({e})")
    return df


def current_state(ticker: str) -> dict:
    """Fetch weekly data for one ticker and return the latest state + detail."""
    df = fetch_weekly(ticker)
    if df.empty or len(df) < MIN_WEEKS_REQUIRED:
        # Still return the latest CLOSE even without enough history for the flowchart
        # — a young listing (INSUFFICIENT DATA) has a real market price, and consumers
        # that VALUE holdings (the digest) must use it, not fall back to cost. Without
        # this the digest priced e.g. Advent Hotels at cost ₹197 while its market price
        # was ₹147 — hiding a 25% loss and overstating the book vs the dashboard
        # (Abinaya, 08-Aug-2026). None only when there are truly no bars at all.
        return {"state": "NO DATA" if df.empty else "INSUFFICIENT DATA",
                "reason": "Could not fetch weekly data" if df.empty
                          else f"Only {len(df)} weeks of history (need {MIN_WEEKS_REQUIRED}+)",
                "close": (float(df["close"].iloc[-1]) if not df.empty else None),
                "ticker": ticker}
    df = _reconcile_last_week(ticker, df)
    ind = compute_indicators(df)
    prev = ind.iloc[-2] if len(ind) >= 2 else None
    d = classify_row(ind.iloc[-1], prev_row=prev)
    d["ticker"] = ticker
    d["as_of"] = str(ind["date"].iloc[-1].date()) if hasattr(ind["date"].iloc[-1], "date") else str(ind["date"].iloc[-1])
    return d


@_cache(ttl=60 * 60 * 4)
def _state_display(d: dict) -> str:
    """The on-screen state, with the WHY attached when there isn't one yet.

    A bare "NO DATA" cell can't tell you whether something is broken, still
    building, or simply unavailable — so you can't know whether to act. Both
    non-states now say which they are, and the building case shows its progress
    so you can see it is counting up rather than stuck (Vishal, 02-Sep-2026)."""
    state = d.get("state", "")
    if state == "INSUFFICIENT DATA":
        m = re.search(r"Only (\d+) weeks", d.get("reason", "") or "")
        if m:
            have = int(m.group(1))
            left = max(MIN_WEEKS_REQUIRED - have, 0)
            return (f"⏳ BUILDING {have}/{MIN_WEEKS_REQUIRED}w "
                    f"(~{left}w to go)")
        return f"⏳ BUILDING (needs {MIN_WEEKS_REQUIRED}w)"
    if state == "NO DATA":
        return "⚠️ NO PRICE DATA — fetch failed"
    return STATE_EMOJI.get(state, state)


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
            "State Display": _state_display(d),
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
PEAK_LOOKBACK = 126  # ~6 trading months — the window for the trailing-stop "recent peak"


def _fetch_daily(ticker: str, lookback: int = 260) -> pd.DataFrame:
    """Recent daily close+low for entry-tranche math. Source order mirrors
    fetch_weekly (house rule #1 — own the data for SME): our bhavcopy table
    FIRST (authoritative + now split/bonus-adjusted on read), Yahoo only for
    mainboard names it doesn't track. Empty df on failure. Returns a plain
    3-column frame {close, high, low}."""
    _bhav_fallback = pd.DataFrame()      # short bhavcopy history, used if Yahoo has none
    # 1) bhavcopy (SME / Emerge / BSE-only). Was the silent gap: the old
    #    Yahoo-only path returned nothing for these, so their watchlist/holding
    #    entry zones never computed at all.
    try:
        import db
        d = db.get_sme_daily_prices((ticker,))
        if not d.empty and "close" in d.columns and "low" in d.columns:
            # A NON-EMPTY table isn't automatically a USABLE one (house rule #7):
            # a newly-tracked ticker has only a handful of rows until the backfill
            # runs, and those few bars would produce junk EMAs/peaks. Require real
            # history before letting bhavcopy win; otherwise fall through to Yahoo
            # (whose DAILY bars are fine — it's the live quote that lies).
            if len(d) >= MIN_BHAV_DAILY_ROWS:
                d = d.sort_values("price_date").tail(lookback)
                hi = d["high"] if "high" in d.columns else d["close"]
                return pd.DataFrame({"close": d["close"].astype(float).to_numpy(),
                                     "high": hi.astype(float).to_numpy(),
                                     "low": d["low"].astype(float).to_numpy()},
                                    index=pd.to_datetime(d["price_date"]))
            print(f"  [signals] {ticker}: only {len(d)} bhavcopy day(s) "
                  f"(<{MIN_BHAV_DAILY_ROWS}) — trying Yahoo for deeper history")
            _short = d.sort_values("price_date").tail(lookback)
            _hi = _short["high"] if "high" in _short.columns else _short["close"]
            _bhav_fallback = pd.DataFrame(
                {"close": _short["close"].astype(float).to_numpy(),
                 "high": _hi.astype(float).to_numpy(),
                 "low": _short["low"].astype(float).to_numpy()},
                index=pd.to_datetime(_short["price_date"]))
    except Exception:
        pass
    # 2) Yahoo (mainboard NSE names)
    try:
        # 1y+ of history so the 52-week high really spans 52 weeks. Was "6mo",
        # which silently made "52W High" only a 6-MONTH high (Lakshmi caught it).
        df = yf.download(ticker, period="2y", interval="1d",
                         progress=False, auto_adjust=False)
        if df is None or df.empty:
            # Prefer our own short history over nothing — see fetch_weekly.
            return _bhav_fallback

        def _col(name):
            s = df[name]
            if hasattr(s, "columns"):        # multi-index from yf
                s = s.iloc[:, 0]
            return s.astype(float)
        # dropna across OHLC TOGETHER so the columns stay row-aligned; KEEP the
        # DatetimeIndex so the 52-week high can span a true calendar year.
        return pd.DataFrame({"close": _col("Close"), "high": _col("High"),
                             "low": _col("Low")}).dropna()
    except Exception:
        return pd.DataFrame()


MINOR_SUPPORT_DAYS = 25    # ~5 weeks of daily bars = the near-term shelf

# How close to a support level counts as "there". Lives HERE rather than in
# alerts.py because the dashboard shows the same number in its help text, and
# app.py must not import the alert engine (that would pull the whole engine into
# the 512 MB free-tier web service). Both sides read this one definition.
SUPPORT_NEAR_PCT = 2.0


def support_levels(ticker: str) -> dict | None:
    """MINOR and MAJOR support for one ticker (Lakshmi 15-Aug-2026 — he buys at
    support, so he wants pinging as price approaches one).

      minor = lowest LOW over the last MINOR_SUPPORT_DAYS daily bars — the
              near-term shelf price has recently bounced off.
      major = the WEEKLY swing-pivot support the flowchart already computes
              (compute_indicators' 'support', a 5-week CENTERED pivot low) —
              the structural level, far more meaningful than a daily wiggle.

    Deliberately two different timeframes: a daily low and a weekly pivot answer
    different questions ("did it just bounce here?" vs "is this the floor?").
    Returns None when neither is computable — a missing level must never be
    silently treated as 0 (rule #2)."""
    out = {}
    try:
        d = _fetch_daily(ticker)
        if d is not None and not d.empty and "low" in d.columns and len(d) >= 10:
            lo = float(d["low"].tail(MINOR_SUPPORT_DAYS).min())
            if lo > 0:
                out["minor"] = lo
    except Exception:
        pass
    try:
        wk = fetch_weekly(ticker)
        if wk is not None and not wk.empty and len(wk) >= MIN_WEEKS_REQUIRED:
            sup = compute_indicators(wk)["support"].iloc[-1]
            if pd.notna(sup) and float(sup) > 0:
                out["major"] = float(sup)
    except Exception:
        pass
    return out or None


def daily_entry_levels(ticker: str) -> dict | None:
    """The 10/21-day EMA LEVELS off daily bars (bhavcopy-first). None if data
    unavailable. These levels only change once a day (built on completed daily
    closes), so the fast intraday poller computes them ONCE and reuses them for
    many cheap live-price checks — never re-downloading history each minute
    (that per-minute history fetch would be exactly the Yahoo-storm that crashed
    Render before, house rule #4). Also returns the last daily close/low so the
    EOD path can classify without a second fetch."""
    d = _fetch_daily(ticker)
    if d.empty or len(d) < 30:
        return None
    close = d["close"]
    low = d["low"]
    high = d["high"] if "high" in d.columns else close
    # 52-week high = highest INTRADAY high over a true rolling CALENDAR year.
    # GUARD (house rule #7): a "52-week" high needs ~a year of history. With
    # less — young listing, or a partially-backfilled bhavcopy ticker (only
    # MIN_BHAV_DAILY_ROWS=60 rows are enough to WIN the source) — return None so
    # the cell renders "—" instead of a short-window max mislabelled 52-week
    # (house rule #2). Without this the window silently differs per row.
    high_52w = None
    try:
        idx = high.index
        if pd.api.types.is_datetime64_any_dtype(idx):
            if (idx.max() - idx.min()).days >= 340:      # ~a full year present
                _recent = high[idx >= idx.max() - pd.Timedelta(days=365)]
                high_52w = float(_recent.max()) if len(_recent) else None
        elif len(high) >= 240:                            # dateless fallback
            high_52w = float(high.tail(252).max())
    except Exception:
        high_52w = None
    return {
        "ema5": float(close.ewm(span=5, adjust=False).mean().iloc[-1]),
        "ema10": float(close.ewm(span=10, adjust=False).mean().iloc[-1]),
        "ema21": float(close.ewm(span=21, adjust=False).mean().iloc[-1]),
        "ref_close": float(close.iloc[-1]),
        "ref_low": float(low.iloc[-1]),
        # yesterday's close — lets a "touch" be detected as an EVENT (came down
        # TO a level) rather than a standing condition (loitering near it).
        "prev_close": float(close.iloc[-2]) if len(close) >= 2 else None,
        # recent peak for the trailing-stop alert — highest close over ~6 months,
        # computed here so the fast poller reuses this one daily fetch (no extra
        # per-minute history download).
        "peak": float(close.tail(PEAK_LOOKBACK).max()),
        # 52-week high — true INTRADAY high over a rolling calendar year (see
        # above). Was close-based over only ~6mo of data; both bugs understated
        # it (up to ~20%) until Lakshmi flagged it 27-Jul-2026.
        "high_52w": high_52w,
    }


def weekly_ema10(ticker: str) -> float | None:
    """Current 10-WEEK EMA — the trend line Lakshmi's weekly system hangs on
    (a touch of it is an act-on level, distinct from the 10/21-DAY entry EMAs).
    Built from the SAME weekly bars the flowchart uses, so it's bhavcopy-first
    for SME. None if there isn't enough history. Computed ONCE per poller launch
    and reused for many live-price comparisons (never per-minute)."""
    df = fetch_weekly(ticker)
    if df.empty or len(df) < 12 or "close" not in df.columns:
        return None
    try:
        return float(df["close"].ewm(span=10, adjust=False).mean().iloc[-1])
    except Exception:
        return None


def classify_entry_zone(ticker: str, cmp_: float, day_low: float,
                        ema10: float, ema21: float) -> dict:
    """PURE classification (no fetch): where does price `cmp_` sit vs the 10/21-
    day EMAs? Shared by the EOD path (cmp_ = last daily close) and the fast
    intraday poller (cmp_ = live quote) so both produce identical zones/wording."""
    def touching(ema):
        # intraday low pierced the EMA, or price sits within the band
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


def daily_entry_state(ticker: str) -> dict | None:
    """Entry-tranche status off the latest DAILY bar (EOD / hourly path). None if
    data unavailable. Kept identical to before — now built from the split
    levels + classify helpers so the fast poller can share the exact logic."""
    lv = daily_entry_levels(ticker)
    if not lv:
        return None
    d = classify_entry_zone(ticker, lv["ref_close"], lv["ref_low"],
                            lv["ema10"], lv["ema21"])
    e5 = lv.get("ema5")
    d["% vs 5DMA"] = round((lv["ref_close"] / e5 - 1) * 100, 1) if e5 else None
    hi = lv.get("high_52w")
    d["52W High"] = round(hi, 2) if hi else None
    d["% vs 52WH"] = round((lv["ref_close"] / hi - 1) * 100, 1) if hi else None
    return d


def entry_states_for_watchlist(tickers: tuple) -> "pd.DataFrame":
    _cols = ["Ticker", "CMP (d)", "10DMA", "21DMA", "% vs 5DMA", "% vs 10DMA", "% vs 21DMA",
             "Entry Zone", "Entry Advice", "52W High", "% vs 52WH"]
    if not tickers:
        return pd.DataFrame(columns=_cols)
    rows = [r for t in tickers if (r := daily_entry_state(t))]
    return pd.DataFrame(rows, columns=_cols) if rows else pd.DataFrame(columns=_cols)
