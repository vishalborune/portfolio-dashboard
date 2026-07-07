"""
backtest.py — classify any stock's real weekly history through Lakshmi's
flowchart and produce the state-strip chart.

Run LOCALLY (needs internet for yfinance):
    pip install yfinance pandas matplotlib
    python backtest.py KWALITY.BO
    python backtest.py WELCORP.NS --period 3y

Outputs:
    <ticker>_states.csv   — week-by-week classification with reasons
    <ticker>_states.png   — price + EMAs + colour-coded state strip
"""

import sys
import argparse

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import yfinance as yf

# --- Same locked definitions as signals.py (kept standalone so this script
#     runs without streamlit installed) ---
CONVERGENCE_BAND_PCT = 2.0
SWING_WINDOW = 5
EXIT_HARD_BREAK_PCT = 3.0   # option (a): single close 3%+ below 40W = confirmed exit

STATE_COLORS = {
    "INSUFFICIENT DATA": "#cbd5e1",
    "EXIT": "#dc2626",
    "BULLISH SIGNAL": "#16a34a",
    "WAIT/WATCH": "#0891b2",
    "BE CAUTIOUS": "#f59e0b",
    "MOMENTUM FADING": "#a855f7",
    "MAINTAIN/ADD": "#22c55e",
}


def fetch_weekly(ticker: str, period: str) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval="1wk",
                     progress=False, auto_adjust=False)
    if df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index().rename(columns={"Date": "date", "Close": "close"})
    return df.dropna(subset=["close"]).reset_index(drop=True)


def compute(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ema10"] = out["close"].ewm(span=10, adjust=False).mean()
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema40"] = out["close"].ewm(span=40, adjust=False).mean()
    stack = out[["ema10", "ema20", "ema40"]]
    out["ema_spread_pct"] = (stack.max(axis=1) - stack.min(axis=1)) / stack.mean(axis=1) * 100
    out["converging"] = out["ema_spread_pct"] <= CONVERGENCE_BAND_PCT

    w = SWING_WINDOW
    roll_min = out["close"].rolling(w, center=True).min()
    roll_max = out["close"].rolling(w, center=True).max()
    sup, res = [], []
    cur_s, cur_r = np.nan, np.nan
    for i in range(len(out)):
        if out["close"].iloc[i] == roll_min.iloc[i] and pd.notna(roll_min.iloc[i]):
            cur_s = out["close"].iloc[i]
        if out["close"].iloc[i] == roll_max.iloc[i] and pd.notna(roll_max.iloc[i]):
            cur_r = out["close"].iloc[i]
        sup.append(cur_s)
        res.append(cur_r)
    out["support"], out["resistance"] = sup, res

    def classify(row, prev):
        if pd.isna(row["support"]) or pd.isna(row["resistance"]):
            return "INSUFFICIENT DATA"
        if row["converging"]:
            if row["close"] < row["support"]:
                return "EXIT"
            if row["close"] > row["resistance"]:
                return "BULLISH SIGNAL"
            return "WAIT/WATCH"
        # Trending branch with option-(a) confirmation buffer:
        below_40 = row["close"] < row["ema40"]
        hard_break = row["close"] < row["ema40"] * (1 - EXIT_HARD_BREAK_PCT / 100)
        prev_below_40 = prev is not None and prev["close"] < prev["ema40"]
        if below_40 and (hard_break or prev_below_40):
            return "EXIT"
        if below_40:
            return "BE CAUTIOUS"   # first mild close below 40W = warning week
        if row["close"] < row["ema20"]:
            return "BE CAUTIOUS"
        if row["close"] < row["ema10"]:
            return "MOMENTUM FADING"
        return "MAINTAIN/ADD"

    states, prev = [], None
    for _, row in out.iterrows():
        states.append(classify(row, prev))
        prev = row
    out["state"] = states
    return out


def plot(df: pd.DataFrame, ticker: str, outfile: str):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8.5), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1]})
    fig.patch.set_facecolor("white")

    ax1.plot(df["date"], df["close"], color="#1e3a8a", lw=1.8, label="Close", zorder=3)
    ax1.plot(df["date"], df["ema10"], color="#f59e0b", lw=1.0, label="10-wk EMA", alpha=0.85)
    ax1.plot(df["date"], df["ema20"], color="#0ea5e9", lw=1.0, label="20-wk EMA", alpha=0.85)
    ax1.plot(df["date"], df["ema40"], color="#dc2626", lw=1.2, label="40-wk EMA", alpha=0.8)
    ax1.set_title(f"{ticker} — real weekly bars classified through the flowchart",
                   fontsize=12, fontweight="bold", loc="left")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.set_ylabel("Price (₹)")
    ax1.grid(alpha=0.2)
    for s in ("top", "right"):
        ax1.spines[s].set_visible(False)

    for i in range(len(df) - 1):
        c = STATE_COLORS.get(df["state"].iloc[i], "#e5e7eb")
        ax2.axvspan(df["date"].iloc[i], df["date"].iloc[i + 1], color=c, alpha=0.85)
    ax2.set_yticks([])
    ax2.set_ylabel("State")
    for s in ("top", "right", "left"):
        ax2.spines[s].set_visible(False)
    handles = [mpatches.Patch(color=v, label=k)
               for k, v in STATE_COLORS.items() if k in df["state"].unique()]
    ax2.legend(handles=handles, loc="upper center",
               bbox_to_anchor=(0.5, -0.35), ncol=3, fontsize=8)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.tight_layout()
    plt.savefig(outfile, dpi=160, bbox_inches="tight", facecolor="white")
    print(f"✓ Chart: {outfile}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker", help="yfinance ticker, e.g. KWALITY.BO or WELCORP.NS")
    ap.add_argument("--period", default="3y", help="history window (default 3y)")
    args = ap.parse_args()

    print(f"Fetching {args.ticker} weekly bars ({args.period})...")
    df = fetch_weekly(args.ticker, args.period)
    if df.empty:
        print("❌ No data returned. Check the ticker symbol.")
        sys.exit(1)
    print(f"✓ {len(df)} weekly bars, {df['date'].iloc[0].date()} → {df['date'].iloc[-1].date()}")

    result = compute(df)
    base = args.ticker.replace(".", "_")
    csv_out = f"{base}_states.csv"
    result.to_csv(csv_out, index=False)
    print(f"✓ CSV: {csv_out}")

    print("\nState distribution:")
    print(result["state"].value_counts().to_string())
    print(f"\nCurrent state: {result['state'].iloc[-1]}")

    plot(result, args.ticker, f"{base}_states.png")


if __name__ == "__main__":
    main()
