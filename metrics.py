"""
metrics.py — trading-performance metrics ("know yourself if you want to improve").

WHY THIS EXISTS (Vishal, 22-Aug-2026)
Everything here is computed from CLOSED trades in the `realised` table, the
`trade_journal` exit audits, and the weekly `digest_history` snapshots. Nothing
is estimated and nothing is asked of a model.

It lives in its own module so the dashboard AND the weekly digest read the SAME
numbers. Two code paths answering one money question is exactly how the digest
and dashboard drifted apart twice (08-Aug-2026).

THE ONE DATA GOTCHA: `realised.pct_gain_loss` is stored as a FRACTION
(0.090 = 9.05%), not a percentage. The dashboard's "{:+.2%}" format multiplies
by 100 so it renders correctly there, but any code doing plain arithmetic on the
column is 100x out. We therefore recompute the % from gain_loss / amount_invested
rather than trusting the stored column at all.
"""
from __future__ import annotations

import pandas as pd

# Synthetic rows (e.g. the FY opening-balance entry) are NOT trades and must be
# excluded from every trade statistic — one Rs 9.1L "opening balance" row would
# otherwise show up as the best trade ever made.
NON_TRADE_MARKERS = ("opening balance",)


def _clean(realised: pd.DataFrame) -> pd.DataFrame:
    if realised is None or realised.empty:
        return pd.DataFrame()
    d = realised.copy()
    for col in ("gain_loss", "amount_invested", "no_of_days"):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
    if "stock_name" in d.columns:
        pat = "|".join(NON_TRADE_MARKERS)
        d = d[~d["stock_name"].astype(str).str.contains(pat, case=False, na=False)]
    d = d.dropna(subset=["gain_loss"])
    # Recompute the % ourselves — see the module docstring.
    if "amount_invested" in d.columns:
        inv = d["amount_invested"].where(d["amount_invested"] > 0)
        d["pct"] = d["gain_loss"] / inv * 100
    else:
        d["pct"] = pd.NA
    return d


def trade_stats(realised: pd.DataFrame) -> dict:
    """Win rate, payoff, profit factor, expectancy and the discipline read.

    `payoff` is avg win ÷ avg loss in RUPEES. Read it together with win rate:
    a 29% win rate with a 4.3:1 payoff is a healthy trend-following book, while
    the same win rate at 1:1 is a losing one. Neither number means much alone —
    which is precisely why they are shown side by side."""
    d = _clean(realised)
    if d.empty:
        return {}
    wins = d[d["gain_loss"] > 0]
    losses = d[d["gain_loss"] <= 0]
    n = len(d)
    gross_p = float(wins["gain_loss"].sum())
    gross_l = float(abs(losses["gain_loss"].sum()))
    avg_w = float(wins["gain_loss"].mean()) if len(wins) else 0.0
    avg_l = float(losses["gain_loss"].mean()) if len(losses) else 0.0
    win_rate = len(wins) / n if n else 0.0

    def _mean(frame, col):
        v = frame[col].dropna() if col in frame.columns else pd.Series(dtype=float)
        return float(v.mean()) if len(v) else None

    return {
        "n_trades": n,
        "n_wins": len(wins),
        "n_losses": len(losses),
        "win_rate": win_rate * 100,
        "avg_win_rs": avg_w,
        "avg_loss_rs": avg_l,
        "avg_win_pct": _mean(wins, "pct"),
        "avg_loss_pct": _mean(losses, "pct"),
        # None (not 0) when there are no losses yet — "no losses" is unmeasurable,
        # not infinitely good, and a 0 here would render as a terrible payoff.
        "payoff": (abs(avg_w / avg_l) if avg_l else None),
        "profit_factor": (gross_p / gross_l if gross_l else None),
        "expectancy_rs": win_rate * avg_w + (1 - win_rate) * avg_l,
        "gross_profit": gross_p,
        "gross_loss": gross_l,
        "net_rs": gross_p - gross_l,
        "avg_hold_win": _mean(wins, "no_of_days"),
        "avg_hold_loss": _mean(losses, "no_of_days"),
        "best_rs": float(d["gain_loss"].max()),
        "worst_rs": float(d["gain_loss"].min()),
        "best_pct": (float(d["pct"].max()) if d["pct"].notna().any() else None),
        "worst_pct": (float(d["pct"].min()) if d["pct"].notna().any() else None),
    }


def discipline_note(st: dict) -> str:
    """One plain sentence on what the numbers say about behaviour. The metric
    that matters most is holding winners LONGER than losers — the opposite is
    the classic retail failure (cut the winner, marry the loser)."""
    if not st:
        return ""
    w, l = st.get("avg_hold_win"), st.get("avg_hold_loss")
    if w is None or l is None:
        return ""
    if w > l * 1.3:
        return (f"Winners are held {w:.0f} days vs {l:.0f} for losers — "
                f"letting winners run and cutting losers early. This is the "
                f"behaviour that makes a sub-50% win rate profitable.")
    if l > w * 1.3:
        return (f"⚠️ Losers are held {l:.0f} days vs {w:.0f} for winners — "
                f"the classic trap: selling winners early and holding losers "
                f"hoping they recover.")
    return (f"Winners and losers are held about equally ({w:.0f}d vs {l:.0f}d) — "
            f"no strong evidence either way yet.")


def exit_quality(journal: pd.DataFrame) -> dict:
    """Was selling the right call? Uses exit_audit's 30/60/90-day post-exit prices.

    A sale is 'right' when the stock was LOWER afterwards (you avoided a fall).
    This is the only metric here that scores DECISIONS rather than outcomes, and
    it is the reason exit_audit exists."""
    if journal is None or journal.empty:
        return {}
    out = {}
    for win in (30, 60, 90):
        col = f"price_{win}d"
        if col not in journal.columns or "exit_price" not in journal.columns:
            continue
        d = journal.dropna(subset=[col, "exit_price"]).copy()
        d[col] = pd.to_numeric(d[col], errors="coerce")
        d["exit_price"] = pd.to_numeric(d["exit_price"], errors="coerce")
        d = d.dropna(subset=[col, "exit_price"])
        d = d[d["exit_price"] > 0]
        if d.empty:
            continue
        chg = (d[col] - d["exit_price"]) / d["exit_price"] * 100
        out[win] = {
            "n": int(len(d)),
            "saved": int((chg < 0).sum()),      # price fell after we sold — good call
            "cost": int((chg >= 0).sum()),      # price rose after we sold — early
            "avg_move_pct": float(chg.mean()),
        }
    return out


def drawdown(snapshots: pd.DataFrame) -> dict:
    """Peak-to-trough decline of PORTFOLIO VALUE from the weekly snapshots.

    'Drawdown' = how far the book has fallen from its own highest point. The
    max drawdown is the worst such fall on record — the honest answer to "what
    is the biggest paper loss I have actually lived through?".

    NOTE this measures VALUE, which moves when money is added or withdrawn too.
    With only weekly snapshots there is no way to strip cashflows out, so a big
    deposit can look like a recovery. Reported as-is and labelled, rather than
    silently presented as a pure return series."""
    if snapshots is None or snapshots.empty or "current_value" not in snapshots.columns:
        return {}
    d = snapshots.copy()
    d["snap_date"] = pd.to_datetime(d["snap_date"], errors="coerce")
    d["current_value"] = pd.to_numeric(d["current_value"], errors="coerce")
    d = d.dropna(subset=["snap_date", "current_value"]).sort_values("snap_date")
    if len(d) < 2:
        return {"n_points": int(len(d))}
    running_peak = d["current_value"].cummax()
    dd = (d["current_value"] / running_peak - 1) * 100
    i = int(dd.values.argmin())
    return {
        "n_points": int(len(d)),
        "from_date": d["snap_date"].iloc[0].date(),
        "current_dd_pct": float(dd.iloc[-1]),
        "peak_value": float(running_peak.iloc[-1]),
        "max_dd_pct": float(dd.min()),
        "max_dd_date": d["snap_date"].iloc[i].date(),
    }


def holding_drawdowns(entry_zones: pd.DataFrame) -> pd.DataFrame:
    """Per-stock decline from each holding's own ~6-month peak.

    Available TODAY with no history required, because `signals.daily_entry_levels`
    already computes that peak for the trailing-stop alert. Expects a frame with
    'Ticker', 'CMP' and 'Peak' columns."""
    if entry_zones is None or entry_zones.empty:
        return pd.DataFrame()
    d = entry_zones.copy()
    if not {"CMP", "Peak"} <= set(d.columns):
        return pd.DataFrame()
    d["Off Peak %"] = (pd.to_numeric(d["CMP"], errors="coerce")
                       / pd.to_numeric(d["Peak"], errors="coerce") - 1) * 100
    return d.dropna(subset=["Off Peak %"]).sort_values("Off Peak %")
