"""
xirr.py — live annualized return (XIRR) from the transactions log.

Same math validated against INDmoney reports (June 2026): brentq root-finding
on the XNPV equation. Cash flows: buys negative, sells positive, plus current
portfolio value as the terminal inflow.
"""

from __future__ import annotations
from datetime import date
import pandas as pd
import streamlit as st

try:
    from scipy.optimize import brentq
    HAVE_SCIPY = True
except ImportError:
    HAVE_SCIPY = False


def _xnpv(rate: float, flows: list) -> float:
    d0 = min(f[0] for f in flows)
    return sum(cf / (1 + rate) ** ((d - d0).days / 365.0) for d, cf in flows)


def _bisect_xirr(flows, lo=-0.99, hi=10.0, tol=1e-7, max_iter=200):
    """Fallback bisection if scipy unavailable."""
    f_lo, f_hi = _xnpv(lo, flows), _xnpv(hi, flows)
    if f_lo * f_hi > 0:
        return None
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        f_mid = _xnpv(mid, flows)
        if abs(f_mid) < tol:
            return mid
        if f_lo * f_mid < 0:
            hi = mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2


def compute_xirr(transactions: pd.DataFrame, current_value: float) -> dict:
    """XIRR from the transactions table + live portfolio value.

    transactions needs: transaction_type ('buy'/'sell'), amount, transaction_date.
    Returns dict with xirr (fraction, e.g. 0.2991), n_flows, first_date; or
    xirr=None with a reason if it can't be computed.
    """
    if transactions is None or transactions.empty:
        return {"xirr": None, "reason": "No transactions logged yet"}
    if not current_value or current_value <= 0:
        return {"xirr": None, "reason": "Portfolio value unavailable"}

    df = transactions.copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df = df.dropna(subset=["transaction_date", "amount"])
    if df.empty:
        return {"xirr": None, "reason": "No dated transactions"}

    flows = []
    for _, r in df.iterrows():
        amt = float(r["amount"])
        sign = -1 if str(r["transaction_type"]).lower() == "buy" else 1
        flows.append((r["transaction_date"].date(), sign * amt))
    flows.append((date.today(), float(current_value)))

    # Need at least one negative (investment) and the terminal positive
    if not any(cf < 0 for _, cf in flows):
        return {"xirr": None, "reason": "No buy transactions found"}

    try:
        if HAVE_SCIPY:
            rate = brentq(lambda r: _xnpv(r, flows), -0.99, 10.0, maxiter=1000)
        else:
            rate = _bisect_xirr(flows)
        if rate is None:
            return {"xirr": None, "reason": "Root-finding failed"}
        return {
            "xirr": rate,
            "n_flows": len(flows),
            "first_date": min(f[0] for f in flows),
        }
    except Exception as e:
        return {"xirr": None, "reason": f"Computation failed: {e}"}
