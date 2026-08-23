"""
charges.py — estimated transaction costs on Indian equity DELIVERY trades.

WHY THIS EXISTS (Vishal, 22-Aug-2026)
"XIRR should be calculated after deducting STT, brokerage and other charges but
it should be pre-tax." We store no charges — neither `realised` nor
`transactions` has a cost column — so the dashboard's XIRR has always been
gross.

The account statements are behind a broker login and out of reach, but the RATE
CARDS are public and published. So we reconstruct the charge on each transaction
from its amount, side and the broker's published schedule. That is an ESTIMATE,
and it is labelled as one everywhere it appears — but it is an estimate built
from real published rates rather than a guessed percentage, and for delivery
equity the formula is almost entirely statutory (STT, stamp duty, exchange and
SEBI fees are identical at every broker), so the uncertainty is small.

WHAT WOULD MAKE IT EXACT: import the broker's own P&L / tradebook export, which
itemises actual charges. Until then this is the honest approximation.

RATES VERIFIED 22-Aug-2026 from the brokers' own charge pages:
  zerodha.com/charges  ·  upstox.com/brokerage-charges  ·  INDmoney rate card

STATUTORY (same at every broker, equity delivery):
  STT              0.1%   on BUY and 0.1% on SELL
  Stamp duty       0.015% on BUY only
  Exchange txn     NSE 0.00307%  ·  BSE 0.00375%
  SEBI turnover    ₹10 per crore (0.0001%)
  GST              18% on (brokerage + exchange txn + SEBI)

BROKER-SPECIFIC:
  Zerodha/Kite  brokerage ₹0        DP ₹15.34 per scrip on SELL
  Upstox        brokerage ₹20/order DP ₹20.00 per scrip on SELL
  INDmoney      brokerage ₹0        DP ₹21.83 per scrip on SELL (₹18.5 + GST)

CAVEAT ON HISTORY: these are CURRENT rates applied to past trades. Statutory
rates do change (stamp duty was unified in July 2020, STT has been revised), so
older trades carry slightly more error. For a book that starts in late 2024 the
drift is immaterial.
"""
from __future__ import annotations

import pandas as pd

# Which broker each portfolio trades through (CLAUDE.md).
PF_BROKER = {1: "indmoney", 2: "zerodha", 3: "upstox"}

STT_PCT = 0.001            # 0.1% both sides, delivery
STAMP_PCT = 0.00015        # 0.015%, BUY only
SEBI_PCT = 0.000001        # ₹10 per crore
GST_RATE = 0.18
EXCHANGE_PCT = {"NSE": 0.0000307, "BSE": 0.0000375}

BROKERS = {
    "zerodha": {"name": "Zerodha / Kite", "brokerage_per_order": 0.0,
                "brokerage_pct": 0.0, "brokerage_cap": 0.0, "dp_per_sell": 15.34},
    "upstox":  {"name": "Upstox", "brokerage_per_order": 20.0,
                "brokerage_pct": 0.0, "brokerage_cap": 20.0, "dp_per_sell": 20.00},
    "indmoney": {"name": "INDmoney", "brokerage_per_order": 0.0,
                 "brokerage_pct": 0.0, "brokerage_cap": 0.0, "dp_per_sell": 21.83},
}


def estimate(amount: float, side: str, broker: str = "zerodha",
             exchange: str = "NSE") -> dict:
    """Estimated charges on ONE delivery transaction. Returns a breakdown in ₹.

    `side` is 'buy' or 'sell'. Charges differ by side: stamp duty is buy-only,
    DP charges are sell-only, and that asymmetry is why a round trip is not
    simply twice the one-way cost."""
    try:
        amount = abs(float(amount))
    except (TypeError, ValueError):
        return {}
    if amount <= 0:
        return {}
    side = str(side).lower().strip()
    b = BROKERS.get(str(broker).lower(), BROKERS["zerodha"])
    ex = EXCHANGE_PCT.get(str(exchange).upper(), EXCHANGE_PCT["NSE"])

    brokerage = min(b["brokerage_per_order"] or amount * b["brokerage_pct"],
                    b["brokerage_cap"]) if b["brokerage_cap"] else \
        (b["brokerage_per_order"] or amount * b["brokerage_pct"])
    brokerage = min(brokerage, amount)          # never exceed the trade value

    stt = amount * STT_PCT                       # both sides on delivery
    stamp = amount * STAMP_PCT if side == "buy" else 0.0
    txn = amount * ex
    sebi = amount * SEBI_PCT
    gst = (brokerage + txn + sebi) * GST_RATE
    dp = b["dp_per_sell"] if side == "sell" else 0.0

    total = brokerage + stt + stamp + txn + sebi + gst + dp
    return {"brokerage": brokerage, "stt": stt, "stamp": stamp, "exchange": txn,
            "sebi": sebi, "gst": gst, "dp": dp, "total": total,
            "pct_of_amount": total / amount * 100}


def _exchange_of(stock_name: str) -> str:
    return "BSE" if "XBOM" in str(stock_name).upper() else "NSE"


def apply_to_transactions(tx: pd.DataFrame, portfolio_id: int) -> pd.DataFrame:
    """Add a `charges` column to a transactions frame. Never raises — a frame
    without the columns we need comes back with charges 0.0, so the caller's
    XIRR still computes (gross) instead of the page erroring."""
    if tx is None or tx.empty:
        return tx
    d = tx.copy()
    broker = PF_BROKER.get(int(portfolio_id), "zerodha")
    if not {"amount", "transaction_type"} <= set(d.columns):
        d["charges"] = 0.0
        return d
    d["charges"] = [
        estimate(r.get("amount"), r.get("transaction_type"), broker,
                 _exchange_of(r.get("stock_name", ""))).get("total", 0.0)
        for _, r in d.iterrows()
    ]
    return d


def summary(tx: pd.DataFrame, portfolio_id: int) -> dict:
    """Total estimated charges paid across all logged transactions."""
    d = apply_to_transactions(tx, portfolio_id)
    if d is None or d.empty or "charges" not in d.columns:
        return {}
    turnover = pd.to_numeric(d.get("amount"), errors="coerce").abs().sum()
    total = float(d["charges"].sum())
    return {
        "broker": BROKERS[PF_BROKER.get(int(portfolio_id), "zerodha")]["name"],
        "n_transactions": int(len(d)),
        "total_charges": total,
        "turnover": float(turnover or 0),
        "pct_of_turnover": (total / turnover * 100) if turnover else None,
    }


if __name__ == "__main__":
    for br in ("zerodha", "upstox", "indmoney"):
        buy = estimate(100000, "buy", br)
        sell = estimate(110000, "sell", br)
        print(f"{BROKERS[br]['name']:16} buy Rs1,00,000 -> Rs {buy['total']:7.2f} "
              f"({buy['pct_of_amount']:.3f}%)   sell Rs1,10,000 -> Rs {sell['total']:7.2f} "
              f"({sell['pct_of_amount']:.3f}%)   round trip Rs {buy['total']+sell['total']:.2f}")
