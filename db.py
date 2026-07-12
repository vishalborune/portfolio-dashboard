"""
db.py — Supabase data layer for the portfolio dashboard.

All reads/writes go through here. Streamlit caches reads briefly so the UI
stays snappy without hammering the database.
"""

from __future__ import annotations
from datetime import date, datetime
from typing import Optional
import pandas as pd
import streamlit as st
from supabase import create_client, Client


@st.cache_resource
def _client() -> Client:
    """Single Supabase client per session."""
    # Environment variables first (Render/HF inject secrets as env vars;
    # os.environ needs no secrets.toml file). st.secrets stays as fallback
    # for Streamlit Cloud / local runs with a secrets file.
    import os
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def _bust():
    """Invalidate all cached reads after a write."""
    st.cache_data.clear()


def _iso(d):
    if d is None or pd.isna(d):
        return None
    if isinstance(d, str):
        return d
    if hasattr(d, "isoformat"):
        return d.isoformat()
    return str(d)


# ===============================================================
# HOLDINGS
# ===============================================================

def _active_pf() -> int:
    """The portfolio currently selected in the UI (set by app.py at login/switch)."""
    try:
        import streamlit as st
        return int(st.session_state.get("portfolio_id", 1))
    except Exception:
        return 1


def get_holdings() -> pd.DataFrame:
    res = _client().table("holdings").select("*").eq("portfolio_id", _active_pf()).order("created_at").execute()
    df = pd.DataFrame(res.data or [])
    return df


def add_holding(stock_name: str, quantity: float, purchase_cost: float,
                amount_invested: Optional[float] = None,
                buy_date: Optional[date] = None,
                notes: Optional[str] = None):
    """Add a brand-new holding and log the buy transaction."""
    if amount_invested is None:
        amount_invested = round(float(quantity) * float(purchase_cost), 2)
    payload = {
        "stock_name": stock_name.strip(),
        "quantity": float(quantity),
        "purchase_cost": float(purchase_cost),
        "amount_invested": float(amount_invested),
        "buy_date": _iso(buy_date),
        "notes": notes,
    }
    payload["portfolio_id"] = _active_pf()
    res = _client().table("holdings").insert(payload).execute()
    new_id = res.data[0]["id"] if res.data else None

    # Log the buy transaction
    _insert_transaction(
        stock_name=stock_name.strip(),
        transaction_type="buy",
        quantity=float(quantity),
        price=float(purchase_cost),
        amount=float(amount_invested),
        transaction_date=buy_date or date.today(),
        notes=notes,
        holding_id=new_id,
    )
    _bust()
    return new_id


def update_holding(holding_id: int, **kwargs):
    payload = {k: v for k, v in kwargs.items() if v is not None}
    if "buy_date" in payload:
        payload["buy_date"] = _iso(payload["buy_date"])
    if "quantity" in payload:
        payload["quantity"] = float(payload["quantity"])
    if "purchase_cost" in payload:
        payload["purchase_cost"] = float(payload["purchase_cost"])
    payload["updated_at"] = datetime.utcnow().isoformat()
    _client().table("holdings").update(payload).eq("id", holding_id).execute()
    _bust()


def delete_holding(holding_id: int):
    _client().table("holdings").delete().eq("id", holding_id).execute()
    _bust()


def delete_transaction(transaction_id: int):
    """Remove a single row from the transactions journal.

    Intended for cleaning up mistaken/test entries. Real trade history
    should normally stay append-only — deleting a genuine buy will make
    XIRR less accurate, so the UI warns before doing this.
    """
    _client().table("transactions").delete().eq("id", transaction_id).execute()
    _bust()


# ===============================================================
# REALISED P&L  (closed trades)
# ===============================================================

@st.cache_data(ttl=60)
def _get_realised_cached(pf: int) -> pd.DataFrame:
    res = _client().table("realised").select("*").eq("portfolio_id", pf).order("sale_date", desc=True).execute()
    return pd.DataFrame(res.data or [])


def get_realised() -> pd.DataFrame:
    return _get_realised_cached(_active_pf())


def _clean_date(d):
    """Normalize a date-ish value to a real date or None.

    Migrated holdings have no buy_date, so it can arrive as NaN, None,
    an empty string, or the literal string 'nan'. All of those -> None.
    """
    if d is None:
        return None
    # pandas NaN / NaT check (guarded — pd.isna can choke on some types)
    try:
        if pd.isna(d):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(d, date):
        return d
    s = str(d).strip()
    if s == "" or s.lower() in ("nan", "nat", "none"):
        return None
    try:
        return datetime.fromisoformat(s).date()
    except (ValueError, TypeError):
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return None


def add_realised(stock_name: str, quantity: float, purchase_cost: float,
                 selling_price: float, sale_date: date,
                 buy_date: Optional[date] = None,
                 amount_invested: Optional[float] = None,
                 notes: Optional[str] = None):
    if amount_invested is None:
        amount_invested = round(float(quantity) * float(purchase_cost), 2)
    sale_consideration = round(float(quantity) * float(selling_price), 2)
    gain_loss = round(sale_consideration - amount_invested, 2)
    pct_gain_loss = round(gain_loss / amount_invested, 4) if amount_invested else 0

    # Normalize dates — buy_date may be missing/NaN on migrated holdings
    buy_date = _clean_date(buy_date)
    sale_date_clean = _clean_date(sale_date)

    no_of_days = None
    if buy_date and sale_date_clean:
        no_of_days = (sale_date_clean - buy_date).days
    payload = {
        "stock_name": stock_name.strip(),
        "quantity": float(quantity),
        "purchase_cost": float(purchase_cost),
        "amount_invested": float(amount_invested),
        "selling_price": float(selling_price),
        "sale_consideration": sale_consideration,
        "gain_loss": gain_loss,
        "pct_gain_loss": pct_gain_loss,
        "sale_date": _iso(sale_date),
        "buy_date": _iso(buy_date),
        "no_of_days": no_of_days,
    }
    payload["portfolio_id"] = _active_pf()
    _client().table("realised").insert(payload).execute()
    _bust()


def delete_realised(realised_id: int):
    _client().table("realised").delete().eq("id", realised_id).execute()
    _bust()


def mark_as_sold(holding_id: int, selling_price: float, sale_date: date,
                 partial_quantity: Optional[float] = None,
                 reason: Optional[str] = None, notes: Optional[str] = None):
    """Move a holding (or part of it) into the realised table.

    If partial_quantity is set and < total, the remaining quantity stays in
    holdings (so partial sells work). Also logs the sell in transactions.

    Sprint 3: if `reason` is given, the exit is also logged in trade_journal
    so the 30/60/90-day audit engine (exit_audit.py) can later measure what
    the stock did after we sold. The journal write is best-effort: a failure
    there never blocks the actual sell.
    """
    holdings = get_holdings()
    row = holdings[holdings["id"] == holding_id]
    if row.empty:
        raise ValueError(f"Holding {holding_id} not found")
    r = row.iloc[0]
    total_qty = float(r["quantity"])
    sold_qty = float(partial_quantity) if partial_quantity else total_qty
    if sold_qty > total_qty:
        raise ValueError(f"Cannot sell {sold_qty} — only {total_qty} held")

    # Insert into realised — capture the id so we can link the transaction
    invested = round(sold_qty * float(r["purchase_cost"]), 2)
    sale_amount = round(sold_qty * float(selling_price), 2)
    gain = round(sale_amount - invested, 2)
    pct = round(gain / invested, 4) if invested else 0
    buy_d = _clean_date(r.get("buy_date"))
    sale_d = _clean_date(sale_date)
    days = (sale_d - buy_d).days if (buy_d and sale_d) else None

    realised_payload = {
        "stock_name": r["stock_name"],
        "quantity": sold_qty,
        "purchase_cost": float(r["purchase_cost"]),
        "amount_invested": invested,
        "selling_price": float(selling_price),
        "sale_consideration": sale_amount,
        "gain_loss": gain,
        "pct_gain_loss": pct,
        "sale_date": _iso(sale_d),
        "buy_date": _iso(buy_d),
        "no_of_days": days,
    }
    realised_payload["portfolio_id"] = _active_pf()
    res = _client().table("realised").insert(realised_payload).execute()
    realised_id = res.data[0]["id"] if res.data else None

    # Log the sell transaction
    _insert_transaction(
        stock_name=r["stock_name"],
        transaction_type="sell",
        quantity=sold_qty,
        price=float(selling_price),
        amount=sale_amount,
        transaction_date=sale_d or date.today(),
        notes=f"Sold {'partial' if partial_quantity and sold_qty < total_qty else 'full'} position",
        realised_id=realised_id,
    )

    if sold_qty >= total_qty:
        delete_holding(holding_id)
    else:
        # Partial sell — reduce quantity on the holding
        new_qty = total_qty - sold_qty
        new_invested = round(new_qty * float(r["purchase_cost"]), 2)
        update_holding(holding_id, quantity=new_qty, amount_invested=new_invested)

    # Sprint 3: trade journal entry (best-effort, never blocks the sell)
    if reason:
        try:
            add_journal_entry(
                stock_name=r["stock_name"],
                exit_date=sale_d or date.today(),
                exit_price=float(selling_price),
                qty_sold=sold_qty,
                reason=reason,
                notes=notes,
            )
        except Exception:
            pass

    _bust()


# ===============================================================
# TRADE JOURNAL (Sprint 3)
# ===============================================================

JOURNAL_REASONS = ("EXIT signal", "Profit booking", "Thesis broken", "Override/Other")


def add_journal_entry(stock_name: str, exit_date: date, exit_price: float,
                      qty_sold: float, reason: str, notes: Optional[str] = None):
    """One row per exit. `ticker` stores the full stock_name string
    ('COMPANY (XNSE:SYMBOL)') so the audit engine can derive the Yahoo
    symbol with the exact same parsing the dashboard uses."""
    payload = {
        "portfolio_id": _active_pf(),
        "ticker": stock_name,
        "exit_date": _iso(_clean_date(exit_date)),
        "exit_price": float(exit_price),
        "qty_sold": float(qty_sold),
        "reason": reason,
        "notes": notes or None,
    }
    _client().table("trade_journal").insert(payload).execute()
    _bust()


@st.cache_data(ttl=60)
def _get_journal_cached(pf: int) -> pd.DataFrame:
    res = (_client().table("trade_journal").select("*")
           .eq("portfolio_id", pf).order("exit_date", desc=True).execute())
    return pd.DataFrame(res.data or [])


def get_trade_journal() -> pd.DataFrame:
    return _get_journal_cached(_active_pf())


# ===============================================================
# WATCHLIST
# ===============================================================

@st.cache_data(ttl=30)
def _get_watchlist_cached(pf: int) -> pd.DataFrame:
    res = _client().table("watchlist").select("*").eq("portfolio_id", pf).order("created_at", desc=True).execute()
    return pd.DataFrame(res.data or [])


def get_watchlist() -> pd.DataFrame:
    return _get_watchlist_cached(_active_pf())


def add_watchlist(stock_name: str, target_buy_price: Optional[float] = None,
                  notes: Optional[str] = None, added_by: Optional[str] = None):
    payload = {
        "stock_name": stock_name.strip(),
        "target_buy_price": float(target_buy_price) if target_buy_price else None,
        "notes": notes,
        "added_by": added_by,
    }
    payload["portfolio_id"] = _active_pf()
    _client().table("watchlist").insert(payload).execute()
    _bust()


def graduate_from_watchlist(stock_name: str) -> bool:
    """When a stock becomes a holding, silently remove it from the SAME
    portfolio's watchlist. Matches on the (XNSE:SYM)/(XBOM:CODE) part so the
    free-text company-name half doesn't have to be identical. Returns True
    if something was removed."""
    import re as _re
    m = _re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", stock_name or "")
    if not m:
        return False
    needle = f"({m.group(1)}:{m.group(2).strip()})".upper()
    res = (_client().table("watchlist").select("id, stock_name")
            .eq("portfolio_id", _active_pf()).execute())
    removed = False
    for row in (res.data or []):
        if needle in str(row.get("stock_name", "")).upper().replace(" ", ""):
            _client().table("watchlist").delete().eq("id", row["id"]).execute()
            removed = True
    if removed:
        _bust()
    return removed


def delete_watchlist(watchlist_id: int):
    _client().table("watchlist").delete().eq("id", watchlist_id).execute()
    _bust()


def update_watchlist(watchlist_id: int, **kwargs):
    payload = {k: v for k, v in kwargs.items() if v is not None}
    if "target_buy_price" in payload and payload["target_buy_price"] is not None:
        payload["target_buy_price"] = float(payload["target_buy_price"])
    _client().table("watchlist").update(payload).eq("id", watchlist_id).execute()
    _bust()


# ===============================================================
# NOTES
# ===============================================================

@st.cache_data(ttl=30)
def _get_notes_cached(pf: int) -> pd.DataFrame:
    res = _client().table("notes").select("*").eq("portfolio_id", pf).order("created_at", desc=True).execute()
    return pd.DataFrame(res.data or [])


def get_notes() -> pd.DataFrame:
    return _get_notes_cached(_active_pf())


def add_note(author: str, note: str, note_date: Optional[date] = None):
    payload = {
        "author": author,
        "note": note,
        "note_date": _iso(note_date or date.today()),
    }
    payload["portfolio_id"] = _active_pf()
    _client().table("notes").insert(payload).execute()
    _bust()


def delete_note(note_id: int):
    _client().table("notes").delete().eq("id", note_id).execute()
    _bust()


# ===============================================================
# SNAPSHOTS  (daily portfolio value for equity curve)
# ===============================================================

@st.cache_data(ttl=60)
def _get_snapshots_cached(pf: int) -> pd.DataFrame:
    res = _client().table("snapshots").select("*").eq("portfolio_id", pf).order("snapshot_date").execute()
    df = pd.DataFrame(res.data or [])
    if not df.empty:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    return df


def get_snapshots() -> pd.DataFrame:
    return _get_snapshots_cached(_active_pf())


def upsert_snapshot(snap: dict):
    """Insert or replace today's snapshot."""
    today = date.today().isoformat()
    payload = {
        "portfolio_id": _active_pf(),
        "snapshot_date": today,
        "invested": round(snap["invested"], 2),
        "current_value": round(snap["current"], 2),
        "unrealised_pnl": round(snap["unrealised"], 2),
        "realised_pnl": round(snap["realised"], 2),
        "total_pnl": round(snap["total_pnl"], 2),
        "n_holdings": int(snap["n_holdings"]),
    }
    # Delete today's existing snap (if any), then insert
    _client().table("snapshots").delete().eq("snapshot_date", today).execute()
    _client().table("snapshots").insert(payload).execute()
    _bust()


# ===============================================================
# TRANSACTIONS  (immutable log of every buy/sell)
# ===============================================================

def _insert_transaction(stock_name: str, transaction_type: str,
                        quantity: float, price: float, amount: float,
                        transaction_date, notes: Optional[str] = None,
                        holding_id: Optional[int] = None,
                        realised_id: Optional[int] = None):
    """Low-level: just append a row to the transactions log."""
    payload = {
        "stock_name": stock_name.strip(),
        "transaction_type": transaction_type,
        "quantity": float(quantity),
        "price": float(price),
        "amount": float(amount),
        "transaction_date": _iso(transaction_date),
        "notes": notes,
        "holding_id": holding_id,
        "realised_id": realised_id,
    }
    payload["portfolio_id"] = _active_pf()
    _client().table("transactions").insert(payload).execute()


@st.cache_data(ttl=30)
def _get_transactions_cached(pf: int) -> pd.DataFrame:
    """All transactions, newest first."""
    res = (_client().table("transactions")
           .select("*").eq("portfolio_id", pf)
           .order("transaction_date", desc=True)
           .order("created_at", desc=True)
           .execute())
    df = pd.DataFrame(res.data or [])
    if not df.empty:
        df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    return df



def get_transactions() -> pd.DataFrame:
    return _get_transactions_cached(_active_pf())

def delete_transaction(tx_id: int):
    _client().table("transactions").delete().eq("id", tx_id).execute()
    _bust()


def buy_more(holding_id: int, additional_qty: float, price: float,
             transaction_date, notes: Optional[str] = None):
    """Add to an existing holding.

    Recalculates the weighted-average purchase cost, updates the holding,
    and logs the new buy in the transactions table.
    """
    holdings = get_holdings()
    row = holdings[holdings["id"] == holding_id]
    if row.empty:
        raise ValueError(f"Holding {holding_id} not found")
    r = row.iloc[0]

    old_qty = float(r["quantity"])
    old_cost = float(r["purchase_cost"])
    add_qty = float(additional_qty)
    add_price = float(price)

    if add_qty <= 0 or add_price < 0:
        raise ValueError("Quantity must be > 0 and price cannot be negative "
                          "(price 0 = bonus shares)")

    new_qty = old_qty + add_qty
    # Weighted average cost
    new_avg = ((old_qty * old_cost) + (add_qty * add_price)) / new_qty
    new_avg = round(new_avg, 2)
    new_invested = round(new_qty * new_avg, 2)

    # Update the holding
    update_holding(
        holding_id,
        quantity=new_qty,
        purchase_cost=new_avg,
        amount_invested=new_invested,
    )

    # Log the additional buy
    _insert_transaction(
        stock_name=r["stock_name"],
        transaction_type="buy",
        quantity=add_qty,
        price=add_price,
        amount=round(add_qty * add_price, 2),
        transaction_date=transaction_date,
        notes=notes or f"Added to existing position (avg cost {old_cost:.2f} -> {new_avg:.2f})",
        holding_id=holding_id,
    )
    _bust()

    return {
        "old_qty": old_qty, "old_avg": old_cost,
        "new_qty": new_qty, "new_avg": new_avg,
        "new_invested": new_invested,
    }


# ---------------------------------------------------------------------------
# SME daily prices (bhavcopy pipeline — Sprint 3)
# ---------------------------------------------------------------------------
def get_sme_daily_prices(tickers: tuple) -> pd.DataFrame:
    """All stored bhavcopy rows for the given tickers, oldest first.
    Empty df if none of these tickers have any bhavcopy data yet
    (e.g. before the first backfill has run) -- callers must handle that
    gracefully, same as any other missing-data case in this codebase."""
    if not tickers:
        return pd.DataFrame()
    try:
        res = (_client().table("sme_daily_prices")
               .select("*").in_("ticker", list(tickers))
               .order("price_date").execute())
        df = pd.DataFrame(res.data or [])
        if not df.empty:
            df["price_date"] = pd.to_datetime(df["price_date"])
        return df
    except Exception:
        return pd.DataFrame()
