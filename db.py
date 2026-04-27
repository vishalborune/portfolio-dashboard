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

@st.cache_data(ttl=30)
def get_holdings() -> pd.DataFrame:
    res = _client().table("holdings").select("*").order("created_at").execute()
    df = pd.DataFrame(res.data or [])
    return df


def add_holding(stock_name: str, quantity: float, purchase_cost: float,
                amount_invested: Optional[float] = None,
                buy_date: Optional[date] = None,
                notes: Optional[str] = None):
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
    _client().table("holdings").insert(payload).execute()
    _bust()


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


# ===============================================================
# REALISED P&L  (closed trades)
# ===============================================================

@st.cache_data(ttl=60)
def get_realised() -> pd.DataFrame:
    res = _client().table("realised").select("*").order("sale_date", desc=True).execute()
    df = pd.DataFrame(res.data or [])
    return df


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
    no_of_days = None
    if buy_date and sale_date:
        bd = buy_date if isinstance(buy_date, date) else datetime.fromisoformat(str(buy_date)).date()
        sd = sale_date if isinstance(sale_date, date) else datetime.fromisoformat(str(sale_date)).date()
        no_of_days = (sd - bd).days
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
    _client().table("realised").insert(payload).execute()
    _bust()


def delete_realised(realised_id: int):
    _client().table("realised").delete().eq("id", realised_id).execute()
    _bust()


def mark_as_sold(holding_id: int, selling_price: float, sale_date: date,
                 partial_quantity: Optional[float] = None):
    """Move a holding (or part of it) into the realised table.

    If partial_quantity is set and < total, the remaining quantity stays in
    holdings (so partial sells work).
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

    add_realised(
        stock_name=r["stock_name"],
        quantity=sold_qty,
        purchase_cost=float(r["purchase_cost"]),
        selling_price=selling_price,
        sale_date=sale_date,
        buy_date=r.get("buy_date"),
        amount_invested=round(sold_qty * float(r["purchase_cost"]), 2),
    )

    if sold_qty >= total_qty:
        delete_holding(holding_id)
    else:
        # Partial sell — reduce quantity on the holding
        new_qty = total_qty - sold_qty
        new_invested = round(new_qty * float(r["purchase_cost"]), 2)
        update_holding(holding_id, quantity=new_qty, amount_invested=new_invested)


# ===============================================================
# WATCHLIST
# ===============================================================

@st.cache_data(ttl=30)
def get_watchlist() -> pd.DataFrame:
    res = _client().table("watchlist").select("*").order("created_at", desc=True).execute()
    return pd.DataFrame(res.data or [])


def add_watchlist(stock_name: str, target_buy_price: Optional[float] = None,
                  notes: Optional[str] = None, added_by: Optional[str] = None):
    payload = {
        "stock_name": stock_name.strip(),
        "target_buy_price": float(target_buy_price) if target_buy_price else None,
        "notes": notes,
        "added_by": added_by,
    }
    _client().table("watchlist").insert(payload).execute()
    _bust()


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
def get_notes() -> pd.DataFrame:
    res = _client().table("notes").select("*").order("created_at", desc=True).execute()
    return pd.DataFrame(res.data or [])


def add_note(author: str, note: str, note_date: Optional[date] = None):
    payload = {
        "author": author,
        "note": note,
        "note_date": _iso(note_date or date.today()),
    }
    _client().table("notes").insert(payload).execute()
    _bust()


def delete_note(note_id: int):
    _client().table("notes").delete().eq("id", note_id).execute()
    _bust()


# ===============================================================
# SNAPSHOTS  (daily portfolio value for equity curve)
# ===============================================================

@st.cache_data(ttl=60)
def get_snapshots() -> pd.DataFrame:
    res = _client().table("snapshots").select("*").order("snapshot_date").execute()
    df = pd.DataFrame(res.data or [])
    if not df.empty:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    return df


def upsert_snapshot(snap: dict):
    """Insert or replace today's snapshot."""
    today = date.today().isoformat()
    payload = {
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
