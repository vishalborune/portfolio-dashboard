"""
exit_audit.py — Sprint 3 audit engine.

For every exit logged in trade_journal, checks back at 30/60/90 days:
what did the stock actually do after we sold?

- Runs headless on GitHub Actions (see alerts.yml, 'audit' job).
- Idempotent and silent: a run with nothing matured does nothing.
- When an audit window matures, it fills price_XXd + audited_XXd on the
  journal row and sends ONE Telegram summary for all newly completed
  audits in that run.
- Price path mirrors the dashboard exactly: yfinance daily bars first,
  our own bhavcopy-derived sme_daily_prices table as fallback for the
  NSE Emerge / BSE SME stocks Yahoo doesn't carry. The ticker string is
  derived from the stored stock_name with the same regex app.py uses,
  so both sides can never disagree on which symbol is being priced.

Usage:  python exit_audit.py
Env:    SUPABASE_URL, SUPABASE_SERVICE_KEY  (required)
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (optional — skip to run silent)
"""
import os
import re
import sys
from datetime import date, timedelta

import pandas as pd
import requests
import yfinance as yf
from supabase import create_client

WINDOWS = (30, 60, 90)
# Don't chase forever: if a window is more than this many days past due and
# still has no price (delisted, symbol changed...), leave it pending and a
# human can look. Prevents infinite retries on dead tickers.
MAX_LATENESS_DAYS = 45


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _client():
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


def extract_yf_ticker(name: str):
    """Same parsing as app.py: 'COMPANY (XNSE:SYMBOL)' -> 'SYMBOL.NS'."""
    if not isinstance(name, str):
        return None
    m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", name)
    if not m:
        return None
    exch, sym = m.group(1), m.group(2).strip()
    return f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"


def short_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = re.sub(r"\s*\([^)]*\)\s*$", "", name)
    name = re.sub(r"\s+(LIMITED|LTD\.?|LTD)\s*$", "", name, flags=re.IGNORECASE)
    return name.strip().title()


def fetch_daily_closes(client, ticker: str, since: date) -> pd.DataFrame:
    """Daily closes from `since` onward. yfinance first; bhavcopy-derived
    sme_daily_prices as fallback (same order as signals.fetch_weekly).
    Returns df with columns [date, close], oldest first. Empty on failure."""
    # 1) Yahoo
    try:
        df = yf.download(ticker, start=since.isoformat(), interval="1d",
                         progress=False, auto_adjust=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.reset_index().rename(columns={"Date": "date", "Close": "close"})
            df = df.dropna(subset=["close"])
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"]).dt.date
                return df[["date", "close"]].reset_index(drop=True)
    except Exception:
        pass
    # 2) Our own SME table (NSE Emerge / BSE SME stocks)
    try:
        res = (_client_or(client).table("sme_daily_prices")
               .select("price_date, close").eq("ticker", ticker)
               .gte("price_date", since.isoformat())
               .order("price_date").execute())
        df = pd.DataFrame(res.data or [])
        if not df.empty:
            df["date"] = pd.to_datetime(df["price_date"]).dt.date
            return df[["date", "close"]].reset_index(drop=True)
    except Exception:
        pass
    return pd.DataFrame()


def _client_or(client):
    return client if client is not None else _client()


def price_on_or_after(daily: pd.DataFrame, target: date):
    """First available close on/after target date (markets close on
    weekends/holidays, so 'exactly +30 days' may not be a trading day)."""
    if daily.empty:
        return None, None
    hit = daily[daily["date"] >= target]
    if hit.empty:
        return None, None
    row = hit.iloc[0]
    return float(row["close"]), row["date"]


def send_telegram(text: str):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        print("[audit] Telegram env not set — printing instead:\n" + text)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
    except Exception as e:
        print(f"[audit] Telegram send failed: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    client = _client()
    today = date.today()

    res = client.table("trade_journal").select("*").execute()
    rows = res.data or []
    if not rows:
        print("[audit] journal empty — nothing to do")
        return

    completed_lines = []
    daily_cache = {}  # ticker -> df, so multiple windows share one fetch

    for row in rows:
        exit_d = date.fromisoformat(str(row["exit_date"])[:10])
        exit_price = float(row["exit_price"])
        ticker = extract_yf_ticker(row["ticker"])
        if not ticker:
            print(f"[audit] cannot parse ticker from '{row['ticker']}' — skipping")
            continue

        updates = {}
        for w in WINDOWS:
            if row.get(f"price_{w}d") is not None:
                continue                      # already audited
            due = exit_d + timedelta(days=w)
            if today < due:
                continue                      # window not matured yet
            if (today - due).days > MAX_LATENESS_DAYS:
                continue                      # stale — leave for a human

            if ticker not in daily_cache:
                daily_cache[ticker] = fetch_daily_closes(client, ticker, exit_d)
            price, actual_d = price_on_or_after(daily_cache[ticker], due)
            if price is None:
                print(f"[audit] no price yet for {ticker} at +{w}d — will retry next run")
                continue

            updates[f"price_{w}d"] = price
            updates[f"audited_{w}d"] = today.isoformat()

            chg = (price - exit_price) / exit_price * 100
            verdict = ("✅ rule <b>saved</b>" if chg < 0
                       else "❌ exit <b>cost</b>")
            completed_lines.append(
                f"• <b>{short_name(row['ticker'])}</b> — exited ₹{exit_price:,.1f} "
                f"({row['reason']}, {exit_d.strftime('%d %b')}) → "
                f"+{w}d: ₹{price:,.1f}. {verdict} {abs(chg):.1f}%"
            )

        if updates:
            try:
                client.table("trade_journal").update(updates).eq("id", row["id"]).execute()
                print(f"[audit] {ticker}: filled {sorted(updates.keys())}")
            except Exception as e:
                print(f"[audit] update failed for journal id {row['id']}: {e}")

    if completed_lines:
        send_telegram("📓 <b>Exit audit update</b>\n"
                      "What your sold stocks did after the exit:\n\n"
                      + "\n".join(completed_lines)
                      + "\n\n<i>'saved' = stock fell after selling. "
                        "'cost' = it kept rising.</i>")
        print(f"[audit] {len(completed_lines)} audit(s) completed and reported")
    else:
        print("[audit] no windows matured today")


if __name__ == "__main__":
    sys.exit(run())
