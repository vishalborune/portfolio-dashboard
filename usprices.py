"""
usprices.py — the US-market price layer for Vishal's US book (portfolio 4).

WHAT IT OWNS (26-Sep-2026):
  1. Nightly EOD store: Yahoo daily bars for every US holding/watchlist name are
     written into `sme_daily_prices` — the SAME table bhavcopy fills for India —
     under the bare US symbol (AMZN, NVDA ...). Because `signals` already reads
     that table first (bhavcopy-first, Yahoo only when our history is short), the
     weekly flowchart, DMAs and peaks become own-data-first for US names with NO
     change to signals.py once >= MIN_BHAV_WEEKS/MIN_BHAV_DAILY_ROWS exist.
  2. Live quotes: Finnhub `/quote` (free tier, 60 calls/min) for the ~1-min entry
     poller. Yahoo's quote is the fallback; both go through the same 25% sanity
     band in the caller (`alerts._sane_quotes` lesson: port the GUARD, not just
     the logic).
  3. Health: coverage / staleness / agreement, Telegrammed to the US group.
     Agreement compares OUR stored close with Finnhub's independently-sourced
     close for the same session — the number that actually matters — and always
     reports the sample size ("0 compared" is itself a finding).

WHY NOT AN OFFICIAL FILE: the US exchanges sell their consolidated daily data;
there is no free bhavcopy equivalent. Stooq (no key) is behind a JavaScript
challenge (checked 26-Sep-2026 — same wall as BSE's announcements API).
Finnhub's candle endpoint is premium-only. Yahoo daily bars are reliable for
US large caps and are split-ADJUSTED (auto_adjust=False still adjusts for
splits, only not for dividends — verified on Websol's 10:1). So, unlike
bhavcopy, rows stored here are adjusted at write time; a split that happens
AFTER rows were stored leaves a step in OUR table (older rows are pre-split),
which the existing corporate-action gap detector flags — register it in
corporate_actions.CORPORATE_ACTIONS exactly as for CWD/PGIL/E2E.

CLI (read-only unless stated):
  python usprices.py check      fetch + print the latest bars and Finnhub quotes
  python usprices.py health     coverage / staleness / agreement (no Telegram)
  python usprices.py quotes     Finnhub live quotes for the universe
  python usprices.py store      WRITE: upsert the last ~10 sessions (nightly job)
  python usprices.py backfill   WRITE: upsert 2 years of history (one-off)
"""
from __future__ import annotations
import io, os, re, sys, time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

NY = ZoneInfo("America/New_York")
US_PORTFOLIOS = {4}
FINNHUB_URL = "https://finnhub.io/api/v1/quote"
FINNHUB_MAX_PER_MIN = 55          # free tier is 60/min; keep headroom
AGREE_TOL_PCT = 1.0               # stored close vs Finnhub close
STALE_DAYS = 5                    # newest stored row older than this = finding


# ---------------------------------------------------------------------------
# universe / clock
# ---------------------------------------------------------------------------

def us_symbol(stock_name: str):
    """'ALPHABET INC CLASS A (XNAS:GOOGL)' -> 'GOOGL' (Yahoo form: BRK.B -> BRK-B)."""
    m = re.search(r"\((?:XNAS|XNYS):([^)]+)\)", str(stock_name or ""))
    return m.group(1).strip().upper().replace(".", "-") if m else None


def us_universe(client) -> list[str]:
    """Every US holding + watchlist symbol, across the US portfolios."""
    syms = set()
    for table in ("holdings", "watchlist"):
        try:
            rows = (client.table(table).select("stock_name,portfolio_id")
                    .in_("portfolio_id", list(US_PORTFOLIOS)).execute().data or [])
        except Exception as e:
            print(f"  [usprices] could not read {table}: {e}")
            continue
        for r in rows:
            s = us_symbol(r.get("stock_name"))
            if s:
                syms.add(s)
    return sorted(syms)


def last_expected_close_date() -> date:
    """The US session whose close we should be able to see right now (NY clock).
    Holidays are not tracked; the health check tolerates STALE_DAYS for that."""
    n = datetime.now(NY)
    d = n.date() if (n.weekday() < 5 and (n.hour * 60 + n.minute) >= 16 * 60) else n.date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def market_is_open() -> bool:
    n = datetime.now(NY)
    return n.weekday() < 5 and (9 * 60 + 30) <= (n.hour * 60 + n.minute) <= 16 * 60


# ---------------------------------------------------------------------------
# EOD bars (Yahoo) -> our table
# ---------------------------------------------------------------------------

def fetch_eod(symbols: list[str], period: str = "10d") -> dict:
    """{symbol: DataFrame[date, open, high, low, close, volume]} from Yahoo daily
    bars. Logs WHY when a symbol comes back empty (House Rule #3)."""
    out = {}
    if not symbols:
        return out
    import yfinance as yf
    try:
        data = yf.download(list(symbols), period=period, interval="1d", auto_adjust=False,
                           group_by="ticker", progress=False, threads=True)
    except Exception as e:
        print(f"  [usprices] Yahoo download failed for {len(symbols)} symbols: {type(e).__name__}: {e}")
        return out
    for s in symbols:
        try:
            df = data if len(symbols) == 1 else data[s]
            df = df.dropna(subset=["Close"])
            if df.empty:
                print(f"  [usprices] {s}: Yahoo returned 0 bars")
                continue
            d = pd.DataFrame({
                "date": [pd.Timestamp(i).date() for i in df.index],
                "open": df["Open"].astype(float).values, "high": df["High"].astype(float).values,
                "low": df["Low"].astype(float).values, "close": df["Close"].astype(float).values,
                "volume": df["Volume"].astype(float).values,
            })
            out[s] = d
        except Exception as e:
            print(f"  [usprices] {s}: could not read bars: {type(e).__name__}: {e}")
    return out


def store(client, symbols: list[str] | None = None, period: str = "10d") -> int:
    """Upsert Yahoo daily bars into sme_daily_prices (one batched upsert per
    symbol). Returns rows written. Idempotent — worker and GitHub can both run it."""
    from bhavcopy import _f
    symbols = symbols or us_universe(client)
    bars = fetch_eod(symbols, period=period)
    written = 0
    for s, df in bars.items():
        rows = [{"ticker": s, "price_date": r.date.isoformat(),
                 "open": _f(r.open), "high": _f(r.high), "low": _f(r.low),
                 "close": _f(r.close), "volume": _f(r.volume)}
                for r in df.itertuples(index=False)]
        for i in range(0, len(rows), 500):
            try:
                client.table("sme_daily_prices").upsert(rows[i:i + 500], on_conflict="ticker,price_date").execute()
                written += len(rows[i:i + 500])
            except Exception as e:
                print(f"  [usprices] store failed for {s} (batch {i // 500}): {e}")
    missing = sorted(set(symbols) - set(bars))
    print(f"  [usprices] stored {written} rows for {len(bars)}/{len(symbols)} symbols"
          + (f" — NO DATA for {', '.join(missing)}" if missing else ""))
    return written


def backfill(client, symbols: list[str] | None = None, period: str = "2y") -> int:
    return store(client, symbols, period=period)


# ---------------------------------------------------------------------------
# live quotes (Finnhub)
# ---------------------------------------------------------------------------

def _finnhub_key():
    k = os.environ.get("FINNHUB_API_KEY")
    if not k:
        try:
            import tomllib
            k = tomllib.load(open(".streamlit/secrets.toml", "rb")).get("FINNHUB_API_KEY")
        except Exception:
            k = None
    return k


def finnhub_quotes(symbols: list[str]) -> dict:
    """{symbol: {"last", "prev_close", "day_pct", "ts"}} from Finnhub /quote.
    Rate-limited to stay inside the free tier. Missing key / failures log WHY."""
    key = _finnhub_key()
    out = {}
    if not key:
        print("  [usprices] FINNHUB_API_KEY missing — no live quotes")
        return out
    for i, s in enumerate(symbols):
        if i and i % FINNHUB_MAX_PER_MIN == 0:
            time.sleep(60)
        try:
            r = requests.get(FINNHUB_URL, params={"symbol": s, "token": key}, timeout=10)
            if r.status_code != 200:
                print(f"  [usprices] finnhub {s}: HTTP {r.status_code} {r.text[:80]}")
                continue
            j = r.json()
            last, pc = float(j.get("c") or 0), float(j.get("pc") or 0)
            if last <= 0:
                print(f"  [usprices] finnhub {s}: empty quote {j}")
                continue
            out[s] = {"last": last, "prev_close": pc or None,
                      "day_pct": float(j.get("dp")) if j.get("dp") is not None else None,
                      "ts": datetime.fromtimestamp(int(j.get("t") or 0), NY)}
        except Exception as e:
            print(f"  [usprices] finnhub {s}: {type(e).__name__}: {e}")
    return out


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------

def health(client, notify: bool = True) -> list[str]:
    """Coverage / staleness / agreement for the US universe. Sample sizes are
    always reported. Telegrams the US group when there is anything to say."""
    syms = us_universe(client)
    findings = []
    if not syms:
        return findings
    res = (client.table("sme_daily_prices").select("ticker,price_date,close")
           .in_("ticker", syms).gte("price_date", (date.today() - timedelta(days=14)).isoformat())
           .order("price_date", desc=True).execute().data or [])
    newest = {}
    for r in res:
        newest.setdefault(r["ticker"], r)          # first seen = newest (DESC)
    expected = last_expected_close_date()
    for s in syms:
        if s not in newest:
            findings.append(f"COVERAGE: {s} has no stored price in the last 14 days")
        else:
            age = (expected - date.fromisoformat(newest[s]["price_date"])).days
            if age > STALE_DAYS:
                findings.append(f"STALE: {s} newest row {newest[s]['price_date']} is {age}d behind the expected close {expected}")
    quotes = finnhub_quotes([s for s in syms if s in newest])
    compared, diverged = 0, []
    for s, q in quotes.items():
        row = newest[s]
        # After the close, Finnhub's `c` IS that session's close; before the next
        # open its `pc` is the same number. Compare whichever matches our date.
        ours = float(row["close"])
        ref = q["last"] if (q["ts"].date() == date.fromisoformat(row["price_date"])) else q["prev_close"]
        if not ref:
            continue
        compared += 1
        gap = abs(ours / ref - 1) * 100
        if gap > AGREE_TOL_PCT:
            diverged.append(f"DISAGREE: {s} stored {ours:.2f} vs Finnhub {ref:.2f} on {row['price_date']} ({gap:.1f}%)")
    findings += diverged
    summary = (f"US price health: {len(syms)} symbols, {len(syms) - sum(1 for f in findings if f.startswith('COVERAGE'))} covered, "
               f"{compared} compared with Finnhub, {len(diverged)} diverged, expected close {expected}")
    print("  [usprices] " + summary)
    for f in findings:
        print("  [usprices]   ⚠️ " + f)
    if compared == 0:
        findings.append("AGREEMENT CHECK RAN ON 0 SYMBOLS — Finnhub quotes unavailable; nothing was verified")
    if notify and findings:
        try:
            import notify as _n
            _n.send_telegram("⚠️ <b>US price data</b>\n" + summary + "\n" + "\n".join("• " + f for f in findings),
                             chat_id=_n.chat_for_group("vishal_us"))
        except Exception as e:
            print(f"  [usprices] telegram failed: {e}")
    return findings


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _client():
    from supabase import create_client
    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY")
    if not (url and key):
        import tomllib
        sec = tomllib.load(open(".streamlit/secrets.toml", "rb"))
        url, key = sec["SUPABASE_URL"], sec["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "check"
    c = _client()
    syms = us_universe(c)
    print(f"US universe ({len(syms)}): {', '.join(syms)} | NY open: {market_is_open()} | expected close: {last_expected_close_date()}")
    if mode == "check":
        bars = fetch_eod(syms, period="5d")
        q = finnhub_quotes(syms)
        print(f"{'sym':<7}{'bars':>5}{'last bar':>12}{'close':>10}{'finnhub':>10}{'prev':>10}{'day%':>7}")
        for s in syms:
            b = bars.get(s)
            qq = q.get(s)
            last = b.iloc[-1] if b is not None and len(b) else None
            nbars = len(b) if b is not None else 0
            last_d = str(last.date) if last is not None else "—"
            last_c = f"{last.close:.2f}" if last is not None else "—"
            fh_last = f"{qq['last']:.2f}" if qq else "—"
            fh_prev = f"{qq['prev_close']:.2f}" if qq and qq.get("prev_close") else "—"
            fh_pct = f"{qq['day_pct']:+.2f}" if qq and qq.get("day_pct") is not None else "—"
            print(f"{s:<7}{nbars:>5}{last_d:>12}{last_c:>10}{fh_last:>10}{fh_prev:>10}{fh_pct:>7}")
    elif mode == "quotes":
        for s, qq in finnhub_quotes(syms).items():
            print(f"{s:<7} last {qq['last']:.2f}  prev {qq['prev_close']}  {qq['day_pct']:+.2f}%  at {qq['ts']:%Y-%m-%d %H:%M} NY")
    elif mode == "health":
        health(c, notify=False)
    elif mode == "store":
        store(c, syms)
    elif mode == "backfill":
        backfill(c, syms)
    else:
        print(__doc__)
