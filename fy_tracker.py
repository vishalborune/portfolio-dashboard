"""
fy_tracker.py — financial-year return target tracker (Lakshmi + Vishal, 04-Oct-2026).

Lakshmi's goal is 50% this financial year; Vishal's and Abinaya's too. The
question it answers: of the money actually at work in stocks, what has it earned
so far, and is that on pace for the target?

MEASURE — Modified Dietz, NOT annualised (agreed 04-Oct-2026):
    R = (V_end - V_start - sum(F)) / (V_start + sum(w_i * F_i))
F = money put into stocks (buys +, sells -) after the start, w_i = share of the
window each flow was invested. Deposits/withdrawals to the broker account don't
matter — only money that went into stocks — so idle cash never dilutes it.
CAGR was rejected (treats every deposit as return); XIRR was rejected as the
tracker (annualises a part-year, so +20% in October reads as ~+45% and looks on
target when it isn't; it stays in the digest for the long view).

PACE — compounding, not straight-line: to make 50% over 365 days the book must be
at (1.5)^(days/365) - 1 on any given day (+22.7% at day 184, not +25%).

START VALUE — rebuilt BACKWARDS from today, never from a stored snapshot:
    holdings at the start = holdings today - buys since + sells since
priced at the start date's RAW exchange close (our bhavcopy table, or NSE/BSE's
own file for names we never tracked). The early weekly snapshots carry data
errors that were fixed later, so they are not trusted as a starting point.

WINDOWS:
  • Vishal (pf1): 1 April — his FY transaction log is complete (verified
    04-Oct-2026 after fixing a swapped Rashi entry and two double-logged sales).
  • Lakshmi (pf2) / Abinaya (pf3): from the 10-Jul-2026 close — live logging began
    12/14-Jul; April–July needs their 31-March holdings + tradebook from the
    brokers. Delete their START_OVERRIDE entries once that history is loaded.
  • Vishal US (pf4): from the first buy (24-Sep-2026), separate scorecard.
  The window's target is the same annual pace applied to the window's length.

FAIL-SAFE (House Rule #2): a position that comes out NEGATIVE at the start (a buy
or sale missing from the log), an opening holding with no price, or a corporate
action between the start and a trade that the log doesn't account for → no number
is shown; the status names what needs fixing instead.
"""
from __future__ import annotations

import json
import os
import re
from datetime import date, timedelta

TARGET_PCT = {1: 50.0, 2: 50.0, 3: 50.0, 4: 50.0}
START_OVERRIDE = {2: date(2026, 7, 10), 3: date(2026, 7, 10)}
START_NOTE = {2: "measured from 10 Jul — April–July pending the broker's 31-Mar holdings + tradebook",
              3: "measured from 10 Jul — April–July pending the broker's 31-Mar holdings + tradebook"}
US_PORTFOLIOS = {4}
INDEX_TICKER = {"IN": ("NIFTYSMLCAP250.IDX", "Nifty Smallcap 250"), "US": ("QQQ", "Nasdaq 100 (QQQ)")}
CACHE_DIR = ".cache"

_TICK_RE = re.compile(r"\((X(?:NSE|BOM|NAS|NYS)):([^)]+)\)")


def ticker_of(stock_name: str):
    m = _TICK_RE.search(str(stock_name or ""))
    if not m:
        return None
    exch, sym = m.group(1), m.group(2).strip()
    if exch == "XNSE":
        return f"{sym}.NS"
    if exch == "XBOM":
        return f"{sym}.BO"
    return sym.replace(".", "-")


def fy_window(today: date):
    start = date(today.year if today.month >= 4 else today.year - 1, 4, 1)
    return start, date(start.year + 1, 3, 31)


def _all(client, table, cols, pf):
    out, i = [], 0
    while True:
        r = (client.table(table).select(cols).eq("portfolio_id", pf)
             .range(i, i + 999).execute().data or [])
        out += r
        i += 1000
        if len(r) < 1000:
            return out


# ---------------------------------------------------------------------------
# prices
# ---------------------------------------------------------------------------

_FILE_MEMO: dict = {}


def _file_closes(d: date) -> dict:
    """{ticker: raw close} for EVERY NSE + BSE name on trading day d, from the
    exchanges' own files. Cached per date (a past day's file never changes)."""
    if d in _FILE_MEMO:
        return _FILE_MEMO[d]
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"bhav_closes_{d:%Y%m%d}.json")
    if os.path.exists(path):
        try:
            return json.load(open(path, encoding="utf-8"))
        except Exception:
            pass
    import bhavcopy
    out = {}
    try:
        n = bhavcopy.fetch_nse_bhavcopy(d)
        if not n.empty and "CLOSE" in n.columns:
            series = n["SERIES"].astype(str).str.strip() if "SERIES" in n.columns else None
            for i, r in n.iterrows():
                if series is not None and series[i] not in ("EQ", "BE", "SM", "ST", "BZ"):
                    continue
                out.setdefault(f"{r['SYMBOL']}.NS", float(r["CLOSE"]))
    except Exception as e:
        print(f"  [fy] NSE file {d} unreadable: {e}")
    try:
        b = bhavcopy.fetch_bse_bhavcopy(d)
        if not b.empty and "SC_CODE" in b.columns:
            for _, r in b.iterrows():
                out.setdefault(f"{int(r['SC_CODE'])}.BO", float(r["CLOSE"]))
    except Exception as e:
        print(f"  [fy] BSE file {d} unreadable: {e}")
    if out:
        json.dump(out, open(path, "w", encoding="utf-8"))
    # Memo empties too (a holiday has no file — 31-Mar-2026 was one), but only
    # per process: a failed download must not be cached to disk forever.
    _FILE_MEMO[d] = out
    return out


def _scrip_code(ticker: str):
    """Numeric BSE code for an alphabetic BSE ticker (XBOM:SHUKRAPHAR)."""
    base = ticker[:-3]
    if base.isdigit():
        return ticker
    try:
        import bhavcopy
        code = (bhavcopy.SME_STOCKS.get(ticker) or {}).get("scrip_code")
        return f"{code}.BO" if code else None
    except Exception:
        return None


def close_on(client, ticker: str, d: date):
    """RAW close on d, or the latest trading day within 7 days before it.
    Our own table first; the exchange file for names we never stored."""
    try:
        r = (client.table("sme_daily_prices").select("price_date,close").eq("ticker", ticker)
             .lte("price_date", d.isoformat()).gte("price_date", (d - timedelta(days=7)).isoformat())
             .order("price_date", desc=True).limit(1).execute().data or [])
        if r:
            return float(r[0]["close"])
    except Exception:
        pass
    if ticker.endswith(".NS") or ticker.endswith(".BO"):
        key = _scrip_code(ticker) if ticker.endswith(".BO") else ticker
        for back in range(0, 7):
            dd = d - timedelta(days=back)
            if dd.weekday() >= 5:
                continue
            closes = _file_closes(dd)
            if closes:
                return closes.get(key)
    return None


def latest_close(client, ticker: str):
    try:
        r = (client.table("sme_daily_prices").select("close").eq("ticker", ticker)
             .order("price_date", desc=True).limit(1).execute().data or [])
        if r:
            return float(r[0]["close"])
    except Exception:
        pass
    try:
        import signals
        df = signals._fetch_daily(ticker)
        return float(df["close"].iloc[-1]) if df is not None and not df.empty else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# the computation
# ---------------------------------------------------------------------------

def compute(client, pf: int, v_end: float | None = None, end: date | None = None) -> dict:
    """FY-target progress for one portfolio. `v_end` lets the caller pass the
    value it already shows (dashboard / digest) so the tracker and that screen
    can never disagree; otherwise it is priced here at the latest stored close."""
    end = end or date.today()
    target = TARGET_PCT.get(pf, 50.0)
    fy_start, fy_end = fy_window(end)
    us = pf in US_PORTFOLIOS
    tx = _all(client, "transactions", "stock_name,transaction_type,quantity,price,amount,transaction_date", pf)
    hold = _all(client, "holdings", "stock_name,quantity", pf)

    if us:
        first = min((date.fromisoformat(str(t["transaction_date"])[:10]) for t in tx), default=end)
        start, note = first - timedelta(days=1), "measured from the book's first buy"
    else:
        start = START_OVERRIDE.get(pf, fy_start - timedelta(days=1))
        note = START_NOTE.get(pf, "measured from 1 April")
    window_end = fy_end if not us or fy_end > start else end

    flows, net_qty = [], {}
    for t in tx:
        d = date.fromisoformat(str(t["transaction_date"])[:10])
        if not (start < d <= end):
            continue
        tk = ticker_of(t["stock_name"])
        q = float(t["quantity"] or 0)
        amt = float(t["amount"]) if t.get("amount") not in (None, "") else q * float(t["price"] or 0)
        sign = 1 if t["transaction_type"] == "buy" else -1
        flows.append((d, sign * amt, tk, sign * q, float(t["price"] or 0)))
        net_qty[tk] = net_qty.get(tk, 0.0) + sign * q

    cur_qty = {}
    for h in hold:
        tk = ticker_of(h["stock_name"])
        cur_qty[tk] = cur_qty.get(tk, 0.0) + float(h["quantity"] or 0)

    base = {"pf": pf, "target_pct": target, "start": start, "end": end, "window_end": window_end,
            "note": note, "market": "US" if us else "IN"}
    problems, opening = [], {}
    for tk in set(cur_qty) | set(net_qty):
        q0 = cur_qty.get(tk, 0.0) - net_qty.get(tk, 0.0)
        if q0 < -0.001:
            problems.append(f"{tk}: {q0:,.4g} shares at the start — a buy or sale is missing from the log")
        elif q0 > 0.001:
            opening[tk] = q0
    if problems:
        return {**base, "status": "CHECK", "problems": sorted(problems)}

    v_start = 0.0
    for tk, q in opening.items():
        px = close_on(client, tk, start)
        if not px:
            problems.append(f"{tk}: no close found for {start:%d %b %Y}")
            continue
        # A split/bonus between the start and the first later trade makes the
        # share count and the start price disagree in basis. Bonus lots logged
        # as price-0 buys are fine (they're in the flows); an unlogged one is not.
        later = [f for f in flows if f[2] == tk and f[4] > 0]
        if later and not any(f[2] == tk and f[4] == 0 for f in flows):
            ratio = later[0][4] / px
            if ratio < 0.5 or ratio > 2.0:
                problems.append(f"{tk}: start price {px:,.2f} vs first trade {later[0][4]:,.2f} "
                                f"({later[0][0]:%d %b}) — likely a split/bonus not in the log")
        v_start += q * px
    if problems:
        return {**base, "status": "CHECK", "problems": sorted(problems)}

    if v_end is None:
        v_end = 0.0
        for tk, q in cur_qty.items():
            if q <= 0:
                continue
            px = latest_close(client, tk)
            if px is None:
                return {**base, "status": "CHECK", "problems": [f"{tk}: no current price"]}
            v_end += q * px

    T = max((end - start).days, 1)
    net_flow = sum(f[1] for f in flows)
    weighted = sum(f[1] * (end - f[0]).days / T for f in flows)
    denom = v_start + weighted
    if denom <= 0:
        return {**base, "status": "CHECK", "problems": ["no capital employed in the window yet"]}
    pnl = v_end - v_start - net_flow
    ret = pnl / denom * 100

    g = 1 + target / 100
    window_days = max((window_end - start).days, 1)
    window_target = (g ** (window_days / 365) - 1) * 100
    pace_now = (g ** (T / 365) - 1) * 100
    remaining = max((window_end - end).days, 0)
    needed = ((1 + window_target / 100) / (1 + ret / 100) - 1) * 100 if remaining else None

    idx = None
    itk, ilabel = INDEX_TICKER["US" if us else "IN"]
    i0, i1 = close_on(client, itk, start), latest_close(client, itk)
    if i0 and i1:
        idx = {"label": ilabel, "ret": (i1 / i0 - 1) * 100}

    return {**base, "status": "OK", "v_start": v_start, "v_end": v_end, "net_flow": net_flow,
            "pnl": pnl, "ret": ret, "pace_now": pace_now, "window_target": window_target,
            "ahead_pts": ret - pace_now, "remaining_days": remaining, "needed": needed,
            "days": T, "n_opening": len(opening), "n_flows": len(flows), "index": idx}


def combine(results: list[dict], label: str) -> dict | None:
    """Household line (Lakshmi + Abinaya): same window, money summed."""
    ok = [r for r in results if r.get("status") == "OK"]
    if len(ok) != len(results) or not ok or len({(r["start"], r["end"]) for r in ok}) != 1:
        return None
    r0 = ok[0]
    v0 = sum(r["v_start"] for r in ok)
    pnl = sum(r["pnl"] for r in ok)
    denom = sum(r["pnl"] / r["ret"] * 100 for r in ok if r["ret"])
    if not denom:
        return None
    ret = pnl / denom * 100
    return {**r0, "pf": None, "label": label, "v_start": v0, "v_end": sum(r["v_end"] for r in ok),
            "net_flow": sum(r["net_flow"] for r in ok), "pnl": pnl, "ret": ret,
            "ahead_pts": ret - r0["pace_now"],
            "needed": (((1 + r0["window_target"] / 100) / (1 + ret / 100) - 1) * 100
                       if r0["remaining_days"] else None)}


def verdict(r: dict) -> tuple[str, str]:
    """(colour, words) for an OK result."""
    if r["ahead_pts"] >= 0:
        return "#16a34a", f"ahead of the {r['target_pct']:.0f}% pace by {r['ahead_pts']:.1f} pts"
    if r["ahead_pts"] >= -5:
        return "#d97706", f"behind the {r['target_pct']:.0f}% pace by {-r['ahead_pts']:.1f} pts"
    return "#dc2626", f"behind the {r['target_pct']:.0f}% pace by {-r['ahead_pts']:.1f} pts"


if __name__ == "__main__":
    import sys
    import tomllib
    from supabase import create_client
    sys.stdout.reconfigure(encoding="utf-8")
    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY")
    if not (url and key):
        sec = tomllib.load(open(".streamlit/secrets.toml", "rb"))
        url, key = sec["SUPABASE_URL"], sec["SUPABASE_SERVICE_KEY"]
    c = create_client(url, key)
    res = {}
    for pf in (1, 2, 3, 4):
        r = compute(c, pf)
        res[pf] = r
        cur = "$" if r["market"] == "US" else "Rs "
        print(f"\npf{pf} · {r['note']} · window {r['start']:%d %b} -> {r['window_end']:%d %b %Y}")
        if r["status"] != "OK":
            print("   CHECK: " + "\n          ".join(r["problems"]))
            continue
        col, words = verdict(r)
        print(f"   start value {cur}{r['v_start']:,.0f} ({r['n_opening']} holdings) · net new money {cur}{r['net_flow']:,.0f} "
              f"({r['n_flows']} trades) · value now {cur}{r['v_end']:,.0f}")
        print(f"   return {r['ret']:+.2f}% (P&L {cur}{r['pnl']:,.0f}) · pace mark today {r['pace_now']:+.2f}% · {words}")
        print(f"   window target {r['window_target']:.1f}% · needed over the remaining {r['remaining_days']} days: "
              f"{r['needed']:+.1f}%" + (f" · {r['index']['label']} {r['index']['ret']:+.2f}% same window" if r["index"] else ""))
    h = combine([res[2], res[3]], "Lakshmi + Abinaya")
    if h:
        print(f"\nhousehold (pf2+pf3): return {h['ret']:+.2f}% · pace {h['pace_now']:+.2f}% · P&L Rs {h['pnl']:,.0f}")
