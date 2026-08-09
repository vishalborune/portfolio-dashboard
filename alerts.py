"""
alerts.py — the headless alert engine (Sprint 2).

Runs on GitHub Actions. Four modes:

  python alerts.py states       # flowchart state-change + volume alerts -> Telegram
                                # (hourly during market hours)
  python alerts.py fast-poll    # LIVE mainboard entry/add-zone poller (~1 min);
                                # loops ~16m, relaunched each market-hours cron
                                # tick. Args: [minutes] [interval_secs]
  python alerts.py eod-entries  # evening entry/add pass off EOD closes (SME +
                                # final mainboard) — runs after the 20:00 bhavcopy
  python alerts.py filings      # NSE/BSE corporate announcements -> Telegram
                                # (twice daily; best-effort, degrades gracefully)
  python alerts.py calendar     # this week's results dates -> Telegram (Mondays)
  python alerts.py digest       # Friday summary -> email

Shares signals.py with the dashboard: ONE flowchart engine, two consumers.

Env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY, TELEGRAM_BOT_TOKEN,
TELEGRAM_CHAT_ID, and for digest: RESEND_API_KEY, DIGEST_EMAILS.
"""

import os
import re
import sys
import gc
import time
import html
import hashlib
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from supabase import create_client

import signals
from notify import send_telegram, send_email, _dry_run as _dry
# _dry() is True when ALERTS_DRY_RUN is set. In dry-run NOTHING is sent AND no
# dedup/state rows are written — otherwise a test run would mark items "seen"
# and the next REAL run would silently skip them (read-only is the whole point).

# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------

class _NoOpQuery:
    """Swallows a write chain (.insert/.upsert(...).execute()) and returns an
    empty result — used only in dry-run so no dedup/state row is ever written."""
    def __getattr__(self, _):
        return lambda *a, **k: self
    def execute(self):
        return type("R", (), {"data": []})()


class _ReadOnlyTable:
    """Reads pass through to the real table; writes become no-ops (dry-run)."""
    def __init__(self, real):
        self._real = real
    def insert(self, *a, **k):
        return _NoOpQuery()
    def upsert(self, *a, **k):
        return _NoOpQuery()
    def update(self, *a, **k):
        return _NoOpQuery()
    def delete(self, *a, **k):
        return _NoOpQuery()
    def __getattr__(self, name):   # select, and anything else, hit the real one
        return getattr(self._real, name)


class _ReadOnlyClient:
    def __init__(self, real):
        self._real = real
    def table(self, name):
        return _ReadOnlyTable(self._real.table(name))
    def __getattr__(self, name):
        return getattr(self._real, name)


def sb():
    client = create_client(os.environ["SUPABASE_URL"],
                           os.environ["SUPABASE_SERVICE_KEY"])
    # In dry-run, hand back a read-only view so a test run can NEVER write a
    # dedup/state row (which would make the next real run skip that alert).
    return _ReadOnlyClient(client) if _dry() else client


def get_holdings(client) -> pd.DataFrame:
    res = client.table("holdings").select("*").execute()
    df = pd.DataFrame(res.data or [])
    if not df.empty and "portfolio_id" not in df.columns:
        df["portfolio_id"] = 1
    return df


# Portfolio -> owner group -> Telegram chat routing
PF_GROUP = {1: "vishal", 2: "lakshmi", 3: "lakshmi"}

# Which owner groups receive Telegram alerts (state changes + filings).
# Vishal opted out — Lakshmi is the TA lead and acts on alerts; Vishal's
# dashboard still shows all states, and his Sunday email digest continues.
# To re-enable Vishal's pings: add "vishal" back to this set.
TELEGRAM_ALERT_GROUPS = {"lakshmi"}
PF_NAME = {1: "Vishal", 2: "Lakshmi", 3: "Abinaya"}


def chat_id_for_group(group: str):
    # Only ONE Telegram group exists — the one already set up. It now carries
    # Lakshmi + Abinaya's alerts (see TELEGRAM_ALERT_GROUPS below for which
    # portfolios' data actually gets sent to it).
    import os as _os
    return _os.environ.get("TELEGRAM_CHAT_ID")


def extract_yf_ticker(name: str):
    m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(name))
    if not m:
        return None
    exch, sym = m.group(1), m.group(2).strip()
    return f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"


def short_name(name: str) -> str:
    n = re.sub(r"\s*\([^)]*\)\s*$", "", str(name))
    n = re.sub(r"\s+(LIMITED|LTD\.?|LTD)\s*$", "", n, flags=re.IGNORECASE)
    return n.strip().title()


# ---------------------------------------------------------------------------
# MODE: states — flowchart state-change detection
# ---------------------------------------------------------------------------

STATE_ICON = {
    "EXIT": "🔴", "BULLISH SIGNAL": "🟢", "WAIT/WATCH": "🔵",
    "BE CAUTIOUS": "🟠", "MOMENTUM FADING": "🟣", "MAINTAIN/ADD": "🟢",
    "INSUFFICIENT DATA": "⚪", "NO DATA": "⚪",
}
# Only these transitions are worth waking people up for
ALERT_WORTHY = {"EXIT", "BE CAUTIOUS", "MOMENTUM FADING", "BULLISH SIGNAL", "MAINTAIN/ADD"}


def vol_context(vol_ratio) -> str:
    if vol_ratio is None or pd.isna(vol_ratio):
        return ""
    if vol_ratio >= 2.0:
        return f"\nVolume: <b>{vol_ratio:.1f}x</b> 10-wk avg — heavy, institutions likely active"
    if vol_ratio >= 1.5:
        return f"\nVolume: <b>{vol_ratio:.1f}x</b> 10-wk avg — elevated"
    if vol_ratio <= 0.6:
        return f"\nVolume: {vol_ratio:.1f}x 10-wk avg — quiet (weak-hands move?)"
    return f"\nVolume: {vol_ratio:.1f}x 10-wk avg"


def check_watchlist_entries(client, price_fn=None):
    """Watchlist ENTRY alerts (added 17-Jul-2026) — the mirror image of the
    exit-side state alerts. Sweeps every portfolio's watchlist hourly:
    - ZONE alert when a stock touches the 10DMA (1st tranche) or 21DMA
      (2nd & final tranche) per Lakshmi's staged-entry system
    - TARGET alert when CMP reaches the stored target buy price
    Dedup: once per stock per group per day per kind (entry_alert_log).
    Entry math is bhavcopy-first (signals._fetch_daily), so SME watchlist names
    ARE covered now — off EOD closes in the evening pass, since they have no live
    feed (the old Yahoo-only 'SME skipped' limitation is gone, 21-Jul-2026)."""
    rows = client.table("watchlist").select("*").execute().data or []
    if not rows:
        return

    today_iso = date.today().isoformat()
    try:
        logged = client.table("entry_alert_log").select("ticker, grp, kind") \
            .eq("alert_date", today_iso).execute().data or []
        already = {(r["ticker"], r["grp"], r["kind"]) for r in logged}
    except Exception:
        already = set()

    # group watchlist rows per ticker: which groups watch it, min target
    by_ticker = {}
    for r in rows:
        m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(r.get("stock_name") or ""))
        if not m:
            continue
        exch, sym = m.group(1), m.group(2).strip()
        ticker = f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"
        grp = PF_GROUP.get(int(r.get("portfolio_id", 1)), "vishal")
        e = by_ticker.setdefault(ticker, {"name": short_name(r["stock_name"]),
                                          "groups": {}, })
        ge = e["groups"].setdefault(grp, {"pfs": [], "targets": []})
        pf = int(r.get("portfolio_id", 1))
        ge["pfs"].append(pf)
        t = r.get("target_buy_price")
        if t:
            # targets are PERSONAL: keep (pf, target) pairs, fire when ANY
            # member's target is hit. (A min() here was caught in testing
            # suppressing one member's legit alert when another's deeper
            # target hadn't been reached yet.)
            ge["targets"].append((pf, float(t)))

    msgs_by_group, to_log = {}, []
    for ticker, e in by_ticker.items():
        # price_fn (fast intraday poller) injects a LIVE-price zone; when absent
        # (EOD/hourly path) we classify off the latest daily close as before.
        d = price_fn(ticker) if price_fn is not None else signals.daily_entry_state(ticker)
        if not d:
            continue                       # SME in fast mode / fetch failure
        zone = d["Entry Zone"]
        cmp_ = d["CMP (d)"]
        for grp, ge in e["groups"].items():
            uniq = sorted(set(ge["pfs"]))
            tag = ""
            if grp == "lakshmi":
                tag = "[Both] " if len(uniq) > 1 else f"[{PF_NAME.get(uniq[0], uniq[0])}] "
            if zone in ("TRANCHE 1", "TRANCHE 2") and (ticker, grp, "ZONE") not in already:
                which = ("1st tranche (10DMA ₹{:,.2f})".format(d["10DMA"])
                         if zone == "TRANCHE 1"
                         else "2nd & FINAL tranche (21DMA ₹{:,.2f})".format(d["21DMA"]))
                msgs_by_group.setdefault(grp, []).append(
                    f"🎯 {tag}<b>{e['name']}</b> — entry zone reached\n"
                    f"CMP ₹{cmp_:,.2f} at the {which}")
                to_log.append((ticker, grp, "ZONE"))
            hits = [(pf, t) for pf, t in ge["targets"] if cmp_ <= t]
            if hits and (ticker, grp, "TARGET") not in already:
                whose = ", ".join(
                    f"₹{t:,.2f} ({PF_NAME.get(pf, pf)})" if grp == "lakshmi"
                    else f"₹{t:,.2f}" for pf, t in hits)
                msgs_by_group.setdefault(grp, []).append(
                    f"💰 {tag}<b>{e['name']}</b> — target buy price hit\n"
                    f"CMP ₹{cmp_:,.2f} ≤ target {whose}")
                to_log.append((ticker, grp, "TARGET"))

    sent = 0
    for grp, msgs in msgs_by_group.items():
        if grp not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(msgs)} entry alert(s) for '{grp}' — Telegram off for this group)")
            continue
        chat = chat_id_for_group(grp)
        if not chat:
            continue
        send_telegram("🛒 <b>Watchlist entry alerts</b>\n\n" + "\n\n".join(msgs),
                      chat_id=chat)
        sent += len(msgs)
    for ticker, grp, kind in to_log:
        try:
            client.table("entry_alert_log").upsert({
                "ticker": ticker, "grp": grp,
                "alert_date": today_iso, "kind": kind}).execute()
        except Exception as ex:
            print(f"⚠️ entry_alert_log write failed for {ticker}: {ex}")
    if sent:
        print(f"Sent {sent} watchlist entry alert(s).")
    else:
        print(f"No watchlist entry alerts. ({len(by_ticker)} watchlist tickers checked)")


def check_holding_adds(client, price_fn=None):
    """Portfolio ADD-zone alerts (added 21-Jul-2026, Lakshmi's request). For
    stocks we ALREADY HOLD, ping when the price pulls back to the 21-DMA — the
    final-tranche add level. (Lakshmi 21-Jul-2026: for holdings only the 21-DMA
    matters; the 10-DMA tranche-1 signal is a watchlist/entry concern, not an
    add-to-existing-position one.) The buy-side mirror of check_watchlist_entries.

    Dedup: once per stock/group/day via entry_alert_log, kind ADD21 (distinct
    from the watchlist 'ZONE' kind, so a stock that is both held and watchlisted
    doesn't cross-suppress). Daily EMA math is bhavcopy-first
    (signals._fetch_daily), so SME holdings are covered — the old Yahoo-only
    path skipped them silently."""
    holdings = get_holdings(client)
    if holdings.empty:
        return

    today_iso = date.today().isoformat()
    try:
        logged = client.table("entry_alert_log").select("ticker, grp, kind") \
            .eq("alert_date", today_iso).execute().data or []
        already = {(r["ticker"], r["grp"], r["kind"]) for r in logged}
    except Exception:
        already = set()

    # group holdings per ticker: which groups hold it (mirror watchlist scoping)
    by_ticker = {}
    for _, r in holdings.iterrows():
        ticker = extract_yf_ticker(r["stock_name"])
        if not ticker:
            continue
        pf = int(r.get("portfolio_id", 1))
        grp = PF_GROUP.get(pf, "vishal")
        e = by_ticker.setdefault(ticker, {"name": short_name(r["stock_name"]), "groups": {}})
        e["groups"].setdefault(grp, {"pfs": []})["pfs"].append(pf)

    msgs_by_group, to_log = {}, []
    for ticker, e in by_ticker.items():
        d = price_fn(ticker) if price_fn is not None else signals.daily_entry_state(ticker)
        if not d:
            continue                       # SME in fast mode / data unavailable
        zone, cmp_ = d["Entry Zone"], d["CMP (d)"]
        for grp, ge in e["groups"].items():
            uniq = sorted(set(ge["pfs"]))
            tag = ""
            if grp == "lakshmi":
                tag = "[Both] " if len(uniq) > 1 else f"[{PF_NAME.get(uniq[0], uniq[0])}] "
            if zone == "TRANCHE 2" and (ticker, grp, "ADD21") not in already:
                msgs_by_group.setdefault(grp, []).append(
                    f"➕ {tag}<b>{e['name']}</b> — add zone (holding)\n"
                    f"CMP ₹{cmp_:,.2f} at the FINAL-tranche 21DMA ₹{d['21DMA']:,.2f}")
                to_log.append((ticker, grp, "ADD21"))

    sent = 0
    for grp, msgs in msgs_by_group.items():
        if grp not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(msgs)} holding add-zone alert(s) for '{grp}' — Telegram off for this group)")
            continue
        chat = chat_id_for_group(grp)
        if not chat:
            continue
        send_telegram("➕ <b>Portfolio add-zone alerts</b>\n\n" + "\n\n".join(msgs), chat_id=chat)
        sent += len(msgs)
    for ticker, grp, kind in to_log:
        try:
            client.table("entry_alert_log").upsert({
                "ticker": ticker, "grp": grp,
                "alert_date": today_iso, "kind": kind}).execute()
        except Exception as ex:
            print(f"⚠️ entry_alert_log write failed for {ticker}: {ex}")
    if sent:
        print(f"Sent {sent} holding add-zone alert(s).")
    else:
        print(f"No holding add-zone alerts. ({len(by_ticker)} holdings checked)")


# ---------------------------------------------------------------------------
# RISK / STOP alerts (Lakshmi 21-Jul-2026) — the fast exit-side backstop the
# weekly flowchart can't give (it only re-evaluates weekly; a stock can crack
# intraday). Personalised to HIS entry, so it's ours to own.
# ---------------------------------------------------------------------------

STOP_FROM_COST = 0.10   # alert if a holding is >=10% below average cost (loss stop)
STOP_FROM_PEAK = 0.15   # ...or >=15% off its ~6-month peak (trailing stop; Lakshmi
                        # tightened 17%->15% on 23-Jul-2026 to exit pullbacks sooner)


def _grp_tag(grp, pfs):
    """'[Both] ' / '[Name] ' prefix for the lakshmi group; '' otherwise."""
    if grp != "lakshmi":
        return ""
    u = sorted(set(pfs))
    return "[Both] " if len(u) > 1 else f"[{PF_NAME.get(u[0], u[0])}] "


def check_risk_stops(client, prices: dict):
    """Alert when a HELD stock is >=10% below cost (loss stop, per each holder's
    OWN cost) or >=15% off its ~6-month peak (trailing stop). `prices` =
    {ticker: (cmp, peak)} — caller passes LIVE prices (fast poller, ~1 min) or
    EOD closes (evening). Dedup once/stock/group/day/kind via entry_alert_log
    (kinds STOP10 / PEAK17). Complements the flowchart EXIT, doesn't replace it."""
    holdings = get_holdings(client)
    if holdings.empty:
        return
    today_iso = date.today().isoformat()
    try:
        logged = client.table("entry_alert_log").select("ticker, grp, kind") \
            .eq("alert_date", today_iso).execute().data or []
        already = {(r["ticker"], r["grp"], r["kind"]) for r in logged}
    except Exception:
        already = set()

    by_ticker = {}
    for _, h in holdings.iterrows():
        t = extract_yf_ticker(h["stock_name"])
        if not t:
            continue
        pf = int(h.get("portfolio_id", 1))
        grp = PF_GROUP.get(pf, "vishal")
        try:
            cost = float(h.get("purchase_cost") or 0) or None
        except (TypeError, ValueError):
            cost = None
        e = by_ticker.setdefault(t, {"name": short_name(h["stock_name"]), "groups": {}})
        e["groups"].setdefault(grp, []).append((pf, cost))

    msgs_by_group, to_log = {}, []
    for ticker, e in by_ticker.items():
        cmp_, peak = prices.get(ticker, (None, None))
        if cmp_ is None:
            continue
        for grp, holders in e["groups"].items():
            # Loss stop — per holder's OWN cost (they may have bought at different
            # prices), fire if ANY member is >=10% underwater (mirrors targets).
            cost_hits = [(pf, c) for pf, c in holders if c and cmp_ <= c * (1 - STOP_FROM_COST)]
            if cost_hits and (ticker, grp, "STOP10") not in already:
                whose = ", ".join(
                    ((f"{PF_NAME.get(pf, pf)} " if grp == "lakshmi" else "")
                     + f"cost ₹{c:,.2f} ({(cmp_/c - 1)*100:+.0f}%)") for pf, c in cost_hits)
                msgs_by_group.setdefault(grp, []).append(
                    f"🛑 {_grp_tag(grp, [pf for pf, _ in cost_hits])}<b>{e['name']}</b> — loss stop\n"
                    f"CMP ₹{cmp_:,.2f}, ≥{int(STOP_FROM_COST*100)}% below {whose}")
                to_log.append((ticker, grp, "STOP10"))
            # Trailing stop — off the recent peak (price-based, same for all holders)
            # dedup key "PEAK17" is a STABLE historical string, independent of the
            # threshold value — don't rename it when the % changes (avoids re-alerts).
            if peak and cmp_ <= peak * (1 - STOP_FROM_PEAK) and (ticker, grp, "PEAK17") not in already:
                dd = (cmp_ / peak - 1) * 100
                msgs_by_group.setdefault(grp, []).append(
                    f"⛔ {_grp_tag(grp, [pf for pf, _ in holders])}<b>{e['name']}</b> — trailing stop\n"
                    f"CMP ₹{cmp_:,.2f} is {dd:+.0f}% from its ~6-mo peak ₹{peak:,.2f}")
                to_log.append((ticker, grp, "PEAK17"))

    sent = 0
    for grp, msgs in msgs_by_group.items():
        if grp not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(msgs)} risk alert(s) for '{grp}' — Telegram off)")
            continue
        chat = chat_id_for_group(grp)
        if not chat:
            continue
        send_telegram("🛑 <b>Risk / stop alerts</b>\n\n" + "\n\n".join(msgs), chat_id=chat)
        sent += len(msgs)
    for ticker, grp, kind in to_log:
        already.add((ticker, grp, kind))
        try:
            client.table("entry_alert_log").upsert({
                "ticker": ticker, "grp": grp, "alert_date": today_iso, "kind": kind}).execute()
        except Exception as ex:
            print(f"⚠️ entry_alert_log write failed for {ticker}: {ex}")
    if sent:
        print(f"Sent {sent} risk/stop alert(s).")


def check_wema_touch(client, prices: dict):
    """10-WEEK EMA touch alert (Lakshmi 22-Jul-2026). The weekly system's trend
    line: when a HELD stock comes back to its 10-week EMA that's an act-on level,
    and until now nothing told him — the weekly flowchart only re-evaluates on a
    state CHANGE, hourly. `prices` = {ticker: (cmp, day_low, wema10, prev_close)};
    caller passes LIVE prices (fast poller, ~1 min) or EOD closes (evening pass).

    A touch is an EVENT, not a state: price must be at the line NOW *and* have
    been clearly above it at the previous close. Measured live, plain proximity
    fired on 19 holdings in one day — the 10-week EMA is a slow mean-reversion
    line that stocks loiter around, and 19 alerts/day would just train him to
    ignore it. Requiring the approach keeps it to real arrivals.
    Dedup kind W10EMA, once/stock/group/day."""
    holdings = get_holdings(client)
    if holdings.empty:
        return
    today_iso = date.today().isoformat()
    try:
        logged = client.table("entry_alert_log").select("ticker, grp, kind") \
            .eq("alert_date", today_iso).execute().data or []
        already = {(r["ticker"], r["grp"], r["kind"]) for r in logged}
    except Exception:
        already = set()

    by_ticker = {}
    for _, h in holdings.iterrows():
        t = extract_yf_ticker(h["stock_name"])
        if not t:
            continue
        pf = int(h.get("portfolio_id", 1))
        e = by_ticker.setdefault(t, {"name": short_name(h["stock_name"]), "groups": {}})
        e["groups"].setdefault(PF_GROUP.get(pf, "vishal"), []).append(pf)

    msgs_by_group, to_log = {}, []
    for ticker, e in by_ticker.items():
        cmp_, day_low, wema, prev_close = prices.get(ticker, (None, None, None, None))
        if cmp_ is None or not wema:
            continue
        at_line = (day_low is not None and day_low <= wema) \
            or abs(cmp_ / wema - 1) <= signals.TOUCH_BAND
        # ...and it ARRIVED here — previous close was clearly above the line.
        came_down = prev_close is not None and prev_close > wema * (1 + signals.TOUCH_BAND)
        if not (at_line and came_down):
            continue
        for grp, pfs in e["groups"].items():
            if (ticker, grp, "W10EMA") in already:
                continue
            pct = (cmp_ / wema - 1) * 100
            msgs_by_group.setdefault(grp, []).append(
                f"📉 {_grp_tag(grp, pfs)}<b>{e['name']}</b> — at the 10-week EMA\n"
                f"CMP ₹{cmp_:,.2f} vs 10wEMA ₹{wema:,.2f} ({pct:+.1f}%)")
            to_log.append((ticker, grp, "W10EMA"))

    sent = 0
    for grp, msgs in msgs_by_group.items():
        if grp not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(msgs)} 10wEMA touch alert(s) for '{grp}' — Telegram off)")
            continue
        chat = chat_id_for_group(grp)
        if not chat:
            continue
        send_telegram("📉 <b>10-week EMA touch</b>\n\n" + "\n\n".join(msgs), chat_id=chat)
        sent += len(msgs)
    for ticker, grp, kind in to_log:
        already.add((ticker, grp, kind))
        try:
            client.table("entry_alert_log").upsert({
                "ticker": ticker, "grp": grp, "alert_date": today_iso, "kind": kind}).execute()
        except Exception as ex:
            print(f"⚠️ entry_alert_log write failed for {ticker}: {ex}")
    if sent:
        print(f"Sent {sent} 10-week EMA touch alert(s).")


JUMP_MIN = 0.01   # 5-EMA touch: CMP must close >=1% above the 5-EMA to count as a
                  # "jump" (not merely holding the line). Tuning knob — lower for
                  # more alerts, higher for only the strongest bounces.


def check_ema5_touch(client, prices: dict):
    """5-DAY EMA touch alert for WATCHLIST stocks (Lakshmi 02-Aug-2026). A fast-
    momentum timing signal — a watchlist name in an UPTREND pulls back to its
    5-day EMA and BOUNCES ('touching ema5 and jumping'). `prices` =
    {ticker: (cmp, day_low, ema5, prev_close, ema21)}; LIVE from the fast poller
    (~1 min) or EOD closes in the evening pass (covers SME watchlist too).

    An EVENT, not proximity. The 5-EMA is so fast that price hugs it every day, so
    a plain 'near it' alert would fire on nearly every trending stock daily (the
    exact lesson the 10-week EMA touch already taught). We require ALL of:
      • dipped   — the day's LOW reached the 5-EMA (it actually touched the line)
      • jumped   — CMP is clearly ABOVE the 5-EMA *and* up on the day (the bounce)
      • uptrend  — CMP above the 21-DMA (only meaningful in an uptrend)
    That captures both a pullback-to-5EMA bounce and a cross-up through it, but
    excludes stocks merely loitering on the line (no dip, or no jump) and anything
    not trending. Dedup kind EMA5, once/stock/group/day."""
    rows = client.table("watchlist").select("stock_name, portfolio_id").execute().data or []
    if not rows:
        return
    today_iso = date.today().isoformat()
    try:
        logged = client.table("entry_alert_log").select("ticker, grp, kind") \
            .eq("alert_date", today_iso).execute().data or []
        already = {(r["ticker"], r["grp"], r["kind"]) for r in logged}
    except Exception:
        already = set()

    by_ticker = {}
    for r in rows:
        m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(r.get("stock_name") or ""))
        if not m:
            continue
        exch, sym = m.group(1), m.group(2).strip()
        ticker = f"{sym}.NS" if exch == "XNSE" else f"{sym}.BO"
        grp = PF_GROUP.get(int(r.get("portfolio_id", 1)), "vishal")
        e = by_ticker.setdefault(ticker, {"name": short_name(r["stock_name"]), "groups": {}})
        e["groups"].setdefault(grp, []).append(int(r.get("portfolio_id", 1)))

    band = signals.TOUCH_BAND
    msgs_by_group, to_log = {}, []
    for ticker, e in by_ticker.items():
        cmp_, day_low, ema5, prev_close, ema21 = prices.get(ticker, (None, None, None, None, None))
        if cmp_ is None or not ema5:
            continue
        dipped = day_low is not None and day_low <= ema5 * (1 + band)         # touched the line
        # bounced DECISIVELY (>=1% above the 5-EMA) and up on the day — not just
        # holding the line (JUMP_MIN is the knob: lower = more alerts).
        jumped = cmp_ >= ema5 * (1 + JUMP_MIN) and (prev_close is None or cmp_ > prev_close)
        uptrend = ema21 is not None and cmp_ > ema21                          # in an uptrend
        if not (dipped and jumped and uptrend):
            continue
        for grp, pfs in e["groups"].items():
            if (ticker, grp, "EMA5") in already:
                continue
            pct = (cmp_ / ema5 - 1) * 100
            msgs_by_group.setdefault(grp, []).append(
                f"⚡ {_grp_tag(grp, pfs)}<b>{e['name']}</b> — bounced off the 5-day EMA (uptrend)\n"
                f"CMP ₹{cmp_:,.2f} vs 5EMA ₹{ema5:,.2f} ({pct:+.1f}%), above 21-DMA")
            to_log.append((ticker, grp, "EMA5"))

    sent = 0
    for grp, msgs in msgs_by_group.items():
        if grp not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(msgs)} 5EMA touch alert(s) for '{grp}' — Telegram off)")
            continue
        chat = chat_id_for_group(grp)
        if not chat:
            continue
        send_telegram("⚡ <b>5-day EMA touch · watchlist</b>\n\n" + "\n\n".join(msgs), chat_id=chat)
        sent += len(msgs)
    for ticker, grp, kind in to_log:
        try:
            client.table("entry_alert_log").upsert({
                "ticker": ticker, "grp": grp, "alert_date": today_iso, "kind": kind}).execute()
        except Exception as ex:
            print(f"⚠️ entry_alert_log write failed for {ticker}: {ex}")
    if sent:
        print(f"Sent {sent} 5-day EMA touch alert(s).")


# ---------------------------------------------------------------------------
# FAST INTRADAY ENTRY POLLING (mainboard) + EOD entry pass (all, incl SME)
# Lakshmi 21-Jul-2026: alert latency is the app's core value. Mainboard names
# get ~1-min live alerts; SME names (EOD-only data, and fine at day-end per
# Lakshmi) get one authoritative pass after bhavcopy. Both reuse the SAME
# proven check_* functions — the fast path just injects a live price.
# ---------------------------------------------------------------------------

def _sme_ticker_set() -> set:
    """Tickers that are SME/bhavcopy-priced (EOD only) — excluded from the
    live poller. Sourced from bhavcopy.SME_STOCKS, the single source of truth."""
    try:
        import bhavcopy
        return set(bhavcopy.SME_STOCKS.keys())
    except Exception as e:
        print(f"⚠️ could not load SME_STOCKS ({e}); treating all as mainboard")
        return set()


def _all_entry_tickers(client) -> set:
    """Every ticker we'd ever alert on: holdings + watchlist, both portfolios."""
    ts = set()
    h = get_holdings(client)
    for _, r in h.iterrows():
        t = extract_yf_ticker(r["stock_name"])
        if t:
            ts.add(t)
    try:
        rows = client.table("watchlist").select("stock_name").execute().data or []
    except Exception:
        rows = []
    for r in rows:
        t = extract_yf_ticker(r.get("stock_name"))
        if t:
            ts.add(t)
    return ts


def _live_quotes(tickers: list) -> dict:
    """{ticker: (last_price, day_low)} live from Yahoo for MAINBOARD names.
    Modest thread count (Render/Yahoo storm history, house rule #4). Failures
    log WHY and return (None, None) for that ticker — never a fake price."""
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor

    def one(t):
        try:
            fi = yf.Ticker(t).fast_info

            def g(key, attr):
                try:
                    v = fi[key]
                except Exception:
                    v = getattr(fi, attr, None)
                try:
                    v = float(v)
                    return v if v > 0 else None
                except (TypeError, ValueError):
                    return None
            return t, g("last_price", "last_price"), g("day_low", "day_low")
        except Exception as e:
            print(f"  [fast-poll] live quote failed for {t}: {type(e).__name__}: {e}")
            return t, None, None

    out = {}
    try:
        with ThreadPoolExecutor(max_workers=4) as ex:
            for t, lp, dl in ex.map(one, tickers):
                out[t] = (lp, dl)
    except Exception as e:
        print(f"⚠️ [fast-poll] live quote batch failed: {type(e).__name__}: {e}")
    return out


MAX_PLAUSIBLE_MOVE = 0.25   # Indian circuit limits are 5/10/20% — a live quote
                            # further than this from the last daily close is
                            # GARBAGE, not a move.


def _sane_quotes(quotes: dict, levels: dict) -> dict:
    """Reject live quotes that are implausibly far from the last daily close.

    WHY (22-Jul-2026): Yahoo returned Rs 498.65 for Kwality (539997.BO) while the
    stock was really ~Rs 2,689. That one bogus number fired a false loss stop AND
    a false trailing stop on a holding that is actually UP ~210% — precisely the
    'a wrong number is worse than a blank one' failure (house rule #2), on the
    Yahoo-on-BSE blind spot (#1). The dashboard already had this guard; the alert
    engine did not. Every rejection logs WHY (#3).

    A genuine split/bonus also trips this — going quiet on that ticker until the
    daily series catches up is the SAFE direction, and corporate_actions.py flags
    the gap separately."""
    out = {}
    for t, (lp, dl) in quotes.items():
        if lp is None:
            continue
        ref = (levels.get(t) or {}).get("ref_close")
        if ref and abs(lp / ref - 1) > MAX_PLAUSIBLE_MOVE:
            print(f"  [fast-poll] REJECTED implausible quote for {t}: live "
                  f"{lp:,.2f} vs last close {ref:,.2f} "
                  f"({(lp/ref - 1) * 100:+.0f}%) — skipping this ticker")
            continue
        out[t] = (lp, dl)
    return out


def _make_live_price_fn(levels: dict, quotes: dict):
    """Returns a price_fn(ticker) -> zone dict, classifying the LIVE price against
    the pre-computed daily EMA levels. None when we have no level or no live
    price (e.g. SME names — which the poller skips, leaving them to the EOD pass)."""
    def fn(ticker):
        lv = levels.get(ticker)
        lp, dl = quotes.get(ticker, (None, None))
        if not lv or lp is None:
            return None
        return signals.classify_entry_zone(
            ticker, lp, dl if dl is not None else lp, lv["ema10"], lv["ema21"])
    return fn


def run_eod_entries():
    """Evening pass (after the 20:00 bhavcopy): entry/add-zone alerts off the
    latest DAILY close for EVERY tracked name. This is the ONLY entry check for
    SME stocks (Lakshmi: SME at day-end is fine) and a final authoritative pass
    for mainboard. Reuses the proven check_* functions with price_fn=None."""
    client = sb()
    print("[eod-entries] evening entry/add pass (EOD closes)…")
    try:
        check_watchlist_entries(client)
    except Exception as e:
        print(f"⚠️ eod watchlist entries failed: {e}")
    try:
        check_holding_adds(client)
    except Exception as e:
        print(f"⚠️ eod holding adds failed: {e}")
    # 5-day EMA touch on the WATCHLIST off EOD closes (the only pass for SME
    # watchlist names; a final authoritative pass for mainboard).
    try:
        wl = client.table("watchlist").select("stock_name").execute().data or []
        ema5_prices, seen5 = {}, set()
        for r in wl:
            t = extract_yf_ticker(r.get("stock_name"))
            if not t or t in seen5:
                continue
            seen5.add(t)
            lv = signals.daily_entry_levels(t)
            if lv:
                ema5_prices[t] = (lv["ref_close"], lv["ref_low"], lv.get("ema5"),
                                  lv.get("prev_close"), lv.get("ema21"))
        check_ema5_touch(client, ema5_prices)
    except Exception as e:
        print(f"⚠️ eod 5EMA touch failed: {e}")
    # Risk stops on EOD closes — the ONLY risk pass for SME (no live feed) and a
    # daily backstop for mainboard. Levels/peak come from the same daily fetch.
    try:
        holdings = get_holdings(client)
        risk_prices, wema_prices, seen = {}, {}, set()
        for _, h in holdings.iterrows():
            t = extract_yf_ticker(h["stock_name"])
            if not t or t in seen:
                continue
            seen.add(t)
            lv = signals.daily_entry_levels(t)
            if lv:
                risk_prices[t] = (lv["ref_close"], lv.get("peak"))
                w = signals.weekly_ema10(t)
                if w:
                    wema_prices[t] = (lv["ref_close"], lv["ref_low"], w, lv.get("prev_close"))
        check_risk_stops(client, risk_prices)
        check_wema_touch(client, wema_prices)   # covers SME too (EOD closes)
    except Exception as e:
        print(f"⚠️ eod risk check failed: {e}")


def compute_fast_levels(client):
    """The once-per-day heavy part: 10/21-DMA + peak + 10-week EMA for every
    MAINBOARD ticker we hold or watch. Returns (levels, wema). Computing this
    ONCE (not per cycle) is what keeps the per-minute loop from re-downloading
    history and starting a Yahoo storm. SME names are excluded — they have no
    live feed and are covered by the evening pass."""
    sme = _sme_ticker_set()
    mainboard = sorted(t for t in _all_entry_tickers(client) if t not in sme)
    levels, wema, skipped = {}, {}, []
    for t in mainboard:
        lv = signals.daily_entry_levels(t)
        if lv:
            levels[t] = lv
        else:
            skipped.append(t)
        w = signals.weekly_ema10(t)
        if w:
            wema[t] = w
    if skipped:
        print(f"[levels] no daily levels for {len(skipped)}: {skipped} (skipped)")
    print(f"[levels] {len(levels)} mainboard tickers ready ({len(wema)} with a 10wEMA)")
    return levels, wema


def fast_cycle(client, levels: dict, wema: dict) -> int:
    """ONE live pass: fetch quotes, sanity-filter them, and run every live check
    (entry/add zones, risk stops, 10-week EMA touch). Shared by the GitHub
    fast-poll job AND the always-on Render worker so the two can never diverge.
    Returns how many tickers were priced this pass."""
    # Sanity-filter BEFORE anything acts on these prices — one bogus quote
    # otherwise fires false stop/entry alerts (see _sane_quotes).
    quotes = _sane_quotes(_live_quotes(list(levels)), levels)
    priced = sum(1 for t in levels if quotes.get(t, (None,))[0] is not None)
    fn = _make_live_price_fn(levels, quotes)
    check_holding_adds(client, price_fn=fn)
    check_watchlist_entries(client, price_fn=fn)
    risk_prices = {t: (quotes.get(t, (None,))[0], lv.get("peak"))
                   for t, lv in levels.items() if quotes.get(t, (None,))[0] is not None}
    check_risk_stops(client, risk_prices)
    wema_prices = {t: (quotes[t][0], quotes[t][1], w, levels.get(t, {}).get("prev_close"))
                   for t, w in wema.items() if quotes.get(t, (None,))[0] is not None}
    check_wema_touch(client, wema_prices)
    ema5_prices = {t: (quotes[t][0], quotes[t][1], lv.get("ema5"),
                       lv.get("prev_close"), lv.get("ema21"))
                   for t, lv in levels.items() if quotes.get(t, (None,))[0] is not None}
    check_ema5_touch(client, ema5_prices)
    return priced


def run_fast_poll(minutes: float = 16.0, interval: int = 60):
    """GitHub-job form of the live poller: compute levels, then cycle for
    ~`minutes` and exit (the cron relaunches it, so a crash self-heals).
    The Render worker runs the same fast_cycle() continuously instead."""
    import time as _time
    client = sb()
    levels, wema = compute_fast_levels(client)
    if not levels:
        print("[fast-poll] no levels computed — exiting.")
        return
    print(f"[fast-poll] watching {len(levels)} tickers every {interval}s for ~{minutes:.0f}m")
    end = _time.time() + minutes * 60
    cycle = 0
    while _time.time() < end:
        cycle += 1
        try:
            priced = fast_cycle(client, levels, wema)
            print(f"  [fast-poll] cycle {cycle}: {priced}/{len(levels)} priced")
        except Exception as e:
            print(f"⚠️ [fast-poll] cycle {cycle} failed: {type(e).__name__}: {e}")
        if _time.time() < end:
            _time.sleep(interval)
    print(f"[fast-poll] done after {cycle} cycle(s).")


def run_states():
    client = sb()
    holdings = get_holdings(client)
    if holdings.empty:
        print("No holdings.")
        return

    # Last known states, keyed (ticker, portfolio)
    prev_rows = client.table("alert_state").select("*").execute().data or []
    prev = {(r["ticker"], r.get("portfolio_id", 1)): r["state"] for r in prev_rows}

    # Compute each ticker ONCE, alert per portfolio that holds it
    state_cache = {}
    pending, changes_by_group, errors = {}, {}, 0
    vol_spikes = {}   # (group, ticker) -> spike info  (volume alerts, 15-Jul-2026)
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        if not ticker:
            continue
        pf = int(h.get("portfolio_id", 1))
        group = PF_GROUP.get(pf, "vishal")
        if ticker not in state_cache:
            try:
                state_cache[ticker] = signals.current_state(ticker)
            except Exception as e:
                errors += 1
                print(f"⚠️ {ticker}: {e}")
                continue
        d = state_cache.get(ticker)
        if not d:
            continue

        state = d["state"]
        old = prev.get((ticker, pf))
        if old != state and state in ALERT_WORTHY:
            # Aggregate per (group, ticker): one alert per stock per group,
            # even when multiple household portfolios hold it.
            key = (group, ticker)
            entry = pending.setdefault(key, {
                "name": short_name(h["stock_name"]), "d": d,
                "state": state, "pfs": [], "olds": [],
            })
            entry["pfs"].append(pf)
            entry["olds"].append(old)

        # --- Volume spike detection (requested by Lakshmi, 15-Jul-2026) ---
        # "Unusually high trading activity" alerts, ScoutQuest-style, from
        # data we already compute. vol_ratio = current week's volume vs the
        # 10-week average -- but early in the week the current bar only has
        # 1-2 days of volume, so we PACE-ADJUST: scale by 5/elapsed trading
        # days. A stock that's already traded 0.8x a full week's average by
        # Tuesday morning is pacing at 2x -- that's the signal. Threshold 2.0.
        vr = d.get("vol_ratio")
        if vr is not None and not pd.isna(vr) and d["state"] not in ("NO DATA", "INSUFFICIENT DATA"):
            elapsed = min(datetime.utcnow().weekday() + 1, 5)  # Mon=1 .. Fri=5
            pace = float(vr) * 5.0 / elapsed
            if pace >= 2.0:
                key = (group, ticker)
                if key not in vol_spikes:
                    vol_spikes[key] = {
                        "name": short_name(h["stock_name"]),
                        "pace": pace, "raw": float(vr),
                        "close": d.get("close"), "pfs": [],
                    }
                vol_spikes[key]["pfs"].append(pf)

        client.table("alert_state").upsert({
            "ticker": ticker, "portfolio_id": pf, "state": state,
            "reason": d.get("reason", ""),
            "updated_at": datetime.utcnow().isoformat(),
        }).execute()

    # Format one message per (group, ticker)
    for (group, ticker), e in pending.items():
        d, state = e["d"], e["state"]
        icon = STATE_ICON.get(state, "•")
        if group == "lakshmi":
            uniq_pfs = sorted(set(e["pfs"]))
            tag = "[Both] " if len(uniq_pfs) > 1 else f"[{PF_NAME.get(uniq_pfs[0], uniq_pfs[0])}] "
        else:
            tag = ""
        olds = {o for o in e["olds"] if o}
        was = f"\n(was {olds.pop()})" if len(olds) == 1 else ""
        msg = (f"{icon} {tag}<b>{e['name']}</b> → <b>{state}</b>"
               + was
               + f"\n{d.get('reason','')}"
               + vol_context(d.get("vol_ratio")))
        if state == "BULLISH SIGNAL":
            vr = d.get("vol_ratio")
            if vr is not None and not pd.isna(vr):
                if vr >= 1.5:
                    msg += "\n✅ Breakout volume-CONFIRMED — full size per rules"
                else:
                    msg += "\n⚠️ Breakout on weak volume — half size per rules"
        changes_by_group.setdefault(group, []).append(msg)

    sent = 0
    for group, changes in changes_by_group.items():
        if group not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(changes)} state change(s) for '{group}' — Telegram off for this group)")
            continue
        chat = chat_id_for_group(group)
        if not chat:
            print(f"⚠️ No Telegram chat configured for group '{group}' "
                  f"({len(changes)} alert(s) dropped)")
            continue
        header = f"📊 <b>State changes</b> · {date.today().strftime('%d %b %Y')}\n\n"
        send_telegram(header + "\n\n".join(changes), chat_id=chat)
        sent += len(changes)
    if sent:
        print(f"Sent {sent} state-change alert(s) across {len(changes_by_group)} group(s).")
    else:
        print(f"No state changes. ({len(holdings)} holdings checked, {errors} fetch errors)")

    # --- Volume spike dispatch (one alert per stock per group per day) ---
    if vol_spikes:
        today_iso = date.today().isoformat()
        try:
            logged = client.table("volume_alert_log").select("ticker, grp") \
                .eq("alert_date", today_iso).execute().data or []
            already = {(r["ticker"], r["grp"]) for r in logged}
        except Exception:
            already = set()

        spikes_by_group = {}
        for (group, ticker), s in vol_spikes.items():
            if (ticker, group) in already:
                continue
            if group == "lakshmi":
                uniq = sorted(set(s["pfs"]))
                tag = "[Both] " if len(uniq) > 1 else f"[{PF_NAME.get(uniq[0], uniq[0])}] "
            else:
                tag = ""
            price = f" · last ₹{s['close']:,.2f}" if s.get("close") else ""
            msg = (f"🔥 {tag}<b>{s['name']}</b> — unusually high trading activity\n"
                   f"Pacing at <b>{s['pace']:.1f}x</b> its 10-week average volume "
                   f"(this week already {s['raw']:.1f}x a full week's average){price}")
            spikes_by_group.setdefault(group, []).append((ticker, msg, s["pace"]))

        v_sent = 0
        for group, items in spikes_by_group.items():
            if group not in TELEGRAM_ALERT_GROUPS:
                print(f"({len(items)} volume spike(s) for '{group}' — Telegram off for this group)")
                continue
            chat = chat_id_for_group(group)
            if not chat:
                continue
            header = f"🔥 <b>Volume alerts</b> · {date.today().strftime('%d %b %Y')}\n\n"
            send_telegram(header + "\n\n".join(m for _, m, _ in items), chat_id=chat)
            for ticker, _, pace in items:
                try:
                    client.table("volume_alert_log").upsert({
                        "ticker": ticker, "grp": group,
                        "alert_date": today_iso, "pace_ratio": round(pace, 2),
                    }).execute()
                except Exception as e:
                    print(f"⚠️ volume_alert_log write failed for {ticker}: {e}")
            v_sent += len(items)
        if v_sent:
            print(f"Sent {v_sent} volume-spike alert(s).")

    # NOTE (21-Jul-2026): entry/add-zone sweeps moved OUT of this hourly job.
    # They now run (a) every ~1 min intraday for mainboard names via the
    # dedicated fast poller (`python alerts.py fast-poll`), and (b) once each
    # evening after bhavcopy for SME + a final EOD pass (`alerts.py eod-entries`).
    # This job stays focused on weekly state changes + volume spikes.


# ---------------------------------------------------------------------------
# MODE: filings — NSE/BSE corporate announcements (best-effort)
# ---------------------------------------------------------------------------

# --- Filing summarization (requested by Lakshmi, 17-Jul-2026) -------------
# ScoutQuest-style: don't just say a filing exists, say WHAT it says.
# Chain: download attachment PDF -> extract text -> Claude Haiku summary
# (2-4 bullets). Every step degrades gracefully: no API key, scanned PDF,
# download failure, API error -- all fall back to headline+link, never
# block the alert itself.

SUMMARY_MODEL = "claude-haiku-4-5-20251001"
MAX_SUMMARIES_PER_RUN = 10   # cost guard: beyond this, headline-only


MAX_PDF_BYTES = 15 * 1024 * 1024  # was 8 MB (SKIPPED scanned results ~9-12 MB, e.g.
                                  # Banswara's 9.5 MB), briefly 20 MB. Trimmed to 15 MB
                                  # 05-Aug-2026 after the 512 MB Render worker OOM-
                                  # restarted: base64 of a PDF is ~1.33x its size AND
                                  # requests copies it again into the API request body,
                                  # so a 20 MB PDF spiked ~54 MB per summary. 15 MB still
                                  # covers scanned results (base64 ≈ 20 MB) but caps the
                                  # spike; giant annual reports/decks stay headline-only.

# Browser UA for pulling NSE-archive filing PDFs. Its ABSENCE (undefined name)
# was NameError-ing inside _download_pdf_b64's bare except → every PDF returned
# None → EVERY filing went out headline-only, no summary ever (27-Jul-2026 bug).
# Same UA the RSS/announcement fetches use (nsearchives is friendly; UA suffices).
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _download_pdf_b64(url: str):
    """Filing PDF as base64, or None. Size-capped: giant documents (annual
    reports, investor decks) fall back to headline-only rather than burning
    tokens on 300 pages nobody asked to summarize."""
    try:
        import base64
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200 or not r.content[:5].startswith(b"%PDF"):
            return None
        if len(r.content) > MAX_PDF_BYTES:
            print(f"  [filings] PDF too large ({len(r.content)//1024} KB) — headline-only")
            return None
        return base64.standard_b64encode(r.content).decode()
    except Exception:
        return None


def _anthropic_pdf_call(api_key: str, pdf_b64: str, prompt: str, max_tokens: int) -> str:
    """POST a PDF + prompt to Claude, return the text response ('' on failure)."""
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                 "content-type": "application/json"},
        json={
            "model": SUMMARY_MODEL, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": [
                {"type": "document",
                 "source": {"type": "base64", "media_type": "application/pdf",
                            "data": pdf_b64}},
                {"type": "text", "text": prompt}]}],
        }, timeout=90)
    if r.status_code != 200:
        print(f"  [filings] summary API {r.status_code}: {r.text[:120]!r}")
        return ""
    blocks = r.json().get("content", [])
    return "\n".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()


RESULTS_HEADLINE_HINTS = (
    "financial result", "unaudited financial", "audited financial",
    "quarterly result", "outcome of board meeting", "statement of financial",
)


def _is_results_filing(headline: str) -> bool:
    """Loose classifier — false positives are harmless because the extraction
    step returns nothing for a non-results PDF and we fall back to a generic
    summary. Better to TRY the typed template than miss a real results filing."""
    h = (headline or "").lower()
    if any(k in h for k in RESULTS_HEADLINE_HINTS):
        return True
    return "result" in h and ("quarter" in h or "q1" in h or "q2" in h
                              or "q3" in h or "q4" in h or "fy" in h)


def summarize_filing(company: str, headline: str, pdf_url: str) -> str:
    """Gist of a filing via Claude reading the PDF NATIVELY (scanned or digital).
    RESULTS filings get Lakshmi's typed template (consolidated Revenue/EBITDA/
    PBT/PAT/EPS with QoQ & YoY %); everything else gets the generic bullet gist.
    '' on any failure -> the alert falls back to the headline."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  [filings] ANTHROPIC_API_KEY not set — filing goes out headline-only")
        return ""
    pdf_b64 = _download_pdf_b64(pdf_url)
    if not pdf_b64:
        print(f"  [filings] PDF fetch failed (headline-only): {pdf_url[:90]}")
        return ""
    try:
        if _is_results_filing(headline):
            typed = _summarize_results(company, pdf_b64, api_key)
            if typed:
                return typed          # else fall through to the generic summary
        return _summarize_generic(company, headline, pdf_b64, api_key)
    finally:
        # FREE the base64 (~1.33x the PDF) + let gc reclaim the transient request-body
        # copy PROMPTLY. Up to MAX_SUMMARIES_PER_RUN of these run per filings cycle on
        # the 512 MB Render worker; without this the big strings linger and the peak
        # tipped it into an OOM auto-restart (Render alert, 05-Aug-2026).
        del pdf_b64
        gc.collect()


def _summarize_generic(company: str, headline: str, pdf_b64: str, api_key: str) -> str:
    try:
        out = _anthropic_pdf_call(api_key, pdf_b64, max_tokens=350, prompt=(
            f"This is an Indian stock-exchange filing by {company} "
            f"(subject: {headline}). Read the ACTUAL document and summarize it so "
            f"an investor can decide whether it matters WITHOUT opening the PDF.\n"
            f"Output EXACTLY this, nothing else:\n"
            f"Line 1: one emoji + a 5-9 word plain headline of WHAT this filing is\n"
            f"Then 2-4 bullets starting with '- ', each under 18 words, ONLY "
            f"concrete facts from the document (amounts, dates, names, quantities, "
            f"percentages, board decisions).\n"
            f"Final line: 'Why it matters: ' + one under-20-word plain note on the "
            f"significance to a shareholder (e.g. equity dilution, one-off vs "
            f"recurring, governance red flag, growth trigger). Describe the "
            f"significance only; do NOT give buy/sell advice or price targets.\n"
            f"If the document is unreadable, output exactly: UNREADABLE"))
        if not out or out.upper().startswith("UNREADABLE"):
            return ""
        # Model prose (no intended HTML) — escape so a stray '&'/'<' can't 400 the
        # Telegram chunk (parse_mode=HTML).
        return html.escape(out[:900])
    except Exception as e:
        print(f"  [filings] generic summary failed for {company}: {e}")
        return ""


# --- Typed RESULTS template (Lakshmi, 21-Jul-2026) -------------------------
# Claude EXTRACTS the raw consolidated line items from the PDF (its strength);
# Python does ALL the arithmetic — EBITDA sum, QoQ/YoY %, unit->Cr — so a model
# arithmetic slip can't put a wrong number in front of a trading decision
# (house rule #2). EBITDA is Lakshmi's definition: PBT + Finance costs + Depn.

# Extract COLUMN-BY-COLUMN, not row-by-row (31-Jul-2026 fix): the old prompt
# asked the model to map each metric to the current/prev/year-ago column, and on
# wide statements (Clean Max has 4 columns incl. a 'Year ended') it silently
# drifted — pulling PBT/PAT from the PRECEDING quarter while revenue came from the
# current one (House Rule #2 wrong-number bug, Lakshmi caught it). Now the model
# transcribes each PERIOD COLUMN as one object (figures can't cross columns) and
# PYTHON assigns current/prev/year-ago by parsing the period-end dates.
_RESULTS_JSON_PROMPT = (
    "This is an Indian listed company's quarterly financial results filing.\n"
    "Use the CONSOLIDATED statement — the one including 'share of profit of "
    "associates/joint ventures' or headed 'Consolidated'. If ONLY standalone "
    "figures exist, use those and set basis='standalone'.\n"
    "Read the statement COLUMN BY COLUMN and return one object PER PERIOD COLUMN, "
    "so EVERY figure inside an object comes from the SAME column — this is "
    "critical, do NOT mix columns. Include EVERY column shown: current quarter, "
    "preceding quarter, year-ago quarter, AND any full-year / 'Year ended' column.\n"
    "For each column give period_end (the date exactly as printed) and is_quarter "
    "(true for a single ~3-month quarter, false for a full-year/'year ended' column).\n"
    "Numbers exactly as printed: strip commas; parentheses mean NEGATIVE; null if "
    "absent. Keep the statement's own unit. total_income = the 'Total income' "
    "line; total_expenses = the 'Total expenses' line; ebitda_reported = the "
    "column's own 'EBITDA' line if printed, else null. pbt='profit before tax' "
    "(after exceptional items if any); pat='profit for the period' after tax. "
    "exceptional_items = the 'Exceptional items' line if the statement has one, "
    "SIGNED so pbt = (profit before exceptional & tax) + exceptional_items: "
    "POSITIVE for an exceptional GAIN/income, NEGATIVE for a charge/expense; "
    "0 if there is no exceptional-items line.\n"
    "Return ONLY a JSON object — no prose, no markdown fences:\n"
    '{"basis":"consolidated|standalone|null","unit":"Lakhs|Crores|Millions|...",'
    '"columns":[{"period_end":"as printed","is_quarter":true,'
    '"revenue_from_operations":n,"total_income":n,"total_expenses":n,'
    '"finance_costs":n,"depreciation":n,"ebitda_reported":n,'
    '"pbt":n,"exceptional_items":n,"pat":n,"basic_eps":n}]}\n'
    "If this is NOT a quarterly results statement, return {\"basis\":null}."
)


def _parse_stmt_date(s):
    """Period-end date string -> (year, month, day) for sorting; None if unclear.
    Handles the many forms NSE statements use: '31-Mar-26', '30-Jun-2026',
    '30.06.2026', '30/06/2026' (day-first), 'June 30, 2026', '30 June 2026',
    ISO '2026-06-30'. 2-digit years -> 2000s."""
    s = str(s or "").strip()
    months = {m: i for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

    def y4(y):
        y = int(y)
        return y + 2000 if y < 100 else y
    try:
        # DD <sep> MonName <sep> YY(YY)   e.g. 31-Mar-26, 30 June 2026
        m = re.search(r"(\d{1,2})[-\s/.]+([A-Za-z]{3,})[-\s/.,]+(\d{2,4})", s)
        if m and m.group(2)[:3].lower() in months:
            return (y4(m.group(3)), months[m.group(2)[:3].lower()], int(m.group(1)))
        # MonName <sep> DD <sep> YYYY   e.g. 'Jun, 30 2026', 'June 30, 2026', 'Mar 31, 2026'.
        # MUST precede the day-optional pattern below: on 'Jun, 30 2026' that pattern's
        # day slot is empty (its separator class excluded the comma), so it took 30 as
        # the YEAR -> 2030, and 'Mar, 31 2026' -> 2031 then out-sorted the real current
        # column -> the Styrenix false alert (04-Aug-2026, Lakshmi caught it: it showed
        # the Mar quarter ₹826 Cr as 'current', not the true Jun quarter ₹1010 Cr).
        m = re.search(r"([A-Za-z]{3,})[\s,./-]+(\d{1,2})[\s,./-]+(\d{2,4})", s)
        if m and m.group(1)[:3].lower() in months:
            return (y4(m.group(3)), months[m.group(1)[:3].lower()], int(m.group(2)))
        # MonName [DD,] YYYY   e.g. Jun-2026 (no day)
        m = re.search(r"([A-Za-z]{3,})[-\s/.]*(\d{1,2})?[-\s/.,]+(\d{2,4})", s)
        if m and m.group(1)[:3].lower() in months:
            return (y4(m.group(3)), months[m.group(1)[:3].lower()], int(m.group(2) or 1))
        # ISO  YYYY-MM-DD
        m = re.match(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", s)
        if m:
            return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        # numeric day-first  DD-MM-YYYY / DD.MM.YYYY
        m = re.match(r"(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})", s)
        if m:
            return (y4(m.group(3)), int(m.group(2)), int(m.group(1)))
    except Exception:
        return None
    return None


def _summarize_results(company: str, pdf_b64: str, api_key: str) -> str:
    """Typed results summary, or '' to fall back to the generic gist. Assembles
    the current/prev-quarter/year-ago trios from the model's per-column transcript
    by DATE (so the period mapping is deterministic Python, not the model)."""
    try:
        # 2000 (was 900): the per-column JSON is bigger than the old flat schema
        # and 900 could TRUNCATE it mid-object -> unparseable -> silent fall-back
        # to the generic gist (seen in the 31-Jul audit). Headroom for ~4 columns.
        raw = _anthropic_pdf_call(api_key, pdf_b64, _RESULTS_JSON_PROMPT, max_tokens=2000)
        if not raw:
            return ""
        import json
        m = re.search(r"\{.*\}", raw, re.DOTALL)   # tolerate stray prose/fences
        if not m:
            return ""
        obj = json.loads(m.group(0))
        if not obj.get("basis"):
            return ""
        cols = obj.get("columns") or []
        for c in cols:
            c["_d"] = _parse_stmt_date(c.get("period_end"))
        cols = [c for c in cols if c["_d"]]
        if not cols:
            return ""

        def _n(c, k):
            v = c.get(k)
            return float(v) if isinstance(v, (int, float)) else None

        def _rev(c):
            # revenue as a tie-break key; missing -> huge so it's NOT mistaken for
            # the (smaller-revenue) single quarter when sharing a date.
            r = _n(c, "revenue_from_operations")
            return r if r is not None else 1e18

        # Pick periods by DATE + magnitude, NOT the model's is_quarter flag (it
        # gets Q4 vs full-year wrong — both end 31-Mar, Banswara 31-Jul-2026).
        # CURRENT = latest date; on a tie the SMALLER-revenue column (a single
        # quarter, not the YTD/full-year column sharing that date).
        maxd = max(c["_d"] for c in cols)
        cur = min([c for c in cols if c["_d"] == maxd], key=_rev)
        # Only compare against SINGLE-QUARTER columns: a period whose revenue is
        # >3x the current quarter is a full-year / YTD column, never a quarter
        # (Banswara's statement lists 'year ended' as the middle column, ~40x a
        # quarter — must not be read as the preceding quarter, 31-Jul-2026).
        cur_rev = _n(cur, "revenue_from_operations")

        def _is_qtr(c):
            r = _n(c, "revenue_from_operations")
            return cur_rev is None or r is None or r <= 3.0 * cur_rev

        before = [c for c in cols if c["_d"] < cur["_d"] and _is_qtr(c)]
        # YEAR-AGO quarter: same month, one year earlier.
        ya = [c for c in before if c["_d"][1] == cur["_d"][1] and c["_d"][0] == cur["_d"][0] - 1]
        year_ago = min(ya, key=_rev) if ya else {}
        # PRECEDING quarter: latest date before current that isn't the year-ago
        # date; smaller-revenue on a tie (picks Q4 over the same-dated full year).
        prev_q = {}
        other = sorted({c["_d"] for c in before if c["_d"] != year_ago.get("_d")}, reverse=True)
        if other:
            prev_q = min([c for c in before if c["_d"] == other[0]], key=_rev)

        # RE-ANCHOR PBT/PAT/EPS to the right column (31-Jul-2026). The model
        # reliably transcribes the TOP rows (revenue/finance/depreciation/EBITDA)
        # per column, but on wide statements it SWAPS the bottom rows (PBT/PAT/EPS)
        # between columns (Clean Max: current revenue paired with the PRECEDING
        # quarter's PBT/PAT). Those rows satisfy PBT ≈ reported-EBITDA − finance −
        # depreciation, so reassign each selected column the (pbt,pat,eps) whose
        # pbt matches its own identity — deterministically undoing any swap.
        # Idempotent if already correct; skipped when EBITDA isn't reported.
        # VERIFY + RE-ANCHOR the profit rows against each column's OWN arithmetic
        # (31-Jul-2026). The model reliably transcribes the top lines (revenue,
        # total income/expenses, finance, depreciation) but can misassign the
        # bottom rows (PBT/PAT/EPS) between columns on wide statements (Clean Max).
        # Every Ind-AS P&L satisfies PBT = TotalIncome − TotalExpenses (Schedule
        # III, expenses all-inclusive) OR − finance − depreciation (when those are
        # shown as separate lines, e.g. Clean Max). So per column compute the
        # target both ways, match each column its best-fitting (pbt,pat,eps), and
        # keep the identity with the smallest residual — deterministically undoing
        # any swap. If even the best fit leaves the CURRENT column badly off (no
        # reconciliation), fall back to the gist rather than emit a wrong number
        # (rule #2). If Total income/expenses weren't extracted, trust the model.
        selected = [c for c in (cur, prev_q, year_ago) if c]

        def _target(c, sub_findep):
            ti, te = _n(c, "total_income"), _n(c, "total_expenses")
            if ti is None or te is None:
                return None
            t = ti - te
            if sub_findep:
                f, d = _n(c, "finance_costs"), _n(c, "depreciation")
                if f is None or d is None:
                    return None
                t -= (f + d)
            return t

        pool = [(_n(c, "pbt"), _n(c, "pat"), _n(c, "basic_eps"), _n(c, "exceptional_items"))
                for c in selected]
        best_assign, best_resid = None, None
        if all(p[0] is not None for p in pool):
            for sub in (False, True):
                tgts = [_target(c, sub) for c in selected]
                if any(t is None for t in tgts):
                    continue
                used, assign, resid = [False] * len(pool), [None] * len(selected), 0.0
                for i, t in enumerate(tgts):
                    j = min((k for k in range(len(pool)) if not used[k]),
                            key=lambda k: abs(pool[k][0] - t))
                    used[j] = True
                    assign[i] = pool[j]
                    resid = max(resid, abs(pool[j][0] - t) / max(abs(t), 1.0))
                if best_resid is None or resid < best_resid:
                    best_resid, best_assign = resid, assign
        if best_assign is not None:
            # current column is index 0 in `selected`; require IT to reconcile.
            cur_t = min((abs(best_assign[0][0] - _target(selected[0], s))
                         for s in (False, True) if _target(selected[0], s) is not None),
                        default=None)
            cur_denom = max(abs(_n(selected[0], "total_income") or 0), abs(best_assign[0][0]), 1.0)
            if cur_t is not None and cur_t / cur_denom > 0.15:
                print(f"  [filings] results won't reconcile for {company} "
                      f"(resid {cur_t/cur_denom:.0%}) — falling back to gist")
                return ""
            for c, tup in zip(selected, best_assign):
                c["pbt"], c["pat"], c["basic_eps"], c["exceptional_items"] = tup

        def trio(metric):
            g = lambda c: (float(c[metric]) if isinstance(c.get(metric), (int, float)) else None)
            return {"current": g(cur), "prev_q": g(prev_q), "year_ago": g(year_ago)}

        data = {
            "basis": obj.get("basis"), "unit": obj.get("unit"),
            "period_current": cur.get("period_end"),
            "revenue_from_operations": trio("revenue_from_operations"),
            "finance_costs": trio("finance_costs"),
            "depreciation": trio("depreciation"),
            "pbt": trio("pbt"), "pat": trio("pat"), "basic_eps": trio("basic_eps"),
            "exceptional_items": trio("exceptional_items"),
            "ebitda_reported": trio("ebitda_reported"),
        }
        return _format_results(company, data)
    except Exception as e:
        print(f"  [filings] results extract failed for {company}: {e}")
        return ""


def _to_cr(v, unit):
    """Value -> ₹ Crore when the unit is recognised, else None (so we show the
    %s but never a wrong-magnitude absolute — rule #2)."""
    if v is None:
        return None
    u = (unit or "").lower()
    if "lakh" in u or "lac" in u:
        return v / 100.0
    if "million" in u:
        return v / 10.0
    if "crore" in u or "cr" in u:
        return float(v)
    return None


def _pct(cur, base):
    if cur is None or base is None or base == 0:
        return None
    return (cur - base) / abs(base) * 100.0


def _format_results(company: str, data: dict) -> str:
    basis = data.get("basis")
    if not basis:
        return ""                       # not a results statement -> fallback
    unit = data.get("unit")

    def trio(metric):
        d = data.get(metric) or {}
        g = lambda k: (float(d[k]) if isinstance(d.get(k), (int, float)) else None)
        return g("current"), g("prev_q"), g("year_ago")

    rev = trio("revenue_from_operations")
    fin = trio("finance_costs")
    dep = trio("depreciation")
    pbt = trio("pbt")
    pat = trio("pat")
    eps = trio("basic_eps")
    exc = trio("exceptional_items")     # signed: +gain / −charge (0/None if absent)
    # EBITDA per period = reported PBT + Finance + Depreciation − EXCEPTIONAL items.
    # Adding finance+depn back to PBT reconstructs EBITDA regardless of whether the
    # statement lists them inside or outside 'total expenses' (Lakshmi's definition);
    # subtracting the exceptional strips one-offs that would otherwise inflate a
    # period's EBITDA and every YoY/QoQ off it. (Advent Hotels 06-Aug-2026: a
    # ₹41.6 Cr year-ago exceptional GAIN in reported PBT faked an 84.7% year-ago
    # margin and a "-58% / margin pressure" read; real operating EBITDA was UP ~8%,
    # margin 32.8→35.3%.) exceptional defaults to 0 so the no-one-off case (the vast
    # majority) is exactly PBT+finance+depn as before.
    def _ebit(i):
        if pbt[i] is None or fin[i] is None or dep[i] is None:
            return None
        return pbt[i] + fin[i] + dep[i] - (exc[i] or 0.0)
    ebitda = tuple(_ebit(i) for i in range(3))

    # Guard: if we couldn't even read the current revenue AND PBT, it's not a
    # usable results table — fall back rather than emit a hollow template.
    if rev[0] is None and pbt[0] is None:
        return ""

    def line(label, triovals, as_eps=False):
        cur, pq, ya = triovals
        qoq, yoy = _pct(cur, pq), _pct(cur, ya)
        if cur is None and qoq is None and yoy is None:
            return None
        if as_eps:
            val = f"₹{cur:,.2f}" if cur is not None else "—"
        else:
            cr = _to_cr(cur, unit)
            val = f"₹{cr:,.1f} Cr" if cr is not None else "—"
        qs = f"{qoq:+.1f}%" if qoq is not None else "—"
        ys = f"{yoy:+.1f}%" if yoy is not None else "—"
        return f"• {label}: {val} (QoQ {qs} · YoY {ys})"

    # EBITDA margin = EBITDA / Revenue per period (Lakshmi 31-Jul-2026: margin
    # expansion is where operating leverage shows — a rising margin drops
    # straight to profit, and rising margin WITH rising sales is the combo he
    # hunts). Units cancel, so this is unit-independent. Shows the actual % for
    # all three periods (not a delta) so the trend reads at a glance.
    emar = tuple((ebitda[i] / rev[i] * 100)
                 if (ebitda[i] is not None and rev[i] not in (None, 0))
                 else None for i in range(3))

    def margin_line():
        if emar[0] is None:
            return None
        f = lambda x: f"{x:.1f}%" if x is not None else "—"
        return f"• EBITDA margin: {f(emar[0])} (prev Q {f(emar[1])} · YoY {f(emar[2])})"

    rows = [
        line("Revenue", rev),
        line("EBITDA", ebitda),
        margin_line(),
        line("PBT", pbt),
        line("PAT", pat),
        line("EPS", eps, as_eps=True),
    ]
    rows = [r for r in rows if r]
    if not rows:
        return ""
    # Escape the model-derived / free-text bits (company can contain '&' e.g.
    # 'Sathlokhar E&C'; period is straight from the model's JSON) — an unescaped
    # & or < 400s the whole Telegram chunk, and with write-after-send that filing
    # would then retry forever and burn summary calls. The static labels/tags stay.
    period = html.escape(str(data.get("period_current") or "latest quarter"))
    header = f"📊 <b>{html.escape(str(company))}</b> — {basis} results, {period}"
    # Plain-English takeaway, computed FROM THE NUMBERS (not the model — rule #2):
    # revenue vs PAT YoY tells the margin story at a glance, which is the "what
    # should I know" Lakshmi wants without re-reading the table.
    # Take leads with the EBITDA-MARGIN trend (Lakshmi's core signal): revenue
    # direction + margin change in bps YoY = his "sales up + margin up" check.
    # Falls back to the PAT-vs-revenue read when margin isn't computable.
    rev_yoy = _pct(rev[0], rev[2])
    take = None
    if rev_yoy is not None and emar[0] is not None and emar[2] is not None:
        bps = (emar[0] - emar[2]) * 100          # EBITDA-margin change YoY, in bps
        mv = f"EBITDA margin {emar[2]:.1f}→{emar[0]:.1f}% ({bps:+.0f} bps YoY)"
        if rev_yoy >= 0 and bps >= 0:
            take = f"revenue +{rev_yoy:.0f}% YoY AND {mv} — sales up + margin up 🔥"
        elif bps >= 0:
            take = f"{mv} on revenue {rev_yoy:+.0f}% — margin expanding"
        elif rev_yoy >= 0:
            take = f"revenue +{rev_yoy:.0f}% YoY but {mv} — margin pressure"
        else:
            take = f"revenue {rev_yoy:.0f}% YoY & {mv} — both weak"
    if take is None:
        pat_yoy = _pct(pat[0], pat[2])
        if rev_yoy is not None and pat_yoy is not None:
            if pat_yoy >= 0 and pat_yoy >= rev_yoy:
                take = f"PAT +{pat_yoy:.0f}% YoY outpacing revenue {rev_yoy:+.0f}% — margins expanding"
            elif rev_yoy >= 0 and pat_yoy < 0:
                take = f"revenue +{rev_yoy:.0f}% YoY but PAT {pat_yoy:.0f}% — margin squeeze"
            elif rev_yoy < 0 and pat_yoy < 0:
                take = f"revenue {rev_yoy:.0f}% & PAT {pat_yoy:.0f}% YoY — both contracting"
            else:
                take = f"revenue {rev_yoy:+.0f}% · PAT {pat_yoy:+.0f}% YoY"
    # Flag a MATERIAL exceptional item in any shown period (reported PBT vs the
    # operating profit above it). EBITDA/margin are already ex-exceptional, but the
    # reported PBT/PAT/EPS lines still carry it, so their YoY/QoQ can be badly
    # distorted by a one-off in the BASE period — say so rather than let the reader
    # misread it (Advent Hotels: year-ago one-off gain → reported PBT/PAT/EPS "fell"
    # 76-81% while the business was actually up). >3% of revenue = real one-off, not
    # rounding noise.
    exc_labels = ["Current quarter", "Preceding quarter", "Year-ago quarter"]
    exc_notes = []
    for i in range(3):
        if exc[i] and rev[i] and abs(exc[i]) > 0.03 * abs(rev[i]):
            cr = _to_cr(abs(exc[i]), unit)
            if cr is not None and cr >= 0.1:
                exc_notes.append(f"{exc_labels[i]} had a ₹{cr:,.1f} Cr exceptional "
                                 f"{'gain' if exc[i] > 0 else 'charge'}")
    warn = None
    if exc_notes:
        warn = ("⚠️ " + "; ".join(exc_notes)
                + " — EBITDA/margin above are operating (ex-exceptional); reported "
                "PBT/PAT/EPS YoY/QoQ are distorted by it.")

    footer = "<i>EBITDA = operating profit (ex-exceptional) + finance costs + depreciation</i>"
    parts = [header] + rows
    if warn:
        parts.append(html.escape(warn))
    if take:
        parts.append(f"<b>Take:</b> {html.escape(take)}")
    parts.append(footer)
    return "\n".join(parts)


MATERIAL_KEYWORDS = [
    "order", "contract", "dividend", "bonus", "split", "buyback", "results",
    "financial result", "acquisition", "pledge", "resignation", "appointment",
    "rating", "fund raise", "preferential", "rights issue", "expansion",
    # "board meeting" added 21-Jul-2026: results / dividends / splits / fund
    # raises are all DECIDED at board meetings, so both the "Board Meeting to be
    # held" notice and the "Outcome of Board Meeting" (which carries the actual
    # results) are material — and were being dropped by the keyword filter.
    "board meeting",
    # Insider/promoter activity via the announcement feed (21-Jul-2026): the
    # dedicated NSE PIT API returns empty/bot-blocked, but these disclosures ALSO
    # file as announcements. Precise terms only — NOT "pit" (matches 'caPITal').
    "insider", "encumbr", "acquisition of shares", "disposal of shares",
]

# ROUTINE = the ONLY things we suppress (23-Jul-2026). We used to alert only on
# MATERIAL_KEYWORDS, i.e. a whitelist — but a whitelist can only catch what we
# thought of in advance, and it silently dropped Solara's PRESS RELEASE and its
# "Change in Directors/KMP/Auditor" filing on 23-Jul (Lakshmi noticed the miss).
# Inverted: alert on everything EXCEPT obvious housekeeping. Missing a filing is
# the cardinal sin here; a little extra noise is not. MATERIAL_KEYWORDS still
# lives on — it now marks the high-signal ones with a ⭐ instead of gating them.
ROUTINE_KEYWORDS = [
    "trading window", "newspaper publication", "monitoring agency",
    "statement of deviation", "compliance certificate", "certificate under",
    "sdd compliance", "corporate governance report", "shareholding pattern",
    "reconciliation of share capital", "loss of share certificate",
    "duplicate share", "schedule of analyst", "schedule of meet",
    "non-applicability",
    "pursuant to exercise",      # routine ESOP allotments
]


def _fingerprint(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:32]


def _fetch_all_rows(client, table, columns, order=None, **eq):
    """Fetch ALL rows, paginating past Supabase's silent 1000-row cap. Use for
    tables we must read in FULL for a correct aggregate — transactions (XIRR),
    realised (FY P&L), trade_journal. Truncating these would produce a WRONG
    number, i.e. House Rule #1 causing a House Rule #2 outcome. (Contrast
    _load_seen, which only needs the newest 1000 fingerprints for dedup.)"""
    out, start, page = [], 0, 1000
    while True:
        q = client.table(table).select(columns)
        for k, v in eq.items():
            q = q.eq(k, v)
        if order:
            q = q.order(order)
        rows = q.range(start, start + page - 1).execute().data or []
        out += rows
        if len(rows) < page:
            return out
        start += page


def _load_seen(client) -> set:
    """Already-alerted fingerprints. filings_seen is the ONE table that grows
    without bound, so this MUST respect the 1000-row cap (House Rule #1): order
    by id DESC so the newest fingerprints are always returned and only ancient
    ones (which can't collide with a filing from the last 3 days) fall off."""
    try:
        rows = (client.table("filings_seen").select("fingerprint")
                .order("id", desc=True).limit(1000).execute().data or [])
        return {r["fingerprint"] for r in rows}
    except Exception as e:
        print(f"  (filings_seen load failed: {e})")
        return set()


# BSE's announcements API needs the NUMERIC scrip code -- these XBOM
# symbols aren't codes, so queries with them can never match ("No Record
# Found!", caught live 19-Jul-2026). Codes verified during the bhavcopy
# and fundamentals builds.
BSE_FILING_SCRIPS = {
    "CWD-MS": "543378",
    "HSIL-MT": "543916",
    "TRUECOLORS": "544531",
    "LEHAR": "532829",
    "SGRL": "540737",
    "TANFACIND": "506854",   # BSE-only (verified: not on NSE), watchlist name
}

# Dual-listed on NSE too — route THEIR filings through the WORKING NSE RSS feed
# (near real-time) instead of the dead BSE announcements API. NSE symbols verified
# against NSE's own master equity list, 04-Aug-2026.
BSE_TO_NSE = {
    "539997": "KPL",          # Kwality Pharmaceuticals
    "532856": "TIMETECHNO",   # Time Technoplast
}


def fetch_bse_announcements(scrip_code: str) -> list:
    """BSE announcements for one scrip. Returns list of dicts; [] on any failure.

    SUPERSEDED 04-Aug-2026 by fetch_bse_announcements_screener and NO LONGER
    CALLED by run_filings: BSE rebuilt this API behind an Akamai JS challenge that
    returns 'No Record Found!' to any plain HTTP client (proven even from real
    headless Chrome — 0 BSE filings had ever reached filings_seen). Kept for
    reference / the response shape. See the Screener workaround below."""
    scrip_code = BSE_FILING_SCRIPS.get(scrip_code, scrip_code)
    if not str(scrip_code).isdigit():
        print(f"  (BSE: no scrip code known for '{scrip_code}' — add to BSE_FILING_SCRIPS)")
        return []
    try:
        d_to = date.today().strftime("%Y%m%d")
        d_from = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
        url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
               f"?pageno=1&strCat=-1&strPrevDate={d_from}&strScrip={scrip_code}"
               f"&strSearch=P&strToDate={d_to}&strType=C")
        r = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bseindia.com/",
        })
        if r.status_code != 200:
            print(f"  (BSE HTTP {r.status_code} for {scrip_code}: {r.text[:100]!r})")
            return []
        payload = r.json()
        # Defensive: BSE sometimes returns a JSON *string* (block/error page)
        # instead of the expected dict -- the old code crashed with
        # "'str' object has no attribute 'get'" and hid what BSE actually
        # sent. Now the log shows the real payload so we can react.
        if not isinstance(payload, dict):
            if "no record" in str(payload).lower():
                return []          # BSE's way of saying "nothing filed" — normal
            print(f"  (BSE unexpected payload for {scrip_code}: "
                  f"{type(payload).__name__} = {str(payload)[:120]!r})")
            return []
        data = payload.get("Table") or []
        if not isinstance(data, list):
            print(f"  (BSE 'Table' not a list for {scrip_code}: {str(data)[:120]!r})")
            return []
        out = []
        for a in data[:10]:
            out.append({
                "headline": a.get("NEWSSUB") or a.get("HEADLINE") or "",
                "date": (a.get("NEWS_DT") or "")[:10],
                "url": f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{a.get('ATTACHMENTNAME')}"
                        if a.get("ATTACHMENTNAME") else "https://www.bseindia.com/corporates/ann.html",
            })
        return out
    except Exception as e:
        print(f"  (BSE fetch failed for {scrip_code}: {e})")
        return []


# --- BSE announcements via Screener.in (the workaround for BSE's dead API) -----
# BSE rebuilt its own announcements API behind an Akamai JS challenge that returns
# "No Record Found!" to any plain HTTP client — verified 04-Aug-2026 even from a
# REAL headless Chrome and every param variant (0 BSE filings had ever landed in
# filings_seen as a result). Screener.in aggregates the same BSE announcements,
# stays reachable from a datacenter IP (our fundamentals scraper already proves
# this), and ingests a filing within MINUTES (measured: a live BSE filing showed
# as '4m' on Screener). So an hourly poll here gives ~1h-latency BSE alerts.
# Returns the SAME {headline, date, url} shape as fetch_bse_announcements, so it
# drops straight into run_filings (dedup / routine-filter / AI summary / Telegram).
_SCREENER_HDRS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"}
_SCREENER_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def _screener_reltime_to_iso(txt: str) -> str:
    """Screener's announcement timestamp label -> ISO date 'YYYY-MM-DD'.
    It shows recent items relative ('36m','5h','2d') and older ones as a date
    ('22 Jul', '15 Jun 2025'). We need only day precision (the filings cutoff is
    coarse) AND stability across polls: 'Nm'/'Nh' -> today, 'Nd' -> today-N,
    'DD Mon[ YYYY]' -> that date — all of which resolve to the real filing DAY, so
    a later poll showing a different label still fingerprints to the same date.
    Empty string on anything unparseable (caller treats blank as 'no cutoff')."""
    t = (txt or "").strip().lower()
    today = date.today()
    try:
        m = re.fullmatch(r"(\d+)\s*(m|min|mins|h|hr|hrs|d)", t)
        if m:
            n, unit = int(m.group(1)), m.group(2)
            return ((today - timedelta(days=n)) if unit.startswith("d") else today).isoformat()
        m = re.fullmatch(r"(\d{1,2})\s+([a-z]{3})(?:\s+(\d{4}))?", t)
        if m:
            mon = _SCREENER_MONTHS.get(m.group(2))
            if mon:
                yr = int(m.group(3)) if m.group(3) else today.year
                iso = date(yr, mon, int(m.group(1)))
                if not m.group(3) and iso > today:      # no year & future -> last year
                    iso = date(yr - 1, mon, int(m.group(1)))
                return iso.isoformat()
    except Exception:
        pass
    return ""


def fetch_bse_announcements_screener(scrip_code: str) -> list:
    """BSE announcements for one scrip, scraped from its Screener.in page.
    [] on any failure (logged, House Rule #3). Polite: one throttled request per
    name, backs off on 429/403."""
    code = BSE_FILING_SCRIPS.get(scrip_code, scrip_code)
    time.sleep(1.5)                       # gentle guest — ~7 names per sweep
    try:
        r = requests.get(f"https://www.screener.in/company/{code}/",
                         timeout=25, headers=_SCREENER_HDRS)
        if r.status_code in (403, 429):
            print(f"  (Screener {r.status_code} for {code} — backing off this cycle)")
            return []
        if r.status_code != 200:
            print(f"  (Screener HTTP {r.status_code} for {code})")
            return []
        # SCOPE to the "Announcements" section only. The page also has "Annual
        # reports" and "Concalls" sections whose <li>s look identical (same PDF-link
        # shape) but are NOT filings — scraping them spammed old "Financial Year
        # 20XX" items (no date → leaked past the cutoff) and their huge PDFs blew
        # the summary token limit. The announcements div is <div class="documents
        # flex-column"><h3>Announcements</h3>…; cut at the next documents section.
        i = r.text.find(">Announcements</h3>")
        if i == -1:
            print(f"  (Screener: Announcements section not found for {code} — layout change?)")
            return []
        j = r.text.find('<div class="documents ', i + 10)
        block = r.text[i:j] if j != -1 else r.text[i:i + 12000]
        # each announcement: <li ...><a href="<bse pdf url>" ...>Headline
        #   <span|div class="ink-600 smaller">36m | DD Mon - desc</span|div></a></li>
        items = re.findall(
            r'<li[^>]*>\s*<a[^>]*href="(https://www\.bseindia\.com/[^"]*'
            r'(?:AnnPdfOpen|AttachLive|AttachHis)[^"]*)"[^>]*>(.*?)</a>\s*</li>',
            block, re.S)
        out = []
        for url, inner in items:
            # the meta is a <span>/<div class="ink-600 smaller"> holding either just
            # a relative time ("36m") or "DD Mon - <description>". Headline is the
            # anchor text BEFORE it; the leading token of the meta is the timestamp.
            meta_m = re.search(r'<(?:span|div)[^>]*ink-600[^>]*>(.*?)</(?:span|div)>', inner, re.S)
            meta = html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", meta_m.group(1)))).strip() if meta_m else ""
            head_html = inner[:meta_m.start()] if meta_m else inner
            headline = html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", head_html))).strip()
            tok = re.match(r"(\d+\s*(?:m|min|mins|h|hr|hrs|d)\b|\d{1,2}\s+[a-z]{3}(?:\s+\d{4})?)", meta, re.I)
            when = tok.group(1) if tok else ""
            if headline:
                out.append({"headline": headline,
                            "date": _screener_reltime_to_iso(when),
                            "url": html.unescape(url)})
        if not out:
            print(f"  (Screener: no announcements parsed for {code} — layout change?)")
        return out[:15]
    except Exception as e:
        print(f"  (Screener fetch failed for {code}: {e})")
        return []


_NSE_RSS_CACHE = None


def _parse_nse_date(pub: str) -> str:
    """NSE RSS pubDate -> 'YYYY-MM-DD'. The feed uses '21-Jul-2026 13:33:17',
    NOT RFC-2822 — so the old parsedate_to_datetime() failed on every item and
    left the date BLANK (which also silently disabled the recency filter).
    Try RFC-2822 first (in case NSE ever changes), then the real format."""
    if not pub:
        return ""
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(pub)
        if dt:
            return dt.date().isoformat()
    except Exception:
        pass
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y"):
        try:
            return datetime.strptime(pub, fmt).date().isoformat()
        except Exception:
            pass
    return ""


def _norm_name(s: str) -> str:
    """Squash a company name for fuzzy matching: drop Ltd/Limited/Pvt/The and all
    non-alphanumerics. 'South West Pinnacle Exploration Limited' -> 'southwestpinnacleexploration'."""
    s = re.sub(r"\b(limited|ltd|private|pvt|the)\b", "", (s or "").lower())
    return re.sub(r"[^a-z0-9]", "", s)


def fetch_nse_rss() -> list:
    """ALL recent NSE corporate announcements in one fetch, via the RSS feed
    on nsearchives.nseindia.com (rewritten 19-Jul-2026).

    WHY: the old per-symbol approach hit www.nseindia.com's API, which
    stonewalls datacenter IPs -- a manual run showed 60/60 read-timeouts,
    meaning the filings feed had silently died for NSE stocks. The archives
    host is the same one bhavcopy.py fetches from daily without issue.
    One request replaces sixty. Cached per process run."""
    global _NSE_RSS_CACHE
    if _NSE_RSS_CACHE is not None:
        return _NSE_RSS_CACHE

    # Fetch with a couple of retries — the feed is ~600 KB and occasionally
    # arrives truncated (seen live: "unclosed token" mid-stream). Because this
    # job only runs twice a day, a single bad download must NOT cost the whole
    # run, so we retry and, if the XML still won't parse, fall back to a lenient
    # per-<item> regex that recovers every COMPLETE item before the break.
    text = ""
    for attempt in (1, 2, 3):
        try:
            r = requests.get(
                "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                timeout=25)
            if r.status_code != 200:
                print(f"  (NSE RSS attempt {attempt}: HTTP {r.status_code})")
                continue
            text = r.text
            break
        except Exception as e:
            print(f"  (NSE RSS attempt {attempt} failed: {type(e).__name__}: {e})")
    if not text:
        print("  (NSE RSS: all attempts failed — no filings this run)")
        _NSE_RSS_CACHE = []
        return []

    items = _parse_rss_strict(text)
    if not items:
        items = _parse_rss_lenient(text)
        print(f"  (NSE RSS: strict parse failed, lenient recovered {len(items)} items)")
    else:
        print(f"  (NSE RSS: {len(items)} announcements fetched in one request)")
    _NSE_RSS_CACHE = items
    return items


def _rss_item(title, desc, link, pub) -> dict:
    """Build one feed item. The trading SYMBOL is NOT in the title (that's the
    company NAME); it's the prefix of the attachment filename in the link, e.g.
    .../corporate/SOUTHWEST_2107...pdf -> SOUTHWEST — the reliable match key."""
    msym = re.search(r"/corporate/([A-Za-z0-9&_-]+?)_\d", link or "")
    return {"title": (title or "").strip(), "desc": (desc or "").strip(),
            "url": (link or "").strip(), "date": _parse_nse_date((pub or "").strip()),
            "sym": msym.group(1).upper() if msym else ""}


def _parse_rss_strict(text: str) -> list:
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(text)
    except Exception as e:
        print(f"  (NSE RSS strict XML parse failed: {e})")
        return []
    return [_rss_item(it.findtext("title"), it.findtext("description"),
                      it.findtext("link"), it.findtext("pubDate"))
            for it in root.iter("item")]


def _parse_rss_lenient(text: str) -> list:
    """Regex-recover complete <item>…</item> blocks — survives a truncated tail."""
    out = []
    def field(block, tag):
        m = re.search(rf"<{tag}>(.*?)</{tag}>", block, re.DOTALL | re.IGNORECASE)
        return re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", m.group(1), flags=re.DOTALL).strip() if m else ""
    for block in re.findall(r"<item>(.*?)</item>", text, re.DOTALL | re.IGNORECASE):
        out.append(_rss_item(field(block, "title"), field(block, "description"),
                             field(block, "link"), field(block, "pubDate")))
    return out


def fetch_nse_announcements(symbol: str, company_name: str = None) -> list:
    """Announcements for one NSE stock from the shared RSS feed.

    Rewritten 21-Jul-2026 after confirming the bug against the LIVE feed: the
    feed's TITLE is the company NAME ('South West Pinnacle Exploration Limited'),
    not the symbol, so the old '^SYMBOL' title match returned ZERO hits for such
    stocks — their filings were silently never alerted. Now we match on the
    SYMBOL parsed from each item's attachment link (exact, case-insensitive),
    with normalised company-name containment as a fallback for the rare item
    whose link has no parseable symbol."""
    sym_u = (symbol or "").upper()
    cnorm = _norm_name(company_name) if company_name else ""
    out = []
    for it in fetch_nse_rss():
        # Match on EITHER exact signal — never a substring. Substring matching
        # (an earlier attempt) false-fired badly: 'EMS' matched inside 'R
        # Systems', 'ZF ... Systems' etc., attributing other companies' filings
        # to the wrong stock (portfolio-wide audit, 21-Jul-2026). Both signals
        # are needed because NSE's filing-link token is often NOT the trading
        # symbol (NEWGEN->NEWGEN2, CENTENKA->CENTURYENKA), so the exact
        # company-name match is the reliable anchor; the link-symbol is a
        # second exact chance when the stored name differs from NSE's.
        matched = (bool(it.get("sym")) and it["sym"] == sym_u) \
            or (bool(cnorm) and _norm_name(it["title"]) == cnorm)
        if not matched:
            continue
        out.append({
            "headline": (it["desc"] or it["title"])[:500],
            "date": it["date"],
            "url": it["url"] or "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
        })
        if len(out) >= 10:
            break
    return out


_XBRL_REDFLAG_TYPES = {"resignation", "cessation", "removal", "disqualification",
                       "death", "vacation of office"}


def _fmt_date(s):
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").strftime("%d-%b-%Y")
    except Exception:
        return str(s)


def _summarize_xbrl(url: str) -> str:
    """Summarize a STRUCTURED NSE XBRL filing by parsing its fields — no PDF,
    no LLM, no token cost. Many material NSE filings (Change in Directors/KMP/
    Auditor etc.) are XBRL-only with a vague generic headline; the machine-
    readable XML carries the actual who/what. Only SOME XBRL types are publicly
    fetchable (Change-in-Management is; others 404) — returns '' on 404 or an
    unrecognised type, so the caller falls back to headline + NSE-page link."""
    if not url.lower().endswith(".xml"):
        return ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200 or b"<" not in r.content[:50]:
            return ""
        import xml.etree.ElementTree as ET
        root = ET.fromstring(r.content)
    except Exception as e:
        print(f"  [filings] XBRL parse failed for {url[:70]}: {type(e).__name__}")
        return ""
    tags = {re.sub(r"\{.*\}", "", el.tag) for el in root.iter()}
    if "TypeOfChange" in tags or "ChangeInManagementDomain" in tags:
        return _summarize_xbrl_cim(root)
    if "NameOfTheTargetEntity" in tags or \
            "TypeOfEventOfAnnouncementPertainingToRegulation30Restructuring" in tags:
        return _summarize_xbrl_reg30(root)
    return ""                       # unknown XBRL type -> headline-only


def _summarize_xbrl_cim(root) -> str:
    """Format a 'Change in Management' (directors/KMP/auditor/RTA) XBRL into a
    Telegram gist: WHO, appointment vs exit, effective date — and a 🚨 flag when
    an AUDITOR or DIRECTOR is resigning/removed (a classic smallcap red flag)."""
    from collections import defaultdict
    vals = defaultdict(list)
    for el in root.iter():
        tag = re.sub(r"\{.*\}", "", el.tag)
        txt = (el.text or "").strip()
        if txt:
            vals[tag].append(txt)
    names = vals.get("NameOfDesignatedPerson", [])
    if not names:
        return ""
    types, cats = vals.get("TypeOfChange", []), vals.get("CategoryForChange", [])
    desigs, sals = vals.get("DesignationOfDesignatedPerson", []), vals.get("SalutationOfDesignatedPerson", [])
    effs = vals.get("EffectiveDateOfReasonOfChange", [])
    lines, redflag = [], False
    for i, nm in enumerate(names):
        typ = types[i] if i < len(types) else ""
        cat = cats[i] if i < len(cats) else ""
        des = desigs[i] if i < len(desigs) else ""
        sal = sals[i] if i < len(sals) else ""
        eff = _fmt_date(effs[i]) if i < len(effs) else ""
        if typ.lower() in _XBRL_REDFLAG_TYPES and ("auditor" in cat.lower() or "director" in cat.lower()):
            redflag = True
        who = (f"{sal} " if sal else "") + nm
        seg = typ + (f" · {cat}" if cat else "")
        tail = (f" — {des}" if des else "") + (f" (eff {eff})" if eff else "")
        lines.append(f"- {seg}: {who}{tail}")
    head = f"👤 {len(names)} management change{'s' if len(names) != 1 else ''}"
    _benign = {"appointment", "re-appointment", "reappointment"}
    if redflag:
        why = "🚨 auditor/director exit — governance red flag, review"
    elif types and all(t.lower() in _benign for t in types):
        why = "routine appointment(s), no exits"
    else:
        why = "board/management change — check who & why"
    return html.escape("\n".join([head] + lines)) + f"\n<b>Why it matters:</b> {html.escape(why)}"


def _summarize_xbrl_reg30(root) -> str:
    """Format a Reg-30 restructuring XBRL — the common case is an ACQUISITION.
    Pulls target / consideration / rationale / target financials / timeline, and
    🚨-flags a RELATED-PARTY deal. Rupee amounts are raw INR → shown in ₹ crore.
    Returns '' if it isn't an acquisition-shaped filing (→ headline fallback)."""
    v = {}
    for el in root.iter():
        tag = re.sub(r"\{.*\}", "", el.tag)
        txt = (el.text or "").strip()
        if txt and tag not in v:                 # keep first occurrence
            v[tag] = txt

    def like(prefix):                            # the objects tag is enormous
        for k, val in v.items():
            if k.startswith(prefix):
                return val
        return None

    def cr(x):
        try:
            return float(str(x).replace(",", "")) / 1e7
        except (TypeError, ValueError):
            return None

    target = v.get("NameOfTheTargetEntity")
    if not target:
        return ""                                # not an acquisition-shaped Reg-30
    ev = (v.get("TypeOfEventOfAnnouncementPertainingToRegulation30Restructuring")
          or "Restructuring").split("(")[0].strip()
    industry = v.get("IndustryToWhichTheEntityBeingAcquiredBelongs")
    nature = v.get("NatureOfConsiderationForAcquisitionEvent")
    cost = cr(v.get("CostOfAcquisitionOrThePriceAtWhichTheSharesAreAcquired")
              or v.get("AmountOfCashConsiderationForAcquisitionEvent"))
    objects = like("ObjectsAndImpact")
    timeline = v.get("IndicativeTimePeriodForCompletionOfTheAcquisition")
    related = v.get("WhetherTheAcquisitionWouldFallWithinRelatedPartyTransactions", "").lower() == "true"
    arms = v.get("WhetherAcquisitionEventIsDoneAtArmsLength", "").lower() == "true"
    to, pat, nw = (cr(v.get("TurnoverOfTargetEntity")),
                   cr(v.get("ProfitAfterTaxOfTargetEntity")),
                   cr(v.get("NetWorthOfTargetEntity")))

    head = f"🤝 {ev} — {target}" + (f" ({industry})" if industry else "")
    lines = []
    c = []
    if nature:
        c.append(f"{nature.lower()} consideration")
    if cost is not None:
        c.append(f"₹{cost:,.1f} cr")
    c.append("related-party" if related else "not related party")
    if arms:
        c.append("arm’s length")
    lines.append("- " + " · ".join(c))
    if objects:
        lines.append("- Purpose: " + objects[:140].rstrip())
    tf = [s for s, x in (("turnover", to), ("PAT", pat), ("net worth", nw)) if x is not None]
    if tf:
        lines.append("- Target: " + " · ".join(
            f"{lbl} ₹{val:,.1f} cr" for lbl, val in
            (("turnover", to), ("PAT", pat), ("net worth", nw)) if val is not None))
    if timeline:
        lines.append("- " + timeline[:120].rstrip())
    why = ("🚨 RELATED-PARTY deal — scrutinise" if related
           else "M&A / expansion — weigh size & rationale vs the company")
    return html.escape("\n".join([head] + lines)) + f"\n<b>Why it matters:</b> {html.escape(why)}"


def _filing_link(url: str, exch: str, sym: str) -> str:
    """Clickable link for a filing. A real PDF → the document itself. But NSE's
    XBRL 'WebXMLFile' attachments (board-meeting prior intimations etc.) are NOT
    publicly downloadable — that link 404s (Lakshmi hit exactly this 31-Jul-2026)
    — so for any non-PDF NSE filing we point at the stock's NSE page instead of
    emitting a dead link. BSE keeps its own attachment URL."""
    url = (url or "").strip()
    esc = html.escape
    if url.lower().endswith(".pdf"):
        return f'<a href="{esc(url, quote=True)}">filing ↗</a>'
    if exch == "XNSE" and sym:
        return (f'<a href="https://www.nseindia.com/get-quotes/equity?'
                f'symbol={esc(sym, quote=True)}">NSE page ↗</a>')
    if url:
        return f'<a href="{esc(url, quote=True)}">filing ↗</a>'
    return ('<a href="https://www.nseindia.com/companies-listing/'
            'corporate-filings-announcements">NSE filings ↗</a>')


_BM_DATE_RE = re.compile(
    r"(?:to be held on|will be held on|convened on|held on|scheduled (?:on|for))\s+"
    r"([0-9]{1,2}(?:st|nd|rd|th)?[-./ ][A-Za-z0-9]{2,9}[-./, ]+[0-9]{2,4}"
    r"|[A-Za-z]{3,9}\s+[0-9]{1,2},?\s*[0-9]{4})", re.I)
_BM_PURPOSE = [("result", "results"), ("dividend", "dividend"), ("fund rais", "fund-raise"),
               ("fund-rais", "fund-raise"), ("buy back", "buyback"), ("buyback", "buyback"),
               ("bonus", "bonus"), ("stock split", "split"), ("sub-division", "split"),
               ("preferential", "preferential"), ("rights issue", "rights issue")]


def _parse_board_meeting(headline):
    """If `headline` is a board-meeting PRIOR INTIMATION, return (event_date_iso,
    purpose) for a TODAY-or-future meeting; else None. Feeds the morning 'today's
    agenda' brief (Lakshmi 02-Aug-2026)."""
    h = headline or ""
    if "board meeting" not in h.lower():
        return None
    m = _BM_DATE_RE.search(h)
    if not m:
        return None
    d = _parse_stmt_date(re.sub(r"(st|nd|rd|th)", "", m.group(1)))
    if not d:
        return None
    try:
        ev = date(d[0], d[1], d[2])
    except Exception:
        return None
    if ev < date.today():
        return None                       # a past meeting (an outcome, or stale)
    hl = h.lower()
    tags = []
    for k, lbl in _BM_PURPOSE:
        if k in hl and lbl not in tags:
            tags.append(lbl)
    return ev.isoformat(), (", ".join(tags) or "board meeting")


def _capture_event(client, ticker, headline, source_date):
    """Store a board-meeting intimation in scheduled_events (idempotent upsert).
    Never raises — a missing table or a non-intimation just means no capture."""
    ev = _parse_board_meeting(headline)
    if not ev:
        return
    try:
        client.table("scheduled_events").upsert({
            "ticker": ticker, "event_date": ev[0], "event_type": "board_meeting",
            "purpose": ev[1], "headline": (headline or "")[:300],
            "source_date": source_date or None},
            on_conflict="ticker,event_date,event_type").execute()
    except Exception:
        pass


def run_filings(nse_only: bool = False):
    """Exchange-announcement alerts for stocks we HOLD or WATCH. nse_only=True
    skips the BSE per-scrip fetch (used by the 3-min fast path so only NSE's
    friendly host is polled; BSE rides the 2-hourly full run to protect its
    bot-hostile API, which shares the runner IP with the daily SME bhavcopy).

    Scoping (rewritten 23-Jul-2026): ONE entry per symbol, unioning EVERY holder
    AND watcher group. The old code built the audience from the first matching
    holdings row only and skipped the rest, so a stock held by two people under
    even a slightly different name string ("Ltd" vs "Limited") alerted only
    whoever's row came first — silently dropping the other's filing and writing
    the seen-fingerprint so it never fired again (the real Solara/Kwality miss).
    Watchlist is now covered the same way (Lakshmi, 23-Jul-2026).

    Delivery: a fingerprint is written to filings_seen ONLY after its Telegram
    send actually succeeds — previously it was written mid-loop before dispatch,
    so any failed/opted-out send meant permanent loss. Headlines/URLs are
    HTML-escaped (parse_mode=HTML) so an '&' in a name like 'E&C' can't 400 the
    whole batch."""
    client = sb()
    holdings = get_holdings(client)
    try:
        watch = client.table("watchlist").select("stock_name, portfolio_id").execute().data or []
    except Exception:
        watch = []

    scope = {}   # sym -> {exch, company, name, groups:set}
    def _add(nm, pf):
        m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(nm))
        if not m:
            return
        exch, sym = m.group(1), m.group(2).strip()
        e = scope.setdefault(sym, {
            "exch": exch,
            "company": re.sub(r"\s*\(X(?:NSE|BOM):[^)]+\)\s*$", "", str(nm)).strip(),
            "name": short_name(nm), "groups": set()})
        e["groups"].add(PF_GROUP.get(int(pf or 1), "vishal"))
    for _, h in holdings.iterrows():
        _add(h["stock_name"], h.get("portfolio_id", 1))
    for r in watch:
        _add(r.get("stock_name"), r.get("portfolio_id", 1))
    if not scope:
        return

    seen = _load_seen(client)
    esc = html.escape
    cutoff = (date.today() - timedelta(days=3)).isoformat()
    alerts_by_group = {}      # group -> [(fps, body)]  (fps = 1+ fingerprints)
    fp_meta = {}              # fp -> (sym, headline, date)
    no_audience = set()       # matched, but no Telegram-enabled group wants it
    summaries_done = 0

    for sym, e in scope.items():
        nse_equiv = BSE_TO_NSE.get(sym)
        if e["exch"] == "XNSE":
            anns = fetch_nse_announcements(sym, e["company"])
        elif nse_equiv:
            # dual-listed BSE name → ride the WORKING NSE RSS feed (near real-time)
            anns = fetch_nse_announcements(nse_equiv, e["company"])
        else:
            # BSE-only → Screener (BSE's own announcements API is dead: Akamai).
            # This is the slower source, so it rides the full run, not the 3-min
            # NSE burst — the nse_only fast path skips it.
            if nse_only:
                continue
            anns = fetch_bse_announcements_screener(sym)
        enabled = [g for g in e["groups"] if g in TELEGRAM_ALERT_GROUPS]
        for a in anns:
            if not a["headline"] or (a["date"] and a["date"] < cutoff):
                continue
            hl = a["headline"].lower()
            if any(k in hl for k in ROUTINE_KEYWORDS):
                continue                      # housekeeping — the ONLY thing dropped
            fp = _fingerprint(sym, a["headline"], a["date"])
            starred = any(k in hl for k in MATERIAL_KEYWORDS)
            url = a["url"] or ""
            is_pdf = url.lower().endswith(".pdf")
            is_xml = url.lower().endswith(".xml")
            fp_sum = _fingerprint(sym, a["headline"], a["date"], "xbrlsummary")

            # Headline ALREADY sent. For XBRL, NSE often publishes the machine
            # file minutes-to-hours LATER, so retry the parse each poll and send a
            # ONE-TIME "details" follow-up once it's live (Lakshmi 31-Jul-2026:
            # alert now, summary follows). fp_sum (a 2nd fingerprint) dedups the
            # follow-up — no schema change. Non-XBRL just skips, as before.
            if fp in seen or fp in fp_meta:
                if is_xml and enabled and fp_sum not in seen and fp_sum not in fp_meta:
                    gist = _summarize_xbrl(url)
                    if gist:
                        # 🔁 = clearly a FOLLOW-UP to a headline already sent; echo
                        # the original headline so the two are unmistakably linked.
                        body = (f"🔁 <b>{esc(e['name'])}</b> — filing update "
                                f"(summary now available)\n"
                                f"<i>↳ earlier: {esc(a['headline'][:160])}</i>\n\n{gist}"
                                f"\n{esc(a['date'] or '')} · {_filing_link(url, e['exch'], sym)}")
                        fp_meta[fp_sum] = (sym, a["headline"] + " [xbrl summary]", a["date"])
                        for g in enabled:
                            alerts_by_group.setdefault(g, []).append(([fp_sum], body))
                continue

            # FIRST sighting: send the headline now. PDF -> summarize via Claude
            # (capped); XBRL -> parse now if already published (else '' and the
            # follow-up above catches it once NSE posts the file).
            gist = ""
            if enabled and is_pdf and summaries_done < MAX_SUMMARIES_PER_RUN:
                gist = summarize_filing(e["name"], a["headline"], url)
                if gist:
                    summaries_done += 1
            elif enabled and is_xml:
                gist = _summarize_xbrl(url)
            # Full headline (was double-truncated to 200 → cut mid-word); for a
            # filing we can't summarize, the headline IS the payload. Link never
            # dead: non-PDF NSE filings route to the stock's NSE page.
            body = (f"{'⭐📢' if starred else '📢'} <b>{esc(e['name'])}</b>: "
                    f"{esc(a['headline'][:500])}"
                    + (f"\n\n{gist}" if gist else "")          # gist may be typed HTML
                    + f"\n{esc(a['date'] or '')} · {_filing_link(url, e['exch'], sym)}")
            fps = [fp]
            if is_xml and enabled and gist:      # summary already in THIS msg —
                fps.append(fp_sum)               # record fp_sum too, no follow-up
            fp_meta[fp] = (sym, a["headline"], a["date"])
            # Capture a board-meeting PRIOR INTIMATION for the morning agenda brief
            # (idempotent; ticker in yf form so it matches holdings later).
            _capture_event(client, f"{sym}.NS" if e["exch"] == "XNSE" else f"{sym}.BO",
                           a["headline"], a["date"])
            if len(fps) > 1:
                fp_meta[fp_sum] = (sym, a["headline"] + " [xbrl summary]", a["date"])
            if enabled:
                for g in enabled:
                    alerts_by_group.setdefault(g, []).append((fps, body))
            else:
                no_audience.add(fp)           # e.g. a Vishal-only stock

    # ---- dispatch; a fingerprint is SEEN only once it actually delivered ----
    # NOTE: no_audience fingerprints are deliberately NOT marked seen. A stock
    # only Vishal holds (opted out) has no send today, but if Lakshmi buys it
    # while the filing is still in the feed we want it to alert then — so we
    # leave it un-recorded (re-considered each cycle, cheap: no summary, no send).
    delivered = set()
    header, budget = "🗞 <b>Exchange filings</b>\n\n", 3500
    for g, items in alerts_by_group.items():
        chat = chat_id_for_group(g)
        if not chat:
            continue                          # can't deliver -> retry next run
        chunk, clen, chunk_fps = [], len(header), []
        for fps, body in items:
            if chunk and clen + len(body) + 2 > budget:
                if send_telegram(header + "\n\n".join(chunk), chat_id=chat):
                    delivered.update(chunk_fps)
                chunk, clen, chunk_fps = [], len(header), []
            chunk.append(body); chunk_fps.extend(fps); clen += len(body) + 2
        if chunk and send_telegram(header + "\n\n".join(chunk), chat_id=chat):
            delivered.update(chunk_fps)

    written = 0
    for fp in delivered:
        if fp not in fp_meta:
            continue
        sym, headline, d = fp_meta[fp]
        try:
            client.table("filings_seen").insert({
                "fingerprint": fp, "ticker": sym,
                "headline": headline[:300], "filing_date": d or None}).execute()
            written += 1
        except Exception as ex:
            print(f"⚠️ filings_seen write failed for {sym}: {ex}")
    # honest counts: matched (new), actually delivered, recorded (House Rule #3)
    print(f"[filings] {len(scope)} names scoped · {len(fp_meta)} new matched · "
          f"{len(delivered)} delivered · {written} recorded")


def run_filings_audit():
    """Read-only diagnostic (NO Telegram): show which of today's NSE filings the
    engine matches for each NSE stock we HOLD or WATCH. Kept in lock-step with
    run_filings (23-Jul-2026): same holdings+watchlist scope, and labels each
    filing exactly as the engine would treat it — ⛔ SUPPRESSED (in
    ROUTINE_KEYWORDS, the ONLY thing dropped now) / ⭐ MATERIAL (starred) / •
    ALERTS. Run it anytime to answer 'why didn't X alert?' across the portfolio."""
    client = sb()
    feed = fetch_nse_rss()
    # holdings + watchlist, NSE only, one entry per symbol (mirror run_filings)
    scope = {}
    for _, h in get_holdings(client).iterrows():
        m = re.search(r"\(XNSE:([^)]+)\)", str(h["stock_name"]))
        if m:
            scope.setdefault(m.group(1).strip(),
                             re.sub(r"\s*\(XNSE:[^)]+\)\s*$", "", str(h["stock_name"])).strip())
    try:
        for r in (client.table("watchlist").select("stock_name").execute().data or []):
            m = re.search(r"\(XNSE:([^)]+)\)", str(r.get("stock_name")))
            if m:
                scope.setdefault(m.group(1).strip(),
                                 re.sub(r"\s*\(XNSE:[^)]+\)\s*$", "", str(r.get("stock_name"))).strip())
    except Exception:
        pass
    print(f"[filings-audit] {len(feed)} NSE announcements in today's feed; "
          f"{len(scope)} NSE names held/watched\n")
    filed = 0
    for sym, company in sorted(scope.items()):
        anns = fetch_nse_announcements(sym, company)
        if not anns:
            continue
        filed += 1
        print(f"  {sym} — {short_name(company)}: {len(anns)} filing(s)")
        for a in anns[:6]:
            hl = a["headline"].lower()
            if any(k in hl for k in ROUTINE_KEYWORDS):
                tag = "⛔ SUPPRESSED"          # the ONLY thing dropped now
            elif any(k in hl for k in MATERIAL_KEYWORDS):
                tag = "⭐ ALERTS (starred)"
            else:
                tag = "•  ALERTS"
            print(f"     [{tag}] {a['date']} · {a['headline'][:70]}")
    print(f"\n[filings-audit] {filed} NSE names have filings in today's feed. "
          f"BSE-scrip names use the separate per-code path (not this feed).")


# ---------------------------------------------------------------------------
# MODE: deals — NSE + BSE bulk & block deals in portfolio/watchlist stocks
# (built 21-Jul-2026, Lakshmi). EOD data: exchanges publish these after close,
# so this is an evening alert — no intraday feed exists. NSE = daily CSV from the
# friendly archives host, matched by SYMBOL. BSE = BulkDeal_Beta/BlockDeal_Beta
# JSON (routes found by inspecting the BSE site's own JS bundle), matched by
# SCRIP_CODE — so BSE-only SME names (CWD, HSIL, ...) are covered too. Matching
# by symbol/code, so none of the company-name fuzziness of filings.
# ---------------------------------------------------------------------------

def fetch_nse_deals() -> list:
    """Today's NSE bulk + block deals, one dict per deal. [] on failure (logged
    with WHY, rule #3). Each: kind/symbol/date/client/side/qty/price."""
    import csv, io
    out = []
    for kind, fname in (("BULK", "bulk.csv"), ("BLOCK", "block.csv")):
        try:
            r = requests.get(
                f"https://nsearchives.nseindia.com/content/equities/{fname}",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                timeout=20)
            if r.status_code != 200 or "Symbol" not in r.text[:200]:
                print(f"  (NSE {kind} deals: HTTP {r.status_code}, {len(r.content)} bytes)")
                continue
            rows = list(csv.DictReader(io.StringIO(r.text)))
            for raw in rows:
                row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
                sym = row.get("Symbol", "").upper()
                if not sym:
                    continue
                out.append({
                    "kind": kind, "symbol": sym, "date": row.get("Date", ""),
                    "client": row.get("Client Name", ""),
                    "side": row.get("Buy/Sell", ""),
                    "qty": row.get("Quantity Traded", ""),
                    "price": row.get("Trade Price / Wght. Avg. Price", ""),
                })
            print(f"  (NSE {kind} deals: {len(rows)} rows fetched)")
        except Exception as e:
            print(f"  (NSE {kind} deals fetch failed: {type(e).__name__}: {e})")
    return out


def fetch_bse_deals() -> list:
    """Today's BSE bulk + block deals via BulkDeal_Beta / BlockDeal_Beta — the
    routes the live BSE site itself uses (found by inspecting its JS bundle,
    21-Jul-2026; the older BulkDeals/w guesses were invalid routes). Matched by
    SCRIP_CODE, so BSE-only SME names are covered. Session-primed with BSE
    headers, same pattern as fetch_bse_announcements. [] on failure (logged)."""
    out = []
    hdr = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
           "Referer": "https://www.bseindia.com/",
           "Accept": "application/json, text/plain, */*"}
    try:
        s = requests.Session(); s.headers.update(hdr)
        s.get("https://www.bseindia.com/", timeout=15)
    except Exception as e:
        print(f"  (BSE deals prime failed: {e})")
        return []
    for kind, route in (("BULK", "BulkDeal_Beta"), ("BLOCK", "BlockDeal_Beta")):
        try:
            r = s.get(f"https://api.bseindia.com/BseIndiaAPI/api/{route}/w", timeout=20)
            if r.status_code != 200 or "json" not in r.headers.get("content-type", "").lower():
                print(f"  (BSE {kind} deals: HTTP {r.status_code}, {len(r.content)} bytes)")
                continue
            tbl = (r.json() or {}).get("Table") or []
            for a in tbl:
                out.append({
                    "kind": kind, "scrip": str(a.get("SCRIP_CODE", "")).strip(),
                    "name": a.get("ScripName", ""), "client": a.get("CLIENT_NAME", ""),
                    "side": str(a.get("TRANSACTION_TYPE", "")),
                    "qty": a.get("QUANTITY", ""), "price": a.get("PRICE", ""),
                    "date": a.get("DEAL_DATE", ""),
                })
            print(f"  (BSE {kind} deals: {len(tbl)} rows fetched)")
        except Exception as e:
            print(f"  (BSE {kind} deals fetch failed: {type(e).__name__}: {e})")
    return out


def run_deals():
    """Alert on any bulk/block deal in a stock we hold OR watch — NSE (by symbol)
    AND BSE (by scrip code, covering BSE-only SME names). Portfolio-scoped,
    [Both]-tagged, deduped via filings_seen (generic fingerprint store)."""
    client = sb()
    holdings = get_holdings(client)
    try:
        watch = client.table("watchlist").select("stock_name, portfolio_id").execute().data or []
    except Exception:
        watch = []

    nse_scope, bse_scope = {}, {}   # symbol / scrip-code -> {name, groups:set}
    def add(nm, pf):
        s_ = str(nm)
        grp = PF_GROUP.get(int(pf or 1), "vishal")
        mn = re.search(r"\(XNSE:([^)]+)\)", s_)
        mb = re.search(r"\(XBOM:([^)]+)\)", s_)
        if mn:
            e = nse_scope.setdefault(mn.group(1).strip().upper(),
                                     {"name": short_name(nm), "groups": set()})
            e["groups"].add(grp)
        elif mb:
            code = mb.group(1).strip()
            code = BSE_FILING_SCRIPS.get(code, code)   # SME symbol -> numeric scrip
            e = bse_scope.setdefault(str(code), {"name": short_name(nm), "groups": set()})
            e["groups"].add(grp)
    for _, h in holdings.iterrows():
        add(h["stock_name"], h.get("portfolio_id", 1))
    for r in watch:
        add(r.get("stock_name"), r.get("portfolio_id", 1))
    if not nse_scope and not bse_scope:
        return

    matched = []   # (scope_entry, key, deal)
    for d in fetch_nse_deals():
        if d["symbol"] in nse_scope:
            matched.append((nse_scope[d["symbol"]], d["symbol"], d))
    for d in fetch_bse_deals():
        if d["scrip"] in bse_scope:
            matched.append((bse_scope[d["scrip"]], d["scrip"], d))

    seen = _load_seen(client)     # capped query (House Rule #1)
    by_group, to_store = {}, []
    for e, key, d in matched:
        fp = _fingerprint("DEAL", d["kind"], key, d["date"], d["client"], d["side"], str(d["qty"]))
        if fp in seen:
            continue
        seen.add(fp)
        try:
            qtxt = f"{int(float(str(d['qty']).replace(',', ''))):,}"
        except (ValueError, AttributeError):
            qtxt = str(d["qty"])
        is_buy = str(d["side"]).upper().startswith("B")
        emoji, side = ("🟢", "BUY") if is_buy else ("🔴", "SELL")
        msg = (f"🏦 <b>{e['name']}</b> — {d['kind'].title()} deal\n"
               f"{emoji} {side} {qtxt} @ ₹{d['price']} — {d['client']}")
        for g in e["groups"]:
            by_group.setdefault(g, []).append(msg)
        to_store.append((fp, str(key), f"{d['kind']} {d['side']} {d['client']}"[:300]))

    total = 0
    for g, msgs in by_group.items():
        if g not in TELEGRAM_ALERT_GROUPS:
            print(f"({len(msgs)} deal alert(s) for '{g}' — Telegram off)")
            continue
        chat = chat_id_for_group(g)
        if not chat:
            continue
        header, budget = "🏦 <b>Bulk / block deals</b>\n\n", 3500
        chunk, clen = [], len(header)
        for m in msgs:
            if chunk and clen + len(m) + 2 > budget:
                send_telegram(header + "\n\n".join(chunk), chat_id=chat)
                chunk, clen = [], len(header)
            chunk.append(m); clen += len(m) + 2
        if chunk:
            send_telegram(header + "\n\n".join(chunk), chat_id=chat)
        total += len(msgs)
    for fp, key, head in to_store:
        try:
            client.table("filings_seen").insert(
                {"fingerprint": fp, "ticker": key, "headline": head, "filing_date": None}).execute()
        except Exception as e:
            print(f"⚠️ deal dedup write failed for {key}: {e}")
    print(f"[deals] matched {len(matched)} deal(s) in your stocks; {total} new alert(s).")


# ---------------------------------------------------------------------------
# MODE: calendar — this week's results dates (best-effort via NSE)
# ---------------------------------------------------------------------------

def run_calendar():
    client = sb()
    holdings = get_holdings(client)
    symbols = {}
    for _, h in holdings.iterrows():
        m = re.search(r"\(XNSE:([^)]+)\)", str(h["stock_name"]))
        if m:
            symbols[m.group(1).strip()] = short_name(h["stock_name"])

    events = []
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        s.get("https://www.nseindia.com", timeout=15)
        r = s.get("https://www.nseindia.com/api/event-calendar", timeout=15)
        if r.status_code == 200:
            week_end = (date.today() + timedelta(days=7)).isoformat()
            for e in (r.json() or []):
                sym = e.get("symbol", "")
                edate = (e.get("date") or "")[:10]
                if sym in symbols and date.today().isoformat() <= edate <= week_end:
                    events.append(f"• <b>{symbols[sym]}</b> — {e.get('purpose','event')} on {edate}")
    except Exception as e:
        print(f"(calendar fetch failed: {e})")

    if events:
        send_telegram("🗓 <b>This week — corporate events on our holdings</b>\n\n"
                      + "\n".join(events))
        print(f"Sent calendar with {len(events)} event(s).")
    else:
        print("No events found for this week (or calendar fetch unavailable).")


# ---------------------------------------------------------------------------
# MODE: digest — Sunday email summary
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Digest v2 helpers (19-Jul-2026) — the weekly review meeting, in one email
# ---------------------------------------------------------------------------

def _xirr(cashflows):
    """Annualised XIRR via bisection. cashflows: [(date, amount)], buys
    negative, sells + final value positive. None when undefined."""
    if len(cashflows) < 2:
        return None
    amts = [a for _, a in cashflows]
    if all(a >= 0 for a in amts) or all(a <= 0 for a in amts):
        return None
    t0 = min(d for d, _ in cashflows)
    flows = [((d - t0).days / 365.0, a) for d, a in cashflows]

    def npv(rate):
        return sum(a / ((1.0 + rate) ** t) for t, a in flows)

    lo, hi = -0.95, 15.0
    try:
        if npv(lo) * npv(hi) > 0:
            return None
        for _ in range(120):
            mid = (lo + hi) / 2
            v = npv(mid)
            if abs(v) < 1e-7:
                break
            if npv(lo) * v < 0:
                hi = mid
            else:
                lo = mid
        return round(mid * 100, 2)
    except (OverflowError, ZeroDivisionError):
        return None


def _pf_cashflows(client, pf: int):
    """(date, amount) list from the transactions table for one portfolio.
    Buys negative, sells positive."""
    rows = _fetch_all_rows(client, "transactions",
                           "transaction_type, amount, transaction_date",
                           order="transaction_date", portfolio_id=pf)
    out = []
    for r in rows:
        try:
            d = date.fromisoformat(str(r["transaction_date"])[:10])
            amt = float(r["amount"] or 0)
        except (ValueError, TypeError):
            continue
        if amt <= 0:
            continue
        out.append((d, -amt if str(r.get("transaction_type", "buy")).lower() == "buy" else amt))
    return out


def _digest_deliv_strength(client, tickers):
    """{ticker: 4wk delivery avg} for tickers with >=10 stored days."""
    out = {}
    try:
        since = (date.today() - timedelta(days=45)).isoformat()
        res = (client.table("delivery_daily").select("ticker, price_date, deliv_pct")
               .in_("ticker", list(tickers)).gte("price_date", since)
               .order("price_date", desc=True).execute())
        rows = res.data or []
        byt = {}
        for r in rows:
            byt.setdefault(r["ticker"], []).append(float(r["deliv_pct"]))
        for t, vals in byt.items():
            if len(vals) >= 10:
                out[t] = sum(vals[:20]) / min(len(vals), 20)
    except Exception:
        pass
    return out



BENCHMARK_TICKER = "^CNXSC"   # Nifty Smallcap 100 on Yahoo
_BENCH_CACHE = None
_BENCH_LABEL = "Nifty Smallcap 100"


def _benchmark_series():
    """Daily closes of the Nifty Smallcap 100, ~3 years, as a pandas Series
    indexed by date. None on failure -- benchmark sections then degrade to
    a note, per house rules. Cached per process run."""
    global _BENCH_CACHE, _BENCH_LABEL
    if _BENCH_CACHE is not None:
        return _BENCH_CACHE if _BENCH_CACHE is not False else None
    try:
        import yfinance as yf
        df = yf.download(BENCHMARK_TICKER, period="3y", interval="1d",
                         progress=False, auto_adjust=False)
        if df.empty:
            print("(digest: Yahoo returned EMPTY for ^CNXSC — falling back to our table)")
            raise ValueError("empty")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        s = df["Close"].dropna()
        s.index = pd.to_datetime(s.index).date
        # STUB GUARD (21-Jul-2026): Yahoo once answered with a ~1-day series;
        # "non-empty" let it beat our 257-day ETF table, every old buy got
        # approximated at a flat level, and the digest printed "index made
        # -0.0%" with a fake full-XIRR alpha. A benchmark series must carry
        # real history to be allowed to win.
        if len(s) < 60:
            print(f"(digest: Yahoo ^CNXSC returned a stub ({len(s)} days) — "
                  f"falling back to our table)")
            raise ValueError("empty")
        _BENCH_LABEL = "Nifty Smallcap 100"
        _BENCH_CACHE = s
        return s
    except Exception as e:
        if str(e) != "empty":
            print(f"(digest: Yahoo benchmark fetch failed: {e} — falling back to our table)")
        # AUTHORITATIVE FALLBACK (19-Jul-2026): our own index history,
        # stored daily by bhavcopy.py from NSE's official ind_close_all
        # file (ticker NIFTYSMLCAP100.IDX in sme_daily_prices). Same
        # own-the-data pattern as every other Yahoo blind spot this week.
        try:
            client = sb()
            # Fallback chain: exact index (if NSE ever restores the file),
            # then Smallcap-250 ETF proxies (priced by our own daily
            # bhavcopy -- the proven path). _BENCH_LABEL records which
            # source won so the email can say so honestly.
            candidates = [
                ("NIFTYSMLCAP100.IDX", "Nifty Smallcap 100"),
                ("HDFCSML250.NS", "Nifty Smallcap 250 (HDFC ETF proxy)"),
                ("MOSMALL250.NS", "Nifty Smallcap 250 (MO ETF proxy)"),
            ]
            for tick, label in candidates:
                res = (client.table("sme_daily_prices")
                       .select("price_date, close").eq("ticker", tick)
                       .order("price_date", desc=True).limit(900).execute())
                rows = res.data or []
                if len(rows) >= 60:      # need real history, not a few days
                    rows.sort(key=lambda r: r["price_date"])
                    s = pd.Series([float(r["close"]) for r in rows],
                                  index=[date.fromisoformat(str(r["price_date"])[:10]) for r in rows])
                    _BENCH_LABEL = label
                    print(f"(digest: benchmark = {label}, {len(s)} days from own table)")
                    _BENCH_CACHE = s
                    return s
            print("(digest: no benchmark series has enough history yet — "
                  "run the standard bhavcopy backfill to build the ETF proxy history)")
        except Exception as e2:
            print(f"(digest: own-table benchmark fallback failed: {e2})")
        _BENCH_CACHE = False
        return None


def _level_on(series, d):
    """Index level on date d, or the nearest trading day BEFORE it."""
    for back in range(0, 8):
        dd = d - timedelta(days=back)
        if dd in series.index:
            return float(series[dd])
    return None


_BENCH_APPROX_FROM = None

def _benchmark_xirr(cashflows):
    """Lakshmi's benchmark rule (19-Jul-2026): the shadow portfolio.
    Every actual cashflow (same rupees, same dates) buys/sells the index
    proxy instead; XIRR of that shadow book is the yardstick, portfolio
    XIRR minus it = true alpha.

    PARTIAL-HISTORY HANDLING (21-Jul-2026): the ETF proxy's stored history
    starts ~mid-2025, but real buys predate it. A flow older than the
    series now uses the EARLIEST available level, and the email discloses
    the approximation ("index history from <date>; earlier flows
    approximated"). This slightly flatters the index (assumes it went
    nowhere before its first data point), i.e. it UNDERSTATES alpha --
    the conservative direction for a "should we even be doing this"
    verdict. Honest partial benchmark > eternal 'unavailable'."""
    global _BENCH_APPROX_FROM
    _BENCH_APPROX_FROM = None
    series = _benchmark_series()
    if series is None or len(cashflows) < 1:
        return None
    first_d = series.index[0]
    first_lvl = float(series.iloc[0])
    units = 0.0
    approx = False
    for d, a in cashflows:
        lvl = _level_on(series, d)
        if lvl is None:
            if d < first_d:
                lvl = first_lvl
                approx = True
            else:
                return None      # gap INSIDE the series: genuinely broken
        if a < 0:
            units += (-a) / lvl          # buy day: rupees into the index
        else:
            units = max(0.0, units - a / lvl)   # sell day: rupees out
    if approx:
        _BENCH_APPROX_FROM = first_d
    final_val = units * float(series.iloc[-1])
    return _xirr(cashflows + [(date.today(), final_val)])


WEEKLY_ALPHA_BAR = 0.14   # Lakshmi 23-Jul-2026: beating the index by 0.14% in a
                          # WEEK is what the best fund managers manage. At/above
                          # this = must continue; below = not great.


def _realised_between(client, pf: int, start_d, end_d) -> float:
    """Profit BOOKED by sales in (start_d, end_d]. Part of the week's return:
    a sale converts unrealised into realised, so the weekly gain must count both
    or selling a winner would look like the week went backwards."""
    try:
        res_rows = _fetch_all_rows(client, "realised", "gain_loss, sale_date",
                                   portfolio_id=pf)
        tot = 0.0
        for r in res_rows:
            try:
                d = date.fromisoformat(str(r["sale_date"])[:10])
            except (ValueError, TypeError):
                continue
            if start_d < d <= end_d:
                tot += float(r.get("gain_loss") or 0)
        return tot
    except Exception as e:
        print(f"(digest: realised-in-week lookup failed for pf {pf}: {e})")
        return 0.0


def _weekly_vs_index(client, pf, prev, val, unreal, today):
    """Lakshmi's weekly scorecard (23-Jul-2026): how the WEEK went for us vs the
    Nifty Smallcap index — deliberately independent of XIRR, which depends on the
    buy dates he part-guessed. Portfolio week return = (change in unrealised +
    profit booked this week) / last week's value. Index measured over the SAME
    dates. Returns HTML, or a note when there's no prior snapshot yet."""
    if not prev or not prev.get("snap_date"):
        return ("<p style='color:#888'>Weekly vs index: baseline set this week — "
                "the comparison starts from next week's digest.</p>", None)
    try:
        prev_d = date.fromisoformat(str(prev["snap_date"])[:10])
        # Guard against a SUB-WEEK gap (08-Aug-2026): the digest is a Friday-to-Friday
        # measure, but if it's run off-cadence the newest prior snapshot can be only a
        # day or two old — comparing over 1 day and calling it "this week" is nonsense
        # (and the live-vs-settled price basis between the two makes the % garbage). A
        # real week is ~7 days; require at least 4 before showing the comparison.
        if (today - prev_d).days < 4:
            return ("<p style='color:#888'>Weekly vs index: last snapshot is only "
                    f"{(today - prev_d).days} day(s) old — the weekly comparison needs a "
                    "full week between digests (resumes next Friday).</p>", None)
        prev_val = float(prev.get("current_value") or 0)
        prev_unreal = float(prev.get("unrealised") or 0)
        if prev_val <= 0:
            return ("<p style='color:#888'>Weekly vs index: no prior value to compare.</p>", None)

        booked = _realised_between(client, pf, prev_d, today)
        gain = (unreal - prev_unreal) + booked
        pf_ret = gain / prev_val * 100

        series = _benchmark_series()
        i0 = _level_on(series, prev_d) if series is not None else None
        i1 = _level_on(series, today) if series is not None else None
        if not i0 or not i1:
            return (f"<p>Our week: <b>{pf_ret:+.2f}%</b> "
                    f"({_fmt_l(gain)}) — index unavailable for comparison</p>", None)
        idx_ret = (i1 / i0 - 1) * 100
        alpha = pf_ret - idx_ret

        if alpha >= WEEKLY_ALPHA_BAR:
            col, verdict = "#16a34a", "beating the index — must continue ✅"
        elif alpha >= 0:
            col, verdict = "#d97706", f"ahead, but under the {WEEKLY_ALPHA_BAR}% bar"
        else:
            col, verdict = "#dc2626", "behind the index this week"
        return (
            f"<table style='width:100%;border-collapse:collapse;font-size:14px'>"
            f"<tr><td style='padding:3px 0;color:#64748b;width:52%'>Our week "
            f"(realised + unrealised)</td>"
            f"<td style='padding:3px 0;font-weight:700'>{pf_ret:+.2f}% "
            f"<span style='font-weight:400;color:#64748b'>({_fmt_l(gain)})</span></td></tr>"
            f"<tr><td style='padding:3px 0;color:#64748b'>{_BENCH_LABEL}</td>"
            f"<td style='padding:3px 0;font-weight:700'>{idx_ret:+.2f}%</td></tr>"
            f"<tr><td style='padding:3px 0;color:#64748b'>Weekly alpha "
            f"(bar: {WEEKLY_ALPHA_BAR}%)</td>"
            f"<td style='padding:3px 0;font-weight:800;color:{col}'>{alpha:+.2f} pts</td></tr>"
            f"</table>"
            f"<p style='margin:6px 0 0;color:{col};font-weight:600'>{verdict}</p>"
            f"<p style='margin:2px 0 0;color:#94a3b8;font-size:11px'>"
            f"week measured {prev_d.strftime('%d %b')} → {today.strftime('%d %b')}; "
            f"independent of XIRR / entry dates</p>",
            {"pf_ret": pf_ret, "idx_ret": idx_ret, "alpha": alpha})
    except Exception as e:
        return (f"<p style='color:#888'>Weekly vs index unavailable: {e}</p>", None)


def _benchmark_week_move():
    """Index % move over the last ~5 trading days, or None."""
    s = _benchmark_series()
    if s is None or len(s) < 6:
        return None
    return (float(s.iloc[-1]) / float(s.iloc[-6]) - 1) * 100


def _fmt_l(x):
    """Rupees in lakh/crore, compact."""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return "—"
    if abs(x) >= 1e7:
        return f"₹{x/1e7:,.2f} Cr"
    return f"₹{x/1e5:,.1f} L"




def _box(title, inner_html, accent="#1e3a8a", bg="#ffffff"):
    """A titled section card. Inline styles only -- email clients ignore
    stylesheets, so every visual decision must travel inside the tag."""
    if not inner_html:
        return ""
    return (f"<div style='background:{bg};border:1px solid #e2e8f0;"
            f"border-left:4px solid {accent};border-radius:8px;"
            f"padding:14px 18px;margin:14px 0'>"
            f"<div style='font-size:15px;font-weight:700;color:{accent};"
            f"margin-bottom:8px'>{title}</div>"
            f"<div style='font-size:14px;color:#334155;line-height:1.55'>{inner_html}</div>"
            f"</div>")


def _bench_html(xirr, bench):
    """Alpha verdict per Lakshmi's rule: beating the Nifty Smallcap 100 by
    5+ pts = clearly worth it; 2-5 = marginal; below 2 = the index would
    have done the job. Honest '--' when either side is unavailable."""
    if xirr is None or bench is None:
        return (f"<p style='margin:4px 0;color:#888'>vs {_BENCH_LABEL}: "
                "benchmark unavailable this week</p>")
    alpha = xirr - bench
    if alpha >= 5:
        col, verdict = "#16a34a", "beating the index — clearly worth it ✅"
    elif alpha >= 2:
        col, verdict = "#d97706", "ahead, but inside the 2–5pt grey zone"
    else:
        col, verdict = "#dc2626", "NOT beating the index meaningfully — review"
    note = ""
    if _BENCH_APPROX_FROM:
        note = (f"<br><span style='color:#94a3b8;font-size:12px'>index history from "
                f"{_BENCH_APPROX_FROM.strftime('%d %b %Y')}; older buys approximated "
                f"at its first level (understates alpha)</span>")
    return (f"<p style='margin:4px 0'>vs <b>{_BENCH_LABEL}</b> "
            f"(same money, same dates): index would have made {bench:.1f}% "
            f"→ alpha <b style='color:{col}'>{alpha:+.1f} pts</b> — "
            f"<span style='color:{col}'>{verdict}</span>{note}</p>")


def run_morning_brief():
    """~08:30 IST daily: ONE Telegram brief of Lakshmi+Abinaya names with a
    corporate event (board meeting → results/dividend/fund-raise/…) scheduled
    TODAY — HOLDINGS (📌) and WATCHLIST (👀) in separate sections — from prior
    intimations captured by run_filings. Forward-looking
    'today's agenda' so Lakshmi knows what's coming before the open (Lakshmi
    02-Aug-2026). SILENT when nothing is due today. Dedup marker in
    entry_alert_log (ticker '__morning_brief__') so worker + a backstop can't
    double-send. NOTE: it populates FORWARD — only meetings intimated AFTER this
    shipped are known, so it fills in over ~1-2 weeks (NSE's forthcoming-meetings
    API is datacenter-blocked, House Rule #1, so no backfill)."""
    client = sb()
    today_iso = date.today().isoformat()
    try:
        if client.table("entry_alert_log").select("ticker").eq("alert_date", today_iso) \
                .eq("ticker", "__morning_brief__").eq("kind", "BRIEF").execute().data:
            print("(morning brief already sent today)")
            return
    except Exception:
        pass
    holdings = get_holdings(client)
    lak = {p for p, g in PF_GROUP.items() if g == "lakshmi"}
    hold_names = {}
    for _, h in holdings.iterrows():
        if int(h.get("portfolio_id", 1)) in lak:
            t = extract_yf_ticker(h["stock_name"])
            if t:
                hold_names[t] = short_name(h["stock_name"])
    try:
        wl = client.table("watchlist").select("stock_name, portfolio_id").execute().data or []
    except Exception:
        wl = []
    watch_names = {}
    for r in wl:
        if int(r.get("portfolio_id", 1)) in lak:
            t = extract_yf_ticker(r.get("stock_name"))
            if t and t not in hold_names:              # a held name counts as a holding
                watch_names[t] = short_name(r["stock_name"])
    all_t = list(set(hold_names) | set(watch_names))
    if not all_t:
        return
    try:
        evs = client.table("scheduled_events").select("*") \
            .eq("event_date", today_iso).in_("ticker", all_t).execute().data or []
    except Exception as e:
        print(f"(morning brief: scheduled_events query failed — is the table created? {e})")
        return
    by_t = {}
    for ev in evs:
        by_t.setdefault(ev["ticker"], ev)
    if not by_t:
        print("(morning brief: no events scheduled today)")
        return

    def _ln(nm, ev):
        return f"• <b>{html.escape(nm)}</b> — {html.escape(ev.get('purpose') or 'board meeting')}"
    hold_lines = sorted(_ln(hold_names[t], ev) for t, ev in by_t.items() if t in hold_names)
    watch_lines = sorted(_ln(watch_names[t], ev) for t, ev in by_t.items() if t in watch_names)
    parts = [f"🗓 <b>Today's agenda · {date.today():%d %b}</b>"]
    if hold_lines:
        parts.append("📌 <b>Holdings</b>\n" + "\n".join(hold_lines))
    if watch_lines:
        parts.append("👀 <b>Watchlist</b>\n" + "\n".join(watch_lines))
    body = "\n\n".join(parts)
    chat = chat_id_for_group("lakshmi")
    if not chat:
        return
    if send_telegram(body, chat_id=chat):
        try:
            client.table("entry_alert_log").upsert({
                "ticker": "__morning_brief__", "grp": "lakshmi",
                "alert_date": today_iso, "kind": "BRIEF"}).execute()
        except Exception:
            pass
        print(f"Morning brief sent: {len(by_t)} event(s) today.")


# ---------------------------------------------------------------------------
# VALUATION CROSS-CHECK (08-Aug-2026, Vishal).
# The digest and the dashboard price holdings through DIFFERENT code paths, and
# they drifted apart twice in one day: (a) a weekly-close reconcile bug knocked
# 10% off PGIL, (b) young listings (<45wk history) had no state close, so the
# digest valued them at PURCHASE COST -- hiding a 25% loss on Advent Hotels and
# understating young winners. Both shipped numbers that looked plausible and
# tripped no alarm; only Vishal reading the email against the dashboard caught
# them. So: before the digest reports a single rupee, re-price every holding
# through an INDEPENDENT path and refuse to send if the two disagree.
#
# House Rule #2 (a wrong number is worse than a blank one) is the whole reason
# this blocks rather than warns; DIGEST_RECONCILE_MODE=warn sends with a banner
# instead, =off disables it.
RECONCILE_PRICE_TOL_PCT = 2.0   # per-holding price gap that counts as a mismatch
RECONCILE_VALUE_TOL_PCT = 0.5   # portfolio-total gap that blocks the send


def _independent_price(ticker: str):
    """Latest market close from the DAILY path (bhavcopy-first, Yahoo for
    mainboard names) — deliberately NOT `current_state`'s weekly bars, so this is
    a genuinely independent second opinion rather than the same source re-read.
    Both paths settle to the same Friday close on healthy data, and the digest
    only runs after the close, so a real gap means one side is stale/wrong.
    None when the ticker has no daily data at all (can't judge -> not a finding)."""
    try:
        d = signals._fetch_daily(ticker)
        if d is None or d.empty or "close" not in d.columns:
            return None
        return float(d["close"].iloc[-1])
    except Exception:
        return None


def _valuation_mismatches(holdings, by_ticker, memo=None):
    """Re-price every holding independently. Returns (findings, digest_total,
    reference_total). A finding is a holding the digest would misprice:
      • 'COST'  — the digest has no close and would fall back to purchase cost
                  (the Advent Hotels bug: silently reports no gain/loss)
      • 'DRIFT' — both paths priced it but disagree by >RECONCILE_PRICE_TOL_PCT
                  (the PGIL bug: one side stale)
    Totals are compared separately so many tiny gaps can't slip through under a
    per-holding threshold."""
    memo = {} if memo is None else memo
    findings, dig_total, ref_total = [], 0.0, 0.0
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        qty = float(h.get("quantity") or 0)
        cost = float(h.get("purchase_cost") or 0)
        if not ticker or qty <= 0:
            continue
        dig = ((by_ticker.get(ticker) or {}).get("d") or {}).get("close")
        try:
            dig = float(dig) if dig not in (None, "") else None
        except (TypeError, ValueError):
            dig = None
        if ticker not in memo:
            memo[ticker] = _independent_price(ticker)
        ref = memo[ticker]
        # value the digest WOULD report (cost fallback is exactly the danger)
        dig_total += qty * (dig if dig is not None else cost)
        ref_total += qty * (ref if ref is not None else
                            (dig if dig is not None else cost))
        if ref is None:
            continue                      # no second opinion -> can't call it wrong
        if dig is None:
            findings.append({"ticker": ticker, "name": short_name(h["stock_name"]),
                             "kind": "COST", "digest": cost, "ref": ref, "qty": qty,
                             "gap_pct": (cost - ref) / ref * 100 if ref else None})
        elif abs(dig - ref) / ref * 100 > RECONCILE_PRICE_TOL_PCT:
            findings.append({"ticker": ticker, "name": short_name(h["stock_name"]),
                             "kind": "DRIFT", "digest": dig, "ref": ref, "qty": qty,
                             "gap_pct": (dig - ref) / ref * 100})
    return findings, dig_total, ref_total


def _report_mismatches(findings, dig_total, ref_total, label=""):
    """Print the cross-check result the same way for the digest and the CLI
    (House Rule #3: say WHY, with the numbers, never just 'failed')."""
    gap = abs(dig_total - ref_total)
    gap_pct = gap / ref_total * 100 if ref_total else 0.0
    who = f" [{label}]" if label else ""
    print(f"[digest-reconcile]{who} digest Rs {dig_total:,.0f} vs independent "
          f"Rs {ref_total:,.0f} (gap Rs {gap:,.0f} = {gap_pct:.2f}%)")
    for f in findings:
        if f["kind"] == "COST":
            print(f"    COST-FALLBACK {f['name']} ({f['ticker']}): digest has no "
                  f"price, would use cost Rs {f['digest']:,.2f} vs market "
                  f"Rs {f['ref']:,.2f} ({f['gap_pct']:+.1f}%), qty {f['qty']:.0f}")
        else:
            print(f"    PRICE-DRIFT   {f['name']} ({f['ticker']}): digest "
                  f"Rs {f['digest']:,.2f} vs independent Rs {f['ref']:,.2f} "
                  f"({f['gap_pct']:+.1f}%), qty {f['qty']:.0f}")
    return gap_pct


def run_reconcile():
    """Read-only: cross-check every portfolio's digest valuation against the
    independent price path and print any mismatch. Nothing sent, nothing written.
    Run it any time (`python alerts.py reconcile`) — the scalable spot-check that
    the digest and the dashboard still agree about money."""
    client = sb()
    holdings = get_holdings(client)
    if holdings.empty:
        print("No holdings.")
        return
    memo, bad = {}, 0
    for pf in sorted(int(p) for p in holdings["portfolio_id"].unique()):
        sub = holdings[holdings["portfolio_id"] == pf]
        by_ticker = {}
        for _, h in sub.iterrows():
            t = extract_yf_ticker(h["stock_name"])
            if not t or t in by_ticker:
                continue
            try:
                by_ticker[t] = {"d": signals.current_state(t) or {}}
            except Exception as e:
                print(f"  ({t}: state failed — {e})")
                by_ticker[t] = {"d": {}}
        finds, dig, ref = _valuation_mismatches(sub, by_ticker, memo)
        gap_pct = _report_mismatches(finds, dig, ref, PF_NAME.get(pf, str(pf)))
        if finds or gap_pct > RECONCILE_VALUE_TOL_PCT:
            bad += 1
    print(f"[digest-reconcile] {'OK — all portfolios agree' if not bad else f'{bad} portfolio(s) MISMATCHED'}")


def run_digest():
    """Weekly digest — TWO separate emails (Vishal 07-Aug-2026): his OWN book in
    one email, the Lakshmi+Abinaya book in another, so neither email carries the
    other's (long) all-holdings states table. Both go to DIGEST_EMAILS. The
    Telegram TEASER rides ONLY the Lakshmi/Abinaya email (Vishal opted out of
    Telegram). Each call stores its portfolios' weekly digest_history snapshots."""
    client = sb()
    all_holdings = get_holdings(client)
    if all_holdings.empty:
        return
    lak = [p for p, g in PF_GROUP.items() if g == "lakshmi"]
    vis = [p for p, g in PF_GROUP.items() if g == "vishal"]
    hl = all_holdings[all_holdings["portfolio_id"].isin(lak)]
    hv = all_holdings[all_holdings["portfolio_id"].isin(vis)]
    if not hl.empty:
        _digest_for(client, hl, tg_pf_ids=lak, label="Lakshmi & Abinaya", telegram=True)
    if not hv.empty:
        _digest_for(client, hv, tg_pf_ids=[], label="Vishal", telegram=False)


def _digest_for(client, holdings, tg_pf_ids=None, label=None, telegram=True):
    """Digest v2 (19-Jul-2026): the weekly review meeting. Per-portfolio
    money numbers with week-over-week trend, states, dead-money flags,
    profit tiers, journal+audit corner, delivery conviction, concentration.
    Every section is individually try/excepted: one broken data layer
    degrades that section to a note, never kills the digest.
    `label` names this email (subject + header, e.g. 'Vishal'); `telegram=False`
    suppresses the Telegram teaser (used for the email-only Vishal digest)."""
    import json as _json
    today = date.today()
    label_suffix = f" — {label}" if label else ""
    pf_ids = sorted(int(p) for p in holdings["portfolio_id"].unique())

    # ---- per-ticker compute (once), incl. bars for dead-money ----
    by_ticker = {}
    for _, h in holdings.iterrows():
        ticker = extract_yf_ticker(h["stock_name"])
        if not ticker:
            continue
        e = by_ticker.setdefault(ticker, {"name": short_name(h["stock_name"]),
                                          "pfs": {}, "state": None, "d": {}})
        pf = int(h.get("portfolio_id", 2))
        e["pfs"][pf] = {"qty": float(h.get("quantity") or 0),
                        "cost": float(h.get("purchase_cost") or 0)}

    dead_money, rows, exits, cautions, adds = [], [], [], [], []
    for ticker, e in by_ticker.items():
        try:
            d = signals.current_state(ticker)
            e["d"] = d or {}
            e["state"] = (d or {}).get("state")
        except Exception:
            continue
        owners = sorted(e["pfs"])
        tag = "[Both] " if len(owners) > 1 else f"[{PF_NAME.get(owners[0], owners[0])}] "
        name = f"{tag}{e['name']}"
        st_ = e["state"] or "NO DATA"
        rows.append((name, st_, e["d"].get("reason", "")))
        if st_ == "EXIT":
            exits.append(name)
        elif st_ in ("BE CAUTIOUS", "MOMENTUM FADING"):
            cautions.append(name)
        elif st_ == "MAINTAIN/ADD":
            adds.append(name)
        # dead money: ~13 weeks sideways (within ±10%), state not EXIT
        try:
            bars = signals.fetch_weekly(ticker)
            if len(bars) >= 14 and st_ != "EXIT":
                move = bars["Close"].iloc[-1] / bars["Close"].iloc[-14] - 1
                if abs(move) < 0.10:
                    dead_money.append((name, move * 100))
        except Exception:
            pass

    # ---- valuation cross-check, BEFORE any money is stored or sent ----------
    # Deliberately placed ahead of the per-portfolio loop: that loop WRITES the
    # digest_history snapshot, and a bad snapshot would poison next week's
    # week-over-week too. Blocking here means nothing wrong is emailed OR stored.
    recon_box = ""
    _mode = (os.environ.get("DIGEST_RECONCILE_MODE") or "block").strip().lower()
    if _mode != "off":
        try:
            _finds, _dig, _ref = _valuation_mismatches(holdings, by_ticker)
            _gap_pct = _report_mismatches(_finds, _dig, _ref, label or "")
            if _finds or _gap_pct > RECONCILE_VALUE_TOL_PCT:
                if _mode == "block":
                    print(f"[digest-reconcile] BLOCKED — digest for '{label or 'portfolio'}' "
                          f"NOT sent and no snapshot stored. Fix the pricing gap above, "
                          f"then re-run. (DIGEST_RECONCILE_MODE=warn to send anyway.)")
                    return
                _rows_html = "<br>".join(
                    f"{html.escape(str(f['name']))}: digest ₹{f['digest']:,.2f} vs "
                    f"market ₹{f['ref']:,.2f} ({f['gap_pct']:+.1f}%)"
                    f"{' — digest has NO price, using cost' if f['kind'] == 'COST' else ''}"
                    for f in _finds) or "portfolio totals disagree"
                recon_box = (
                    "<div style='background:#fef2f2;border:1px solid #fecaca;"
                    "border-left:4px solid #dc2626;border-radius:8px;padding:14px 18px;"
                    "margin:14px 0'><div style='font-size:15px;font-weight:800;color:#dc2626'>"
                    "⚠️ VALUATION MISMATCH — treat the numbers below as unverified</div>"
                    f"<div style='font-size:13px;color:#7f1d1d;margin-top:6px'>{_rows_html}"
                    f"<br><i>Digest total ₹{_dig:,.0f} vs independent re-pricing "
                    f"₹{_ref:,.0f} ({_gap_pct:.2f}% apart).</i></div></div>")
        except Exception as ex:
            print(f"(digest: valuation cross-check failed — {ex})")

    # ---- per-portfolio money numbers + snapshot diffs ----
    pf_sections = []
    detail_by_pf = {}
    weekly_by_pf = {}          # pf -> {pf_ret, idx_ret, alpha} for the Telegram recap
    for pf in pf_ids:
        try:
            inv = val = 0.0
            detail = {}
            for ticker, e in by_ticker.items():
                if pf not in e["pfs"]:
                    continue
                p = e["pfs"][pf]
                inv += p["qty"] * p["cost"]
                close = e["d"].get("close")
                v = p["qty"] * float(close) if close else p["qty"] * p["cost"]
                val += v
                pnl_pct = ((float(close) - p["cost"]) / p["cost"] * 100
                           if close and p["cost"] else 0.0)
                detail[ticker] = {"state": (str(e["state"]) if e["state"] else None),
                                  "pnl_pct": float(round(pnl_pct, 2))}
            unreal = val - inv
            raw_cfs = _pf_cashflows(client, pf)
            cfs = raw_cfs + [(today, val)]
            xirr = _xirr(cfs)
            bench = _benchmark_xirr(raw_cfs)
            detail_by_pf[pf] = detail

            # previous snapshot for trend
            prev = None
            try:
                r = (client.table("digest_history").select("*")
                     .eq("portfolio_id", pf).lt("snap_date", today.isoformat())
                     .order("snap_date", desc=True).limit(1).execute())
                prev = (r.data or [None])[0]
            except Exception:
                pass
            # A weekly digest is a Friday-to-Friday measure. If run OFF-CADENCE the
            # newest prior snapshot can be only a day or two old — treat that as NO
            # prior (baseline) so NO metric (money deltas OR the weekly-vs-index box)
            # shows a garbage sub-week "WoW": adjacent-day snapshots differ mostly by
            # the live-vs-settled price basis, which makes any 1-day % nonsense
            # (surfaced as a bogus "-4.35% behind the index" mid-week, 08-Aug-2026).
            if prev and prev.get("snap_date"):
                try:
                    if (today - date.fromisoformat(str(prev["snap_date"])[:10])).days < 4:
                        prev = None
                except (ValueError, TypeError):
                    pass

            def _delta(cur, prev_v, pct=False, pts=False):
                if prev_v is None or cur is None:
                    return "<span style='color:#888'>(baseline set this week)</span>"
                dv = cur - float(prev_v)
                col = "#16a34a" if dv >= 0 else "#dc2626"
                arrow = "▲" if dv >= 0 else "▼"
                if pts:
                    return f"<span style='color:{col}'>{arrow} {abs(dv):.2f} pts WoW</span>"
                return f"<span style='color:{col}'>{arrow} {_fmt_l(abs(dv))} WoW</span>"

            trend_pnl = _delta(unreal, prev.get("unrealised") if prev else None)
            trend_xirr = (_delta(xirr, prev.get("xirr") if prev else None, pts=True)
                          if xirr is not None else "")

            # profit tiers: crossings vs last week's per-ticker pnl_pct
            tiers_html = ""
            try:
                prev_detail = (prev or {}).get("detail") or {}
                if isinstance(prev_detail, str):
                    prev_detail = _json.loads(prev_detail)
                crossed = []
                for t, cur_d in detail.items():
                    cur_p = cur_d["pnl_pct"]
                    prev_p = (prev_detail.get(t) or {}).get("pnl_pct")
                    for tier in (150, 100, 50):
                        if cur_p >= tier and (prev_p is None or prev_p < tier):
                            nm = by_ticker[t]["name"]
                            crossed.append(f"{nm} crossed <b>+{tier}%</b> (now {cur_p:+.0f}%)")
                            break
                if crossed:
                    tiers_html = ("<p>🏆 <b>Profit tiers this week:</b> "
                                  + " · ".join(crossed[:6]) + "</p>")
            except Exception:
                pass

            # concentration: top-5 share
            conc_html = ""
            try:
                vals = sorted((e["pfs"][pf]["qty"] * float(e["d"].get("close") or e["pfs"][pf]["cost"]), e["name"])
                              for t, e in by_ticker.items() if pf in e["pfs"])
                top5 = sum(v for v, _ in vals[-5:])
                share = top5 / val * 100 if val else 0
                warn = " style='color:#d97706'" if share >= 50 else ""
                conc_html = (f"<p{warn}>Top-5 concentration: <b>{share:.0f}%</b> of the book"
                             + (" — worth a look" if share >= 50 else "") + "</p>")
            except Exception:
                pass

            # Lakshmi's weekly scorecard — the headline he actually judges on,
            # because unlike XIRR it doesn't depend on the entry dates.
            weekly_html, weekly_stats = _weekly_vs_index(client, pf, prev, val, unreal, today)
            weekly_by_pf[pf] = weekly_stats

            # FY-to-date realised P&L (1 Apr–31 Mar, capped at today so a future-
            # dated row can't inflate it) — mirrors the dashboard KPI.
            fy_start, _fy_end, fy_label = signals.fy_bounds(today)
            fy_realised = 0.0
            try:
                for rr in _fetch_all_rows(client, "realised", "gain_loss, sale_date",
                                          portfolio_id=pf):
                    try:
                        sdd = date.fromisoformat(str(rr["sale_date"])[:10])
                    except (ValueError, TypeError):
                        continue
                    if fy_start <= sdd <= today:
                        fy_realised += float(rr.get("gain_loss") or 0)
            except Exception as ex:
                print(f"(digest: FY realised lookup failed for pf {pf}: {ex})")

            up = unreal >= 0
            pnl_col = "#16a34a" if up else "#dc2626"
            pf_sections.append(f"""
            <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;
                        padding:16px 20px;margin:12px 0">
              <div style="font-size:17px;font-weight:800;color:#0f172a;
                          border-bottom:2px solid #1e3a8a;padding-bottom:6px;
                          margin-bottom:10px">{PF_NAME.get(pf, pf)}</div>
              <table style="width:100%;border-collapse:collapse;font-size:14px">
                <tr>
                  <td style="padding:4px 0;color:#64748b;width:34%">Invested</td>
                  <td style="padding:4px 0;font-weight:700">{_fmt_l(inv)}</td>
                </tr><tr>
                  <td style="padding:4px 0;color:#64748b">Current value</td>
                  <td style="padding:4px 0;font-weight:700">{_fmt_l(val)}</td>
                </tr><tr>
                  <td style="padding:4px 0;color:#64748b">Unrealised P&amp;L</td>
                  <td style="padding:4px 0;font-weight:700;color:{pnl_col}">
                    {_fmt_l(unreal)} ({(unreal/inv*100 if inv else 0):+.1f}%)
                    &nbsp;<span style="font-weight:400;font-size:13px">{trend_pnl}</span></td>
                </tr><tr>
                  <td style="padding:4px 0;color:#64748b">XIRR (annualised)</td>
                  <td style="padding:4px 0;font-weight:700">
                    {f"{xirr:.1f}%" if xirr is not None else "—"}
                    &nbsp;<span style="font-weight:400;font-size:13px">{trend_xirr}</span></td>
                </tr><tr>
                  <td style="padding:4px 0;color:#64748b">Realised · {fy_label} (to date)</td>
                  <td style="padding:4px 0;font-weight:700;color:{'#16a34a' if fy_realised >= 0 else '#dc2626'}">
                    {_fmt_l(fy_realised)}</td>
                </tr>
              </table>
              <div style="background:#eef2ff;border:1px solid #c7d2fe;
                          border-left:4px solid #4338ca;border-radius:8px;
                          padding:12px 14px;margin:12px 0">
                <div style="font-size:14px;font-weight:800;color:#3730a3;
                            margin-bottom:6px">📅 THIS WEEK vs the index</div>
                {weekly_html}
              </div>
              <div style="margin-top:8px">{_bench_html(xirr, bench)}</div>
              {tiers_html}{conc_html}
            </div>""")

            # store this week's snapshot (upsert -> reruns safe)
            try:
                client.table("digest_history").upsert({
                    "portfolio_id": int(pf), "snap_date": today.isoformat(),
                    "invested": float(round(inv, 2)),
                    "current_value": float(round(val, 2)),
                    "unrealised": float(round(unreal, 2)),
                    "xirr": float(xirr) if xirr is not None else None,
                    "bench_xirr": float(bench) if bench is not None else None,
                    "detail": detail,
                }, on_conflict="portfolio_id,snap_date").execute()
            except Exception as ex:
                print(f"(digest: snapshot store failed for pf {pf}: {ex})")
        except Exception as ex:
            pf_sections.append(f"<p>({PF_NAME.get(pf, pf)}: numbers unavailable — {ex})</p>")

    # ---- journal + audits this week ----
    journal_html = ""
    try:
        wk_ago = (today - timedelta(days=7)).isoformat()
        jr = client.table("trade_journal").select("*") \
            .in_("portfolio_id", [int(p) for p in pf_ids]).execute().data or []
        entries = [j for j in jr if str(j.get("exit_date", "")) >= wk_ago]
        verdicts = []
        for j in jr:
            for w in (30, 60, 90):
                if str(j.get(f"audited_{w}d") or "") >= wk_ago and j.get(f"price_{w}d"):
                    chg = (float(j[f"price_{w}d"]) - float(j["exit_price"])) / float(j["exit_price"]) * 100
                    verdict = "saved" if chg < 0 else "cost"
                    verdicts.append(f"{short_name(j['ticker'])} +{w}d: exit "
                                    f"<b>{verdict} {abs(chg):.1f}%</b>")
        lines = []
        for j in entries:
            lines.append(f"{short_name(j['ticker'])} sold @ ₹{float(j['exit_price']):,.1f} "
                         f"({j['reason']})" + (f" — <i>{j['notes']}</i>" if j.get("notes") else ""))
        if lines or verdicts:
            journal_html = "<br>".join(lines + verdicts)
    except Exception:
        pass

    # ---- delivery conviction ----
    deliv_html = ""
    try:
        strengths = _digest_deliv_strength(client, tuple(by_ticker.keys()))
        conv = []
        for t, avg in strengths.items():
            e = by_ticker.get(t) or {}
            if avg >= 60 and e.get("state") in ("MAINTAIN/ADD", "BULLISH SIGNAL"):
                conv.append(f"{e['name']} ({avg:.0f}% delivery)")
        if conv:
            deliv_html = ", ".join(sorted(conv)[:8])
    except Exception:
        pass

    # ---- dead money ----
    dead_html = ""
    if dead_money:
        dead_html = ", ".join(f"{n} <span style='color:#64748b'>({m:+.0f}% in 13wk)</span>"
                              for n, m in sorted(dead_money)[:8])

    # ---- states table (unchanged core) ----
    color = {"EXIT": "#dc2626", "BE CAUTIOUS": "#d97706", "MOMENTUM FADING": "#7c3aed",
             "MAINTAIN/ADD": "#16a34a", "BULLISH SIGNAL": "#16a34a",
             "WAIT/WATCH": "#0891b2"}
    trs = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid #eee'>{n}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee;"
        f"color:{color.get(s,'#333')};font-weight:600'>{s}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee;color:#555;"
        f"font-size:13px'>{r}</td></tr>"
        for n, s, r in sorted(rows, key=lambda x: x[1]))

    action_box = ""
    if exits:
        action_box = ("<div style='background:#fef2f2;border:1px solid #fecaca;"
                      "border-left:4px solid #dc2626;border-radius:8px;"
                      "padding:14px 18px;margin:14px 0'>"
                      "<div style='font-size:15px;font-weight:800;color:#dc2626'>"
                      "⚠️ ACTION NEEDED — EXIT signals</div>"
                      "<div style='font-size:14px;color:#7f1d1d;margin-top:6px'>"
                      + ", ".join(exits) +
                      "</div></div>")

    html = f"""
    <div style="font-family:-apple-system,Segoe UI,Arial,sans-serif;max-width:700px;
                margin:0 auto;background:#f1f5f9;padding:18px">
      <div style="background:#1e3a8a;color:#ffffff;border-radius:10px;
                  padding:20px 24px;margin-bottom:6px">
        <div style="font-size:21px;font-weight:800">📊 Weekly Portfolio Digest{label_suffix}</div>
        <div style="font-size:13px;opacity:.85;margin-top:4px">
          {today.strftime('%A, %d %B %Y')} · {len(rows)} holdings scanned ·
          {len(exits)} EXIT · {len(cautions)} caution · {len(adds)} healthy</div>
      </div>

      {recon_box}
      {action_box}
      {''.join(pf_sections)}

      {_box("💤 Dead money watch — 90+ days sideways", dead_html, accent="#64748b")}
      {_box("🏛 Conviction moves — healthy state + 60%+ delivery", deliv_html, accent="#0891b2")}
      {_box("📓 Journal &amp; audits this week", journal_html, accent="#7c3aed")}

      <div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:10px;
                  padding:16px 18px;margin:14px 0">
        <div style="font-size:15px;font-weight:700;color:#1e3a8a;margin-bottom:10px">
          📋 All holdings — flowchart states</div>
        <table style="border-collapse:collapse;width:100%;font-size:13px">
          <tr style="background:#1e3a8a;color:#fff">
            <th style="padding:8px 10px;text-align:left;border-radius:6px 0 0 0">Stock</th>
            <th style="padding:8px 10px;text-align:left">State</th>
            <th style="padding:8px 10px;text-align:left;border-radius:0 6px 0 0">Reason</th></tr>
          {trs}
        </table>
      </div>

      <div style="color:#94a3b8;font-size:11px;text-align:center;margin-top:14px">
        Generated by the alert engine · flowchart v1.0 (40W EMA) · prices via
        yfinance + official NSE/BSE files · benchmark: Nifty Smallcap 100 ·
        trends vs last week's snapshot</div>
    </div>"""

    send_email(f"Weekly Portfolio Digest{f' ({label})' if label else ''} — "
               f"{today.strftime('%d %b')}", html)

    # compact Telegram version of the same review
    try:
        tg = [f"🗓 <b>Weekly digest</b> · {today.strftime('%d %b')}"]
        for pf, sec in zip(pf_ids, pf_sections):
            det = detail_by_pf.get(pf)
            if det is None:
                continue
        for pf in pf_ids:
            if tg_pf_ids is not None and pf not in tg_pf_ids:
                continue                      # Telegram teaser: Lakshmi/Abinaya only
            det = detail_by_pf.get(pf)
            if det is None:
                continue
            snap = client.table("digest_history").select("*") \
                .eq("portfolio_id", pf).eq("snap_date", today.isoformat()) \
                .limit(1).execute().data
            if snap:
                s = snap[0]
                x = f" · XIRR {float(s['xirr']):.1f}%" if s.get("xirr") is not None else ""
                tg.append(f"<b>{PF_NAME.get(pf, pf)}</b>: {_fmt_l(s['current_value'])} "
                          f"({float(s['unrealised'])/float(s['invested'])*100:+.1f}%){x}")
            # Lakshmi's weekly scorecard — the line he judges on
            wk = weekly_by_pf.get(pf)
            if wk:
                icon = "✅" if wk["alpha"] >= WEEKLY_ALPHA_BAR else (
                    "🟠" if wk["alpha"] >= 0 else "🔴")
                tg.append(f"   {icon} week: <b>{wk['pf_ret']:+.2f}%</b> vs index "
                          f"{wk['idx_ret']:+.2f}% → alpha <b>{wk['alpha']:+.2f} pts</b>")
        tg_exits = exits
        if tg_pf_ids is not None:               # keep only in-scope portfolios' exits
            allowed = tuple(f"[{PF_NAME.get(p)}]" for p in tg_pf_ids) + ("[Both]",)
            tg_exits = [e for e in exits if e.startswith(allowed)]
        if tg_exits:
            tg.append("🔴 EXIT: " + ", ".join(tg_exits))
        if dead_money:
            tg.append(f"💤 {len(dead_money)} stock(s) on dead-money watch")
        tg.append("Full review in the email 📧")
        chat = chat_id_for_group("lakshmi")
        if telegram and chat:            # Vishal's email-only digest sends no teaser
            send_telegram("\n".join(tg), chat_id=chat)
    except Exception as ex:
        print(f"(digest: telegram summary failed: {ex})")
    print(f"Digest sent: {len(rows)} holdings, {len(exits)} exits, {len(cautions)} cautions.")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "states"
    if mode == "fast-poll":
        # optional args: minutes, interval-seconds
        mins = float(sys.argv[2]) if len(sys.argv) > 2 else 16.0
        secs = int(sys.argv[3]) if len(sys.argv) > 3 else 60
        run_fast_poll(minutes=mins, interval=secs)
    else:
        {"states": run_states,
         "filings": run_filings,
         "filings-nse": lambda: run_filings(nse_only=True),
         "filings-audit": run_filings_audit,
         "deals": run_deals,
         "calendar": run_calendar,
         "digest": run_digest,
         "reconcile": run_reconcile,
         "morning-brief": run_morning_brief,
         "eod-entries": run_eod_entries}.get(mode, run_states)()
