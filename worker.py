#!/usr/bin/env python
"""worker.py — the always-on alert engine (Render Background Worker).

WHY THIS EXISTS
GitHub Actions' scheduler is best-effort and was silently dropping the morning
runs: on 23-Jul-2026 there was NOT ONE scheduled run between 23:22 the previous
night and 11:20 the next morning, so Lakshmi's alerts arrived at 11:22 instead of
before the 09:15 open — twice in a row, even after moving every cron to off-peak
minutes. A process that is ALREADY RUNNING doesn't need anyone to launch it.

RHYTHMS (all IST, Mon–Fri)
  • live checks   every 60s   09:10–15:35 — 21-DMA / 10-week EMA touch /
                                            watchlist zones / risk stops
  • NSE filings   every 3min  08:30–23:00 — friendly archives host, safe to poll
  • BSE filings   every 2h    08:30–23:00 — its API is bot-hostile AND shares the
                                            runner IP with the SME bhavcopy, so
                                            it stays deliberately gentle

Everything heavy/nightly (bhavcopy, deals, evening EOD pass, exit audit, weekly
digest) stays on GitHub Actions for now. Dedup (entry_alert_log / filings_seen)
means the two running side by side can never double-alert — so this can be
switched on with zero risk, and GitHub acts as a backstop.

The 10/21-DMA, peak and 10-week-EMA levels are computed ONCE per trading day and
reused for every 60s cycle — the loop only fetches a cheap live quote.

ENV: SUPABASE_URL, SUPABASE_SERVICE_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
     ANTHROPIC_API_KEY (optional — filing summaries)
     ALERTS_DRY_RUN=1  -> print what WOULD be sent, write nothing (safe first run)
"""
from __future__ import annotations

import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import alerts

IST = ZoneInfo("Asia/Kolkata")

LIVE_INTERVAL = 60          # seconds between live price checks
NSE_FILINGS_INTERVAL = 180  # 3 minutes
BSE_FILINGS_INTERVAL = 7200 # 2 hours
TICK = 5                    # main loop granularity (seconds)

MARKET_OPEN = (9, 10)       # a few minutes before the 09:15 open
MARKET_CLOSE = (15, 35)
FILINGS_OPEN = (8, 30)
FILINGS_CLOSE = (23, 0)


def _now():
    return datetime.now(IST)


def _within(now, start, end) -> bool:
    """Is (hh, mm) inside [start, end] on a weekday?"""
    if now.weekday() >= 5:
        return False
    return start <= (now.hour, now.minute) <= end


def main():
    dry = alerts._dry()
    print("=" * 68)
    print(f"[worker] starting {_now():%Y-%m-%d %H:%M:%S} IST"
          f"{'  (DRY-RUN — nothing will be sent or written)' if dry else ''}")
    print(f"[worker] live every {LIVE_INTERVAL}s {MARKET_OPEN}-{MARKET_CLOSE} | "
          f"NSE filings every {NSE_FILINGS_INTERVAL}s | "
          f"BSE filings every {BSE_FILINGS_INTERVAL // 3600}h "
          f"{FILINGS_OPEN}-{FILINGS_CLOSE} IST")
    print("=" * 68)

    client = alerts.sb()
    levels, wema, levels_day = {}, {}, None
    last_live = last_nse = last_bse = 0.0
    last_beat = 0.0

    while True:
        try:
            now = _now()
            today = now.date()

            # ---- refresh the day's levels once, just before the open --------
            if _within(now, FILINGS_OPEN, MARKET_CLOSE) and levels_day != today:
                print(f"[worker] computing levels for {today}…")
                try:
                    levels, wema = alerts.compute_fast_levels(client)
                    levels_day = today
                except Exception as e:
                    print(f"⚠️ [worker] level computation failed: {type(e).__name__}: {e}")
                    traceback.print_exc()

            # ---- live price checks (market hours) ---------------------------
            if (_within(now, MARKET_OPEN, MARKET_CLOSE) and levels
                    and time.time() - last_live >= LIVE_INTERVAL):
                last_live = time.time()
                try:
                    priced = alerts.fast_cycle(client, levels, wema)
                    print(f"[{now:%H:%M:%S}] live: {priced}/{len(levels)} priced")
                except Exception as e:
                    print(f"⚠️ [worker] live cycle failed: {type(e).__name__}: {e}")

            # ---- NSE filings ------------------------------------------------
            if (_within(now, FILINGS_OPEN, FILINGS_CLOSE)
                    and time.time() - last_nse >= NSE_FILINGS_INTERVAL):
                last_nse = time.time()
                try:
                    alerts._NSE_RSS_CACHE = None      # force a fresh feed pull
                    alerts.run_filings(nse_only=True)
                except Exception as e:
                    print(f"⚠️ [worker] NSE filings failed: {type(e).__name__}: {e}")

            # ---- BSE filings (gentle) ---------------------------------------
            if (_within(now, FILINGS_OPEN, FILINGS_CLOSE)
                    and time.time() - last_bse >= BSE_FILINGS_INTERVAL):
                last_bse = time.time()
                try:
                    alerts._NSE_RSS_CACHE = None
                    alerts.run_filings()              # full run, incl BSE
                except Exception as e:
                    print(f"⚠️ [worker] BSE filings failed: {type(e).__name__}: {e}")

            # ---- heartbeat so the logs show it's alive ----------------------
            if time.time() - last_beat >= 900:
                last_beat = time.time()
                where = ("market hours" if _within(now, MARKET_OPEN, MARKET_CLOSE)
                         else ("filings window" if _within(now, FILINGS_OPEN, FILINGS_CLOSE)
                               else "idle (outside hours)"))
                print(f"[{now:%Y-%m-%d %H:%M}] heartbeat — {where}, "
                      f"{len(levels)} tickers armed")

            time.sleep(TICK)

        except KeyboardInterrupt:
            print("[worker] stopping (interrupt)")
            return
        except Exception as e:
            # Never let one bad iteration kill the process — Render would restart
            # it, but staying up means we don't miss the next cycle either.
            print(f"⚠️ [worker] loop error: {type(e).__name__}: {e}")
            traceback.print_exc()
            time.sleep(30)


if __name__ == "__main__":
    main()
