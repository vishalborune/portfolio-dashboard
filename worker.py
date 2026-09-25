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
  • BSE filings   every 1h    08:30–23:00 — BSE's own API is dead (Akamai), so
                                            BSE-only names are scraped from
                                            Screener.in (~7 light requests/sweep);
                                            Screener ingests filings within minutes
                                            so hourly gives ~1h latency

Everything heavy/nightly (bhavcopy, deals, evening EOD pass, exit audit, weekly
digest) stays on GitHub Actions for now. Dedup (entry_alert_log / filings_seen)
means the two running side by side can never double-alert — so this can be
switched on with zero risk, and GitHub acts as a backstop.

The 10/21-DMA, peak and 10-week-EMA levels are computed ONCE per trading day and
reused for every 60s cycle — the loop only fetches a cheap live quote.

ENV: SUPABASE_URL, SUPABASE_SERVICE_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
     ANTHROPIC_API_KEY (optional — filing summaries)
     RESEND_API_KEY + DIGEST_EMAILS — REQUIRED since the weekly digest moved here
       (28-Aug-2026). Without them the digest builds, posts its Telegram teaser and
       sends NO EMAIL: exactly what happened the first Friday it ran here.
     ALERTS_DRY_RUN=1  -> print what WOULD be sent, write nothing (safe first run)
"""
from __future__ import annotations

import gc
import time
import ctypes
import ctypes.util
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import alerts

IST = ZoneInfo("Asia/Kolkata")

_LIBC = None
try:
    _LIBC = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
except Exception:
    _LIBC = None


def _reclaim_memory():
    """Hand freed heap back to the OS. On the 512 MB Render worker, glibc's malloc
    keeps freed blocks in its arena — Python releases the objects but RSS stays
    high and creeps up over hours of the loop until an OOM auto-restart (Render
    alerts, 04/05-Aug-2026). gc.collect() drops cyclic garbage (big PDF-summary /
    RSS-parse transients); malloc_trim(0) returns the now-free pages to the kernel
    so RSS actually falls. No-op on non-glibc platforms (e.g. local Windows)."""
    gc.collect()
    if _LIBC is not None and hasattr(_LIBC, "malloc_trim"):
        try:
            _LIBC.malloc_trim(0)
        except Exception:
            pass

LIVE_INTERVAL = 60          # seconds between live price checks
NSE_FILINGS_INTERVAL = 180  # 3 minutes
BSE_FILINGS_INTERVAL = 3600 # 1 hour (BSE-only names via Screener; light + fast)
TICK = 5                    # main loop granularity (seconds)

MARKET_OPEN = (9, 10)       # a few minutes before the 09:15 open
MARKET_CLOSE = (15, 35)
FILINGS_OPEN = (8, 30)
FILINGS_CLOSE = (23, 0)
BRIEF_OPEN = (8, 30)        # morning 'today's agenda' brief window (once/day)
BRIEF_CLOSE = (9, 15)
LEVELS_OPEN = (8, 45)       # morning 5/10-DMA order levels (once/day, WEEKDAYS:
LEVELS_CLOSE = (9, 12)      # it exists to place orders, so no point Sat/Sun)
DIGEST_OPEN = (21, 0)       # weekly digest — FRIDAY 21:00 IST, after the 20:00/20:30
DIGEST_CLOSE = (23, 30)     # data jobs. Retried inside this window until it succeeds.
DIGEST_RETRY = 1800         # 30 min between attempts
BHAV_OPEN = (20, 5)         # nightly bhavcopy price store — the digest and every
BHAV_CLOSE = (23, 30)       # EOD alert depend on it, so it lives HERE, not only
BHAV_RETRY = 900            # on GitHub's best-effort cron (which skipped 04-Sep
                            # and starved the digest into a reconcile block).
US_STORE_OPEN = (7, 0)      # US EOD store (Vishal's US book, 26-Sep-2026): the US
US_STORE_CLOSE = (9, 30)    # session closes 01:30/02:30 IST, so the morning AFTER
US_STORE_RETRY = 900        # — Tue..Sat IST (Sat stores Friday's close).
US_FILINGS_INTERVAL = 900   # SEC EDGAR poll every 15 min, 7 days (filings accepted
                            # 06:00-22:00 ET = 15:30-07:30 IST; dedup makes it cheap)
US_EARN_OPEN = (18, 30)     # earnings-date brief, once a day before the US open
US_EARN_CLOSE = (19, 15)


def _now():
    return datetime.now(IST)


def _within(now, start, end, weekends=False) -> bool:
    """Is (hh, mm) inside [start, end]? Weekday-only unless weekends=True.
    Market/live checks stay weekday (market's shut Sat/Sun) but FILINGS run 7
    days — companies file board-meeting outcomes/results on Saturdays and NSE's
    feed is a ~1-day snapshot, so a weekend filing would age off before Monday's
    first weekday run and be MISSED (found 01-Aug-2026: 13 holdings' filings sat
    unpolled on a Saturday)."""
    if not weekends and now.weekday() >= 5:
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

    # Startup credential check. The worker ran the digest for the first time on
    # 28-Aug-2026, posted its Telegram teaser, and delivered NO EMAIL because
    # RESEND_API_KEY was not in its env — and nobody could have known until the
    # email failed to arrive. So say it plainly at boot, where the Render log
    # shows it immediately, instead of discovering it once a week at 21:00.
    import os as _os
    _mail = bool(_os.environ.get("RESEND_API_KEY")) and bool(_os.environ.get("DIGEST_EMAILS"))
    print(f"[worker] telegram: {'configured' if _os.environ.get('TELEGRAM_BOT_TOKEN') else 'MISSING'}"
          f" | email (weekly digest): {'configured' if _mail else 'NOT CONFIGURED'}")
    if not _mail:
        print("⚠️ [worker] RESEND_API_KEY / DIGEST_EMAILS missing — the Friday digest "
              "will post its Telegram teaser and send NO EMAIL. Add both in the "
              "Render service's Environment.")

    client = alerts.sb()
    levels, wema, levels_day = {}, {}, None
    last_live = last_nse = last_bse = 0.0
    last_beat = 0.0
    brief_day = None            # morning brief runs once per calendar day
    bhav_day = None             # nightly price store done for this date
    last_bhav_try = 0.0         # its retry throttle (file can publish late)
    us_store_day = None         # US EOD store done for this (IST) date
    last_us_store_try = 0.0
    us_levels, us_wema, us_levels_day = {}, {}, None   # US live poller (NY clock)
    last_us_live = 0.0
    last_us_filings = 0.0       # SEC EDGAR poll throttle
    us_earn_day = None          # earnings brief once per day
    # NOTE: deliberately NOT called `levels_day` — that name is already taken by
    # the fast-poll level cache above, which sets it at 08:30. Reusing it meant
    # this digest's `!= today` guard was already satisfied by 08:45 and it never
    # fired once (11-Aug-2026, Lakshmi got no alert).
    morning_levels_day = None   # morning 5/10-DMA order levels — once per day
    last_digest_try = 0.0       # weekly digest attempt throttle (Friday evening)

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

            # ---- US book: live entry/add checks on NEW YORK hours -----------
            # Same fast_cycle as India, run under alerts.set_market("US"):
            # US holdings/watchlist only, Finnhub quotes, $ in messages, the
            # US Telegram group. Levels are computed once per NY session from
            # our own stored bars. try/finally so the market context can never
            # leak into the next Indian pass.
            try:
                import usprices as _usp
                _ny_today = datetime.now(_usp.NY).date()
                if (_usp.market_is_open() or (datetime.now(_usp.NY).hour == 9)) and us_levels_day != _ny_today:
                    alerts.set_market("US")
                    try:
                        print(f"[worker] computing US levels for {_ny_today}…")
                        us_levels, us_wema = alerts.compute_fast_levels(client)
                        us_levels_day = _ny_today
                    finally:
                        alerts.set_market("IN")
                if (_usp.market_is_open() and us_levels
                        and time.time() - last_us_live >= LIVE_INTERVAL):
                    last_us_live = time.time()
                    alerts.set_market("US")
                    try:
                        priced = alerts.fast_cycle(client, us_levels, us_wema)
                        print(f"[{now:%H:%M:%S}] US live: {priced}/{len(us_levels)} priced")
                    finally:
                        alerts.set_market("IN")
            except Exception as e:
                alerts.set_market("IN")
                print(f"⚠️ [worker] US live cycle failed: {type(e).__name__}: {e}")

            # ---- US filings (SEC EDGAR, every 15 min, 7 days) + earnings brief --
            if time.time() - last_us_filings >= US_FILINGS_INTERVAL:
                last_us_filings = time.time()
                try:
                    import usfilings
                    n = usfilings.run(alerts.sb())
                    if n:
                        print(f"[{now:%H:%M:%S}] US filings: {n} alert(s)")
                except Exception as e:
                    print(f"⚠️ [worker] US filings failed: {type(e).__name__}: {e}")
                _reclaim_memory()
            if _within(now, US_EARN_OPEN, US_EARN_CLOSE) and us_earn_day != today:
                us_earn_day = today
                try:
                    import usfilings
                    usfilings.earnings_brief(alerts.sb())
                except Exception as e:
                    print(f"⚠️ [worker] US earnings brief failed: {type(e).__name__}: {e}")

            # ---- morning 'today's agenda' brief (once/day ~08:30, 7 days) ---
            if _within(now, BRIEF_OPEN, BRIEF_CLOSE, weekends=True) and brief_day != today:
                brief_day = today
                try:
                    alerts.run_morning_brief()
                except Exception as e:
                    print(f"⚠️ [worker] morning brief failed: {type(e).__name__}: {e}")

            # ---- morning 5/10-DMA order levels (once/day, weekdays only) ----
            if _within(now, LEVELS_OPEN, LEVELS_CLOSE) and morning_levels_day != today:
                morning_levels_day = today
                try:
                    alerts.run_morning_levels()
                except Exception as e:
                    print(f"⚠️ [worker] morning levels failed: {type(e).__name__}: {e}")

            # ---- US EOD price store (Tue..Sat morning IST) -----------------
            # Vishal's US book (portfolio 4). Yahoo daily bars -> our own table,
            # then a health check against Finnhub's independent close, reported
            # to the US Telegram group. Idempotent upsert; GitHub's 07:10 cron
            # backstops it from a different IP in case Render's is blocked.
            if (now.weekday() in (1, 2, 3, 4, 5)
                    and _within(now, US_STORE_OPEN, US_STORE_CLOSE, weekends=True)
                    and us_store_day != today
                    and time.time() - last_us_store_try >= US_STORE_RETRY):
                last_us_store_try = time.time()
                try:
                    import usprices
                    _c = alerts.sb()
                    n = usprices.store(_c)
                    if n:
                        us_store_day = today
                        try:
                            import signals
                            for fn_name in ("fetch_weekly", "_fetch_daily"):
                                fn = getattr(signals, fn_name, None)
                                if fn is not None and hasattr(fn, "clear"):
                                    fn.clear()
                        except Exception:
                            pass
                        usprices.health(_c)
                        try:
                            import usfundamentals
                            usfundamentals.update_all(_c)
                        except Exception as e:
                            print(f"⚠️ [worker] US fundamentals failed: {type(e).__name__}: {e}")
                        _reclaim_memory()      # companyfacts JSONs are ~4 MB each
                        # State changes + EOD entry/stop pass for the US book,
                        # off the closes just stored (US equivalent of the
                        # 20:47 states + 20:20 eod-entries runs).
                        alerts.set_market("US")
                        try:
                            alerts.run_states()
                            alerts.run_eod_entries()
                        finally:
                            alerts.set_market("IN")
                    else:
                        print(f"[worker] usprices: nothing stored for {today} — will retry")
                except Exception as e:
                    print(f"⚠️ [worker] US price store failed: {type(e).__name__}: {e}")
                    traceback.print_exc()

            # ---- nightly bhavcopy price store (WEEKDAYS, evening) ----------
            # Moved here 04-Sep-2026: GitHub's 20:00 cron silently skipped, our
            # stored prices ended a day earlier, and the Friday digest was
            # (rightly) blocked by its own reconciliation guard — no Telegram,
            # no email. The digest can't be more reliable than the data job it
            # depends on, so the data job now runs on the same always-on worker,
            # BEFORE the digest block in this loop. GitHub's 20:00 cron stays as
            # a backstop; the store is an idempotent upsert so both can run.
            if (now.weekday() < 5 and _within(now, BHAV_OPEN, BHAV_CLOSE)
                    and bhav_day != today
                    and time.time() - last_bhav_try >= BHAV_RETRY):
                last_bhav_try = time.time()
                try:
                    import bhavcopy
                    _c = alerts.sb()
                    uni = bhavcopy.tracked_universe(_c)
                    prices = bhavcopy.extract_prices_for_date(today, universe=uni)
                    # Benchmark index closes ride the same store — miss them and
                    # the weekly scorecard measures Fri->Thu against a Fri->Fri
                    # portfolio (the -0.15%-vs-+0.08% wrong-week of 04-Sep-2026).
                    prices.update(bhavcopy.fetch_index_closes(today))
                    if prices:
                        bhavcopy.store_prices(_c, today, prices)
                        print(f"[worker] bhavcopy: stored {len(prices)} closes "
                              f"for {today}")
                        bhav_day = today
                        # The digest attempt at 21:00 may already have CACHED
                        # weekly bars built from yesterday's table (in-process
                        # 4h TTL) — drop them so its next retry recomputes on
                        # tonight's closes instead of blocking until 23:30.
                        try:
                            import signals
                            for fn_name in ("fetch_weekly", "_fetch_daily"):
                                fn = getattr(signals, fn_name, None)
                                if fn is not None and hasattr(fn, "clear"):
                                    fn.clear()
                        except Exception:
                            pass
                        bhavcopy.report_health(_c)
                    else:
                        print(f"[worker] bhavcopy: no rows for {today} yet "
                              f"(holiday or file not published) — will retry")
                except Exception as e:
                    print(f"⚠️ [worker] bhavcopy store failed: "
                          f"{type(e).__name__}: {e}")
                    traceback.print_exc()
                _reclaim_memory()      # the two exchange files are big downloads

            # ---- weekly digest (FRIDAY evening) ----------------------------
            # Moved here 28-Aug-2026: GitHub Actions' scheduler dropped the Friday
            # digest entirely and Vishal got no email. Same best-effort behaviour
            # that moved the live alerts onto this worker — a process that is
            # ALREADY RUNNING doesn't need anyone to launch it.
            # Retries every 30 min inside the window rather than firing once, so a
            # transient failure (or a reconciliation block that gets fixed) still
            # lands. alerts.run_digest_if_due() checks whether today's snapshot is
            # already stored, so the GitHub backstop can never double-send.
            if (now.weekday() == 4 and _within(now, DIGEST_OPEN, DIGEST_CLOSE)
                    and time.time() - last_digest_try >= DIGEST_RETRY):
                last_digest_try = time.time()
                try:
                    # SUBPROCESS with a hard timeout (18-Sep-2026): the in-loop
                    # call hung at its 21:00 attempt and froze the ENTIRE worker
                    # loop — filings stopped at 20:42, no retry ever happened,
                    # nothing was sent and nothing was logged, because the loop
                    # itself was stuck inside run_digest. A child process cannot
                    # freeze the loop (the timeout kills it), a crash inside it
                    # cannot take the worker down, and its peak memory — the
                    # digest is the heaviest job on this 512 MB box — returns to
                    # the OS on exit instead of joining the malloc arena.
                    import subprocess, sys as _sys, os as _os
                    r = subprocess.run(
                        [_sys.executable, "-u", "alerts.py", "digest-if-due"],
                        timeout=1200,
                        cwd=_os.path.dirname(_os.path.abspath(__file__)))
                    print(f"[worker] digest-if-due subprocess exited {r.returncode}")
                except Exception as e:
                    print(f"⚠️ [worker] weekly digest failed: {type(e).__name__}: {e}")
                    traceback.print_exc()
                _reclaim_memory()      # digest builds big HTML + a full price sweep

            # ---- NSE filings (7 days — companies file on weekends too) ------
            if (_within(now, FILINGS_OPEN, FILINGS_CLOSE, weekends=True)
                    and time.time() - last_nse >= NSE_FILINGS_INTERVAL):
                last_nse = time.time()
                try:
                    alerts._NSE_RSS_CACHE = None      # force a fresh feed pull
                    alerts.run_filings(nse_only=True)
                except Exception as e:
                    print(f"⚠️ [worker] NSE filings failed: {type(e).__name__}: {e}")
                alerts._NSE_RSS_CACHE = None          # release the ~2000-item feed
                _reclaim_memory()                     # RSS-parse transients -> back to OS

            # ---- BSE filings (hourly, 7 days — BSE-only names scraped from
            #      Screener since BSE's own API is dead; weekends included
            #      because results/board outcomes file on Saturdays) ----------
            if (_within(now, FILINGS_OPEN, FILINGS_CLOSE, weekends=True)
                    and time.time() - last_bse >= BSE_FILINGS_INTERVAL):
                last_bse = time.time()
                try:
                    alerts._NSE_RSS_CACHE = None
                    alerts.run_filings()              # full run, incl BSE-via-Screener
                except Exception as e:
                    print(f"⚠️ [worker] BSE filings failed: {type(e).__name__}: {e}")
                alerts._NSE_RSS_CACHE = None          # release the feed + PDF transients
                _reclaim_memory()

            # ---- heartbeat so the logs show it's alive ----------------------
            if time.time() - last_beat >= 900:
                last_beat = time.time()
                where = ("market hours" if _within(now, MARKET_OPEN, MARKET_CLOSE)
                         else ("filings window" if _within(now, FILINGS_OPEN, FILINGS_CLOSE, weekends=True)
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
