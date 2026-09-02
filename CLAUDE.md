# Portfolio Dashboard — Project Memory

Real-time Indian smallcap stock portfolio dashboard for a household of 3 users.
Built iteratively over several sessions in Claude.ai chat; migrating to Claude Code
for continuity. This file is the "memory" that chat couldn't reliably carry forward.

## Who this is for
- **Vishal** (non-technical, Chennai, Windows) — builds/maintains this, portfolio_id 1
- **Lakshmi** — primary active investor, portfolio_id 2 (~₹3.3 Cr), trades via Kite,
  uses a weekly 10/20/40-week EMA flowchart methodology ("TheWrap" TA rules) for
  entries/exits, staged entries at 10-DMA (tranche 1) / 21-DMA (tranche 2 & final)
- **Abinaya** — Lakshmi's partner, portfolio_id 3 (~₹1.5 Cr), trades via Upstox

## Stack
- **DB**: Supabase (Postgres). **KNOWN GOTCHA: silent 1,000-row query cap.**
  Always query `ORDER BY ... DESC` for time-series data so the cap trims
  history, never recent data. Never assume an ascending query is complete.
- **Hosting**: Render — TWO Streamlit Web Services from the same repo (distinguished
  by `APP_TENANT` env var "vishal" / "lakshmi") PLUS one **Background Worker**
  (`worker.py`, added 23-Jul-2026) that is now the PRIMARY runtime for live alerts
  + filings. Web services on free tier (watch memory/perf); the worker is Starter
  (~$7/mo) because free workers sleep.
- **CI/CD**: GitHub Actions, single workflow `.github/workflows/alerts.yml`
- **App**: Streamlit (`app.py`), Python throughout
- **Alerting**: Telegram (bot + group chat), email via Resend
- **AI summaries**: Claude Haiku (claude-haiku-4-5-20251001) reads filing PDFs
  natively via the API's document block — no OCR/text-extraction pipeline

## Files (repo root unless noted)
| File | Purpose |
|---|---|
| `app.py` | Streamlit dashboard — holdings, watchlist, journal, fundamentals |
| `db.py` | All Supabase reads/writes |
| `notify.py` | Telegram + email (Resend) delivery. `send_telegram` RETURNS False on failure (doesn't raise) — callers must check it. Honors `ALERTS_DRY_RUN`. |
| `xirr.py` | XIRR (money-weighted return) for the dashboard/digest |
| `backtest.py` | Offline flowchart backtest helper (not in any scheduled job) |
| `app_lakshmi.py` | Tiny entry shim for the lakshmi tenant |
| `signals.py` | Weekly EMA flowchart states, entry-zone (DMA) math |
| `alerts.py` | The alert engine — states, volume spikes, watchlist entries, filings, digest |
| `bhavcopy.py` | Official NSE/BSE daily price files — the Yahoo-blind-spot fix |
| `delivery.py` | NSE + BSE daily delivery % pipeline |
| `fundamentals.py` | Screener.in scraper — Market Cap/PE/Book Value (Yahoo `.info` is blocked) |
| `exit_audit.py` | 30/60/90-day post-exit price checks |
| `corporate_actions.py` | Split/bonus adjustment for bhavcopy prices + unadjusted-gap detector (see House Rule 10) |
| `dryrun.py` | Test-run any alert mode against live data, PRINTING what would be sent — no Telegram, no DB writes (`python dryrun.py deals\|eod-entries\|filings-nse\|states\|fast-poll\|digest`) |
| `screener_data.py` | Deterministic screener.in parser — quarterly table, annual P&L, ROCE/ROE, promoter/pledge trend. **Python computes every QoQ/YoY %.** `python screener_data.py HFCL` |
| `thesis.py` | Stage-2 thesis scorecard (7 pillars /14 + verdict). Opus 5 + web search researches only DU/SM/GV; RQ/MG/VA come from screener_data, TE is scored in Python. `python thesis.py "<name>" [--reason ..] [--send]` |
| `worker.py` | **The always-on alert engine (Render Background Worker).** Live checks every 60s in market hours (weekdays) + NSE filings every 3 min + BSE filings every 2h **7 DAYS** (companies file board-meeting outcomes/results on Saturdays; the RSS feed is a ~1-day snapshot so a weekend filing ages off before Monday — `_within(..., weekends=True)` for filings, weekday-only for live/market checks; 01-Aug-2026, Vishal caught it). Exists because GitHub Actions' scheduler DROPPED entire mornings (23-Jul-2026: no scheduled run between 23:22 and 11:20). Shares `alerts.compute_fast_levels` / `alerts.fast_cycle` with the GitHub job so the two can never diverge. Start command must be `python -u worker.py` — without `-u` Python buffers stdout and the Render logs look dead. |
| `.github/workflows/alerts.yml` | The single CI workflow — see Schedule below |
| ~~`*_schema.sql`~~ | (Historical — schema was applied to Supabase directly; no `.sql` files are committed in the repo.) |

## THE CORE HOUSE RULES (learned the hard way — do not violate)
1. **Yahoo Finance is unreliable for: SME/Emerge stocks, most indices, and
   `.info`/fundamentals calls from datacenter IPs (Render/GitHub Actions).**
   It has gone from "sometimes fails" to "100% blocked" more than once.
   Default posture: **own the data** — fetch from NSE/BSE's own official
   daily files (bhavcopy) instead of relying on Yahoo for anything price-
   or-fundamentals related on smallcap/SME names. Yahoo is fine for
   ordinary NSE mainboard live quotes.
2. **A wrong number is worse than a blank one.** Every parser should have
   an identity/sanity check (e.g. fundamentals.py's page-title match)
   rather than silently accepting a plausible-looking wrong value.
3. **Every fetch failure logs WHY** (HTTP status, byte count, a content
   snippet) — never just "failed" or empty-and-silent. This is the
   difference between diagnosing in 2 minutes vs. burning hours.
4. **Never couple a new/fragile fetch into the longest-running job.**
   (Lesson from bolting an index-fetch into the 2-year backfill: turned a
   1hr job into 5.5hrs for zero rows.) New fragile things get their OWN
   fast diagnostic mode first (see `python bhavcopy.py check`), proven
   before going anywhere near a backfill.
5. **Verify a symbol/URL against the REAL file before trusting a web
   search's claim about what exchange something trades on.** Multiple
   times this session, search said "NSE-listed" and the real bhavcopy file
   proved it BSE-only (or vice versa). Ground truth = the actual exchange
   file, not an article.
6. **Cast numpy/pandas types to plain Python (`float()`, `int()`) at every
   DB write boundary.** `int64`/`float64` silently fail Supabase's JSON
   serialization — caught this bug live in the digest snapshot code.
7. **A "non-empty" API response isn't automatically a USABLE one.** Add
   minimum-length/stub guards (e.g. the digest benchmark rejects a Yahoo
   index series under 60 days) before letting a source "win" a fallback
   chain.
8. **Pandas `.sum()` silently treats NaN as 0** in aggregations — always
   handle missing prices explicitly (e.g. value at cost, not at NaN×qty=0).
9. **`@st.cache_data` on functions that depend on session state (like
   active portfolio) must include ALL discriminating params in the cache
   key** — caching just `_active_pf()` caused cross-portfolio bugs.
10. **bhavcopy stores RAW, split/bonus-UNADJUSTED prices.** A corporate
   action steps the price overnight (CWD's 4:1 bonus, ex-date 02-Jan-2026:
   ₹1970→₹415), leaving old high prices beside new low ones — which inflated
   the 40W EMA to ₹567 and fired a FALSE 🔴 EXIT while the stock never broke
   its real 40W EMA (~₹302). Fix (21-Jul-2026): `corporate_actions.py` holds a
   registry of events and adjusts pre-ex-date prices ON READ at the single
   chokepoint `db.get_sme_daily_prices` (raw DB rows stay untouched —
   authoritative + reversible). A >25% overnight-gap detector runs in the
   daily bhavcopy job and via `python corporate_actions.py`; any gap with no
   registry entry is flagged loudly (fail-visible). **To add an event: verify
   the ratio against the BSE/NSE filing (rule #5), append one dict to
   `CORPORATE_ACTIONS`.** `price_divisor` = shares multiplier (4:1 bonus or
   1:5 split → 5).
   **Second event caught 06-Aug-2026 — Time Technoplast (532856.BO) 1:1 bonus,
   ex/record 23-Sep-2025 (₹477.75→₹227.35), divisor 2.** It had been UNADJUSTED
   since the ticker was added (22-Jul-2026), inflating the 40wEMA to ₹211.1 and
   firing a **FALSE 🔴 EXIT** (close ₹207.3 just under it); adjusted, the 40wEMA
   is ₹189.95 and the real state is **MAINTAIN/ADD** (close above all EMAs). Same
   failure mode as CWD — verified vs multiple sources AND our own raw price step.
   Lesson: when a NEW bhavcopy ticker is added, its full backfill can contain an
   OLD unadjusted split/bonus — the detector will flag it on the next daily run,
   so act on that flag (don't let it recur silently every run).

## Dashboard performance: `db._bust()` must NOT clear market caches (01-Sep-2026)
It used to call `st.cache_data.clear()`, which drops EVERY cache in the app — including
`signals.fetch_weekly` (4h), `states_for_holdings` (4h), `fetch_entry_zones` (20m),
`fetch_live_prices`, fundamentals and delivery %. So saving ONE watchlist support level
threw away 3 years of weekly bars for all 73 tickers and the next render refetched the
lot from Yahoo. Vishal: *"everytime a stock is amended it takes so much time to load
again"* — bulk editing was effectively impossible.
Now it clears only `_USER_EDITED_CACHES` (realised, watchlist, notes, transactions,
journal). Market data doesn't change because a target price was typed; fundamentals and
delivery % are refreshed by the nightly jobs and snapshots by the Friday digest, so none
of them belong in a post-write invalidation. **When adding a new cached DB reader that
the USER edits, add its name to that tuple** — and never reintroduce a blanket clear.
Note this also compounded the Yahoo-blocked outage: every edit triggered a fresh
~73-ticker history storm from the 512 MB web service.

## Architecture: portfolio-scoping
Every table has `portfolio_id`. Alerts aggregate per (group, ticker) so a
stock BOTH Lakshmi and Abinaya hold gets ONE Telegram message tagged
`[Both]`, not two. Dedup (state changes, volume spikes, entry zones, filings)
is always keyed to include portfolio/group scope — never just ticker alone,
or one person's alert can suppress another's.
**Telegram routing: only `TELEGRAM_ALERT_GROUPS = {"lakshmi"}` receives pushes**
(alerts.py) — Vishal (portfolio 1, "vishal" group) opted OUT; his data still shows
on his dashboard and he can get the email digest, but no Telegram. So an alert
scoped only to the "vishal" group is computed then dropped at the send gate.
**This is exactly why filings must union ALL holders' groups per symbol** — a
stock held by both Vishal and Lakshmi must not be scoped to {vishal} alone (that
was the 23-Jul filings bug: Vishal's holdings row processed first → Lakshmi
silently got nothing). Every alert path aggregates per (group, ticker); run_filings
was the one that regressed and is now fixed.

## Known SME/BSE-only tickers requiring bhavcopy (not Yahoo)
`bhavcopy.py`'s `SME_STOCKS` dict is the single source of truth. Currently
tracks: OBSCP, TCL, UTSSAV, VIESL (NSE Emerge), SSEGL (NSE Emerge, series ST),
CWD-MS, HSIL-MT, TRUECOLORS, LEHAR, SGRL (BSE, matched by scrip_code — NOT
by symbol string), plus HDFCSML250.NS / MOSMALL250.NS (Nifty Smallcap 250
ETFs, used as the portfolio benchmark proxy — see below).
Added 22-Jul-2026 after the Kwality bogus-quote incident: the BSE holdings stored
by NUMERIC scrip code — **539997.BO (Kwality/KPL), 532856.BO (Time Technoplast),
532829.BO (Lehar), 542669.BO (BMW)** — were being priced by YAHOO. All four
verified present in the real BSE bhavcopy before adding (rule #5).
**Transition guard:** `signals.MIN_BHAV_DAILY_ROWS` (60) / `MIN_BHAV_WEEKS` (20)
— a newly-tracked ticker has ~0 bhavcopy rows until the backfill runs, and a few
bars would make junk EMAs/peaks, so bhavcopy only WINS once it has real history;
until then Yahoo's daily bars are used (they're fine — it's the LIVE quote that
lies). **After adding a ticker here, run the bhavcopy backfill** or it stays on
Yahoo. NOTE: Lehar is held by two people under two identifiers (XBOM:532829 =
Vishal, XBOM:LEHAR = Abinaya) — different portfolios, so not a duplicate.

## The benchmark (Lakshmi's rule: beat the index by 2-5+ pts or stop)
NSE discontinued public access to exact Nifty Smallcap 100 daily data
(their legacy `ind_close_all` CSV is dead — confirmed via uniform 404/503
across ~500 dates). Yahoo's `^CNXSC` returns either nothing or a useless
1-day stub. **Resolution: use HDFCSML250.NS / MOSMALL250.NS (Smallcap 250
ETFs) as a proxy**, priced via the normal daily bhavcopy job (no extra
fetch cost). Lakshmi signed off on this substitution. The digest computes
a **shadow portfolio**: replay the real cashflows (same dates, same rupees)
into the ETF instead of the actual stocks, XIRR that, subtract from real
XIRR = alpha. Handles partial index history (buys older than the series)
by approximating at the earliest available level — this UNDERSTATES alpha,
which is the conservative/safe direction. The email always labels which
benchmark source won (exact index / HDFC ETF / MO ETF) — never silently
switch sources without disclosure.

## Schedule
> **PRIMARY RUNTIME IS `worker.py` ON RENDER (23-Jul-2026).** Live checks every 60s
> (market hours) + NSE filings every 3 min + BSE filings every 2h (08:30–23:00 IST)
> run on the always-on worker, because GitHub's scheduler dropped whole mornings.
> The GitHub Actions crons below still exist as a BACKSTOP + the nightly data jobs;
> dedup (entry_alert_log / filings_seen) means worker + Actions can't double-alert.
> Actual GitHub cron times (converted from `.github/workflows/alerts.yml`, off-peak
> minutes to dodge lag): states ~09:12 & 09:42 then hourly to 15:12 IST; filings-nse
> every 15 min 08:38–22:23; filings-full (incl BSE) every 2h 10:10–22:10; deals 21:15;
> eod-entries 20:20; bhavcopy 20:00; exit-audit 20:30; calendar Mon 09:00; digest Fri 21:00.
> Dispatch tick-boxes: bhavcopy-backfill, delivery-backfill, index-backfill,
> send_digest_now, run_filings_now, run_symbol_check, run_fast_poll_now, dry_run.

> **GitHub scheduler caveat (learned 22-Jul-2026):** Actions cron is best-effort —
> runs get DELAYED or DROPPED at high-load minutes (`:00/:15/:30/:45` and the top
> of the hour). Alerts once landed at 11:15 instead of 09:15 because of this. All
> market-hours crons are therefore on ODD, spread-out minutes (fast-poll
> :03/:18/:33/:48, states :42, filings-nse :08/:23/:38/:53). This REDUCES lag but
> GitHub gives NO timing guarantee — the only way to guarantee market-open-sharp
> alerts is an always-on worker (Render background worker, not GitHub cron).
- Hourly, 9:45–15:45 IST, Mon-Fri: flowchart states + volume spikes (2x
  pace-adjusted). NOTE (21-Jul-2026): entry/add-zone alerts NO LONGER ride this
  hourly job — see the two dedicated modes below.
- **Every 15 min, market hours (`fast-poll`)**: LIVE mainboard entry/add-zone
  alerts, ~1-min latency (Lakshmi: alert speed = the app's core value). Each run
  loops ~16 min (relaunched by cron, so a crash self-heals). Design is storm-safe:
  `signals.daily_entry_levels` computes the 10/21-DMA ONCE per launch, then only a
  cheap live quote (`alerts._live_quotes`, Yahoo) is fetched each ~60s cycle and
  run through the SAME deduped `check_holding_adds`/`check_watchlist_entries` via
  an injected `price_fn`. SME names are SKIPPED here (no live feed).
- **10-week EMA touch (`alerts.check_wema_touch`, Lakshmi 22-Jul-2026)**: fires when
  a HOLDING arrives at its 10-week EMA — the weekly-system trend line he acts on.
  Level from `signals.weekly_ema10` (same weekly bars as the flowchart, so SME
  rides bhavcopy), computed ONCE per poller launch. Rides the same cadence as
  entries: mainboard live (~1 min), SME in the evening pass. Dedup kind W10EMA.
  **It is an EVENT, not a state:** price must be at/through the line NOW *and*
  have closed clearly above it previously. Plain proximity fired on 19 holdings
  in one day (the 10wEMA is a slow mean-reversion line stocks loiter around);
  requiring the approach cut it to 8 real arrivals. Don't "simplify" that away.
- **5-day EMA touch (`alerts.check_ema5_touch`, Lakshmi 02-Aug-2026)**: WATCHLIST-only
  fast-momentum timing — a name that TOUCHES its 5-DAY EMA and JUMPS. Same cadence
  (mainboard live ~1 min via `fast_cycle`; SME + final pass in `run_eod_entries`),
  `ema5` added to `signals.daily_entry_levels`, dedup kind EMA5. **The 5-EMA is
  faster than the 10wEMA — price hugs it DAILY — so an even stricter EVENT gate:**
  fires only when (a) the day's LOW reached the 5-EMA (dipped), (b) CMP closed
  **≥`JUMP_MIN`=1% ABOVE** it AND up on the day (a real bounce, not just holding),
  AND (c) CMP > 21-DMA (uptrend). A plain proximity/hold gate fired on 6/15
  watchlist names in one day; the 1% jump gate cut it to 4 decisive bounces.
  `JUMP_MIN` is the tuning knob (lower = more alerts). Don't drop the dipped/jumped/
  uptrend trio — that's what keeps this off nearly-every-trending-stock-daily. The
  **watchlist dashboard also shows a `% vs 5DMA` column** (02-Aug-2026) next to
  10/21 — `signals.daily_entry_state`/`entry_states_for_watchlist` carry it (the
  alert is Telegram; this is the on-screen view Lakshmi expected to see).
- **States + risk stops moved to ONCE DAILY, evening (Lakshmi 15-Aug-2026)**: he
  wants state changes and stop-losses once a day, not hourly/per-minute. `states`
  cron is now a single **~20:47 IST weekday** run (was hourly 09:12–15:12 plus a
  09:42 second shot) and `check_risk_stops` was REMOVED from `fast_cycle` — it now
  runs only in `run_eod_entries` (20:20). **Why evening:** after bhavcopy (20:00)
  so SME/BSE names price off official exchange files; on SETTLED closes, which is
  also methodologically right since `peak` is a max of CLOSES; and every false EXIT
  we have fixed (HFCL, CWD, Time Technoplast) came from intraday or stale bars, so
  removing intraday evaluation removes a whole class of false signal. The rhythm is
  **evening = decide, morning = execute** — the 08:45 levels digest gives the prices
  to act on next morning. NOTE: when changing a cron, the job's `if:
  github.event.schedule == '...'` MUST be updated to match or the job silently never
  runs (nearly shipped exactly that here).
- **Watchlist SUPPORT alerts (`alerts.check_support_touch`, Lakshmi 15-Aug-2026)**:
  fires when a WATCHLIST name trades within `SUPPORT_NEAR_PCT`=2% of either level
  from `signals.support_levels`: **minor** (lowest daily low of the last
  `MINOR_SUPPORT_DAYS`=25 bars — the near-term shelf, kind SUPMIN) or **major** (the
  WEEKLY swing-pivot `support` the flowchart already computes — the structural
  floor, kind SUPMAJ). Both reported when both are in range; separate dedup kinds so
  neither suppresses the other. Unlike `check_ema5_touch` this is deliberately
  PROXIMITY, not a dip-and-jump event — he wants to place a limit order as price
  APPROACHES support, not after the bounce. Rides the live poller + the evening EOD
  pass (the only pass for SME watchlist names). **Expect these to be QUIET in a
  rising market by design** — measured 15-Aug-2026, all 6 watchlist names sat
  13–55% above support. Shallow pullbacks are already covered by the 10/21-DMA ZONE
  alerts; these are the DEEP-support ones.
  - **LEVELS ARE NOW MANUAL, entered per stock on the watchlist (Lakshmi 16-Aug-2026).**
    They were originally DERIVED (minor = lowest daily low of 25 bars; major = the weekly
    5-week CENTERED swing-pivot low from `compute_indicators`). He asked how that worked
    and concluded it *"might not make sense to predict it easily for different stocks"* —
    correctly: on HFCL those rules put **major (₹193.92) ABOVE minor (₹181.05)**, because
    a lone intraday wick set the minor and no weekly close ever confirmed it. Defensible
    per-rule, nonsense per-chart. He reads the chart, he types the level.
    **`alerts.SUPPORT_ALERTS_REQUIRE_MANUAL = True`: a blank level is SILENT — there is NO
    fallback to the computed value.** An alert at a level he doesn't believe is worse than
    no alert and would train him to ignore the channel (House Rule #2). Flip the flag to
    False to restore auto-firing. `signals.support_levels()` survives only as an on-screen
    SUGGESTION under the edit form — shown as text and deliberately NOT pre-filled into the
    input, because an auto value sitting in a box gets saved by accident and then looks
    like a level he chose — and is labelled "auto-derived" in `thesis.py`.
    `alerts.manual_support_levels()` reads them in ONE query per run (cheaper than the old
    per-ticker history fetch) and logs loudly + returns {} if the migration is missing, so
    the worker degrades to silence, never a crash. `SUPPORT_NEAR_PCT` moved to `signals.py`
    so the dashboard can show the same number without importing the alert engine into the
    512 MB web service. Dashboard gains `Minor Sup` / `Major Sup` + % distance columns.
    **Needs a one-time migration:**
    ```sql
    alter table watchlist
      add column if not exists support_minor numeric,
      add column if not exists support_major numeric;
    ```
- **Windows console encoding (16-Aug-2026):** `alerts.py` now reconfigures stdout/stderr to
  UTF-8 at import. Its log lines are full of ⚠️/emoji and most exist to explain a FAILURE —
  under Windows cp1252 the `print()` inside an `except` raised UnicodeEncodeError and turned
  a *handled* error into an unhandled crash, losing the diagnostic it was written to give
  (House Rule #3). Found when a correctly-caught "column does not exist" error killed a local
  dry-run. Render is UTF-8, so this only ever bit local runs — which is exactly where
  `dryrun.py` lives.
- **Morning 5/10-DMA order levels (`alerts.run_morning_levels`, Lakshmi 11-Aug-2026)**:
  ~08:45 IST **weekdays** (it exists to place orders — pointless Sat/Sun), ONE Telegram
  message listing Lakshmi+Abinaya HOLDINGS (📌) and WATCHLIST (👀) names that are
  **trending up AND sitting on the 5/10-day EMA** — with the **rupee levels** to place
  morning limit orders at. His words: *"send a digest of ema 5/10 in the morning so that
  it sends us the price for us to place orders, it is just kissing and moving up."*
  **Deliberate exception to the dashboard %-only rule** (22-Jul-2026: dashboard shows %
  distance, never the ₹ level) — here the ₹ LEVEL IS the deliverable, so both are shown.
  Don't "fix" it back to percent-only. Levels come off COMPLETED daily closes
  (`signals.daily_entry_levels`), which is the right pre-open reference — today's bar
  doesn't exist yet. Gates: uptrend (close > 21-DMA, same gate as `check_ema5_touch`)
  AND within **`MORNING_NEAR_PCT`=1.5%** of the 5- or 10-DMA. **The 1.5 matters:** the
  first cut used 3.0 and listed 26+ names — in a trending market most stocks sit within
  3% of those lines, so it was noise not an order list. Sorted nearest-first, capped at
  `MORNING_MAX_PER_SECTION`=12/section with the overflow count DISCLOSED (no silent
  truncation). Silent when nothing qualifies. Dedup marker in entry_alert_log
  (ticker `__morning_levels__`, kind LEVELS). Test: `python dryrun.py morning-levels`.
  **Shipped broken, fixed 12-Aug-2026 (Lakshmi got nothing on day one):** the worker
  guard reused the variable name `levels_day`, which the FAST-POLL level cache already
  sets at 08:30 (`_within(now, FILINGS_OPEN, MARKET_CLOSE)`). By 08:45 the `!= today`
  test was therefore already satisfied and this digest never ran — not once. Renamed to
  `morning_levels_day`. **Lesson: a once-per-day guard is only as good as its variable
  being unique — grep the worker for the name before adding a new daily block.**
- **Morning agenda brief (`alerts.run_morning_brief`, Lakshmi 02-Aug-2026)**: ONE
  Telegram message ~08:30 IST listing Lakshmi+Abinaya names with a corporate
  event (board meeting → results/dividend/fund-raise) scheduled TODAY — HOLDINGS
  (📌) and WATCHLIST (👀) in separate marked sections — forward-looking 'what's
  coming today' so he plans the session. Board-meeting PRIOR
  INTIMATIONS ("Board Meeting to be held on <date>") are captured by `run_filings`
  (`_capture_event`/`_parse_board_meeting`) into the `scheduled_events` table as
  they arrive; the brief queries event_date==today. Runs on the WORKER (precise,
  once/day via `brief_day` guard, `_within(...,weekends=True)`) + a dedup marker
  in entry_alert_log (ticker '__morning_brief__') so it can't double-send. SILENT
  when nothing's due. **Populates FORWARD only** — meetings intimated before this
  shipped aren't known (NSE's forthcoming-meetings API is datacenter-blocked, no
  backfill), so it fills in over ~1-2 weeks. **NEEDS a one-time table** (schema not
  in repo, per the Files table note):
  `create table if not exists scheduled_events (id bigserial primary key, ticker
  text not null, event_date date not null, event_type text default 'board_meeting',
  purpose text, headline text, source_date date, created_at timestamptz default
  now(), unique (ticker, event_date, event_type));`
- Dashboard shows **% distance** to the DMAs/10wEMA, never the ₹ level (Lakshmi
  22-Jul-2026: "how far from the zone" is the decision; the rupee value isn't).
- **TRAILING STOP PAUSED (Lakshmi 16-Aug-2026)** — *"pause these alerts for the time
  being"*. `alerts.TRAILING_STOP_ENABLED = False` switches OFF only the 15%-off-peak
  alert (kind PEAK17). **The 10%-below-COST loss stop is untouched and still fires.**
  Set the flag back to True to resume — threshold, message and dedup key are all
  still in place, so it returns byte-identical (verified: same forced prices give
  0 trailing / 55 loss with the flag off, 55 / 55 with it on). `check_risk_stops`
  PRINTS one line every run saying the trailing half is paused — a stop-loss that
  silently stops working is the most dangerous kind of dead alert, so "why didn't I
  get a trailing stop?" must always have a visible answer in the log.
- **Risk / stop alerts (`alerts.check_risk_stops`, Lakshmi 21-Jul-2026)**: fires
  when a HOLDING is ≥10% below cost (loss stop, per each holder's own cost) OR
  ≥15% off its ~6-month peak (trailing stop, tightened from 17% 23-Jul-2026; peak = `signals.daily_entry_levels`
  "peak", max close over PEAK_LOOKBACK=126d). Rides the SAME cadence as entries:
  mainboard live (~1 min) in the fast poller, SME + backstop in the evening eod
  pass. Dedup kinds STOP10 / PEAK17 in entry_alert_log. The fast exit-side signal
  the weekly flowchart EXIT can't give (it only re-evaluates weekly).
- **21:15 IST (`deals`)**: NSE bulk/block deals in stocks you hold or watch
  (EOD data; evening-only by nature). Portfolio-scoped, deduped via filings_seen.
- **20:20 IST after bhavcopy (`eod-entries`)**: entry/add pass off EOD closes for
  ALL names — the ONLY entry check for SME (Lakshmi: SME at day-end is fine) plus
  a final mainboard pass. Dedup (entry_alert_log) means the evening pass never
  double-alerts a mainboard name the live poller already caught.
  Latency ceiling honesty: GitHub cron can lag; SME is EOD-only (no free intraday
  feed — the bhavcopy blind spot). If Yahoo's mainboard live path proves flaky,
  the insurance is a paid feed (Dhan ₹499/mo, 24h auto-refresh token, or TrueData
  stable key) swapped into `_live_quotes` only — nothing else changes.
- 10:45 & 14:45 IST: exchange filings + AI summaries (Claude reads the PDF
  natively — handles scanned docs; capped at 10 summaries/run, 20MB/PDF)
- Daily 20:00 IST: bhavcopy + delivery + fundamentals
- Daily 20:30 IST: exit audit (30/60/90-day post-sale price checks)
- **WEEKLY DIGEST NOW RUNS ON THE RENDER WORKER (28-Aug-2026).** GitHub's scheduler
  dropped the Friday digest outright — Vishal got no email, and there was no
  `digest_history` row for 28-Aug at all, i.e. the job never started (the digest itself
  was healthy: a dry-run reconciled to Rs 0 / 0.04% and produced both emails). Same
  best-effort behaviour that moved the live alerts onto the worker in July.
  `worker.py` now runs it **Friday 21:00–23:30 IST, retrying every 30 min** until it
  succeeds (so a transient failure, or a reconciliation block that gets fixed, still
  lands). GitHub keeps a **BACKSTOP at 22:30 IST** (`0 17 * * 5`, was `30 15 * * 5`).
  **Double-send is impossible:** both call `alerts.run_digest_if_due()`, which skips when
  a `digest_history` snapshot already exists for today for every portfolio — the same
  dedup principle as `filings_seen`. Because the snapshot is written only on a SUCCESSFUL
  digest, a blocked run correctly leaves the guard open so the next attempt retries.
  CLI: `python alerts.py digest-if-due` (the raw `digest` mode still force-sends).
  **THE WORKER NEEDS `RESEND_API_KEY` + `DIGEST_EMAILS` IN ITS RENDER ENV.** Its original
  env had only SUPABASE/TELEGRAM/ANTHROPIC, so on the first Friday it ran the digest it
  posted the Telegram teaser and delivered NO EMAIL. Worse, it stored the snapshot anyway
  and the dedup guard concluded "already sent", which would have suppressed the 22:30
  backstop too.
  **Dedup is therefore keyed on DELIVERY, not on the snapshot** — `entry_alert_log`
  ticker `__digest_email__`, written only after `send_email` returns True. The snapshot
  proves the digest was COMPUTED; only the marker proves someone received it.
  `send_email` returns False and never raises, so its result MUST be checked — this is the
  same class of bug as `send_telegram`'s documented return-False contract.
  The manual `send_digest_now` tick-box FORCE-sends (bypasses the guard): a human pressing
  the button means "send it now", and the guard would otherwise refuse the rescue.
- **Friday 21:00 IST**: weekly digest (moved from Sunday per Lakshmi's
  request — he plans portfolio strategy on Saturdays)
- Manual tick-boxes on `workflow_dispatch`: bhavcopy-backfill,
  delivery-backfill, index-backfill, send_digest_now, run_filings_now,
  run_symbol_check (diagnostic — tests all tracked symbols against the
  last completed trading day's real files in ~2 min, use this BEFORE any
  long backfill)

## Trade journal + exit audit loop
Mark-as-Sold asks for an exit reason (EXIT signal / Profit booking / Thesis
broken / Override+notes). `exit_audit.py` checks price 30/60/90 days later,
sends a "saved X%" / "cost X%" verdict to Telegram. This is the system's
long-run self-scoring mechanism — treat it as sacred, don't let it silently
break.

## Known issues / backlog (as of 21-Jul-2026)
- **EBITDA IS NOW REAL (01-Sep-2026)** — was permanently blank because
  `fundamentals_daily` had no such column. `fundamentals.fetch_one` now delegates to
  `screener_data.snapshot` (one parser for that page, not two drifting ones) and stores
  **ROCE, ROE, Revenue/EBITDA TTM and OPM%**. `screener_data.ttm_metrics` prefers
  screener's own TTM column and falls back to summing trailing periods — using the
  ACTUAL reporting gap, so a half-yearly SME sums 2 periods, not 4 (verified: CWD).
  Needs a one-time migration:
  ```sql
  alter table fundamentals_daily
    add column if not exists roce numeric, add column if not exists roe numeric,
    add column if not exists revenue_ttm_cr numeric,
    add column if not exists ebitda_ttm_cr numeric,
    add column if not exists opm_ttm_pct numeric;
  ```
- **P/B was NULL for all 112 tickers** — `fundamentals.update_all` took the price from
  Yahoo `fast_info`, the exact call House Rule #1 says is blocked from datacenter IPs, so
  on Render it returned None and P/B was never stored. It now uses screener's own
  "Current Price" off the page already fetched; the Yahoo call is gone entirely. (The
  dashboard had been masking this by computing CMP/BookValue at render time.)
- EV/EBITDA still not shown: it needs enterprise value (debt + cash), a balance-sheet
  join we don't do. A blank beats a wrong multiple.
- Young listings show "INSUFFICIENT DATA" state until 45+ weeks of price
  history exist — self-heals, no action needed
- Render free-tier stability under real load — watch for exit-139 crashes;
  root-caused once already to Yahoo retry storms (fixed by excluding SME
  tickers from Yahoo calls and reducing quote-fetch threads 8→4)
  - **Worker OOM-restart FIXED without upsizing (05-Aug-2026, Vishal — "make it
    work with what we have", no upgrade):** the 512 MB Starter worker exceeded its
    memory limit and auto-restarted (Render email) during the results-season
    evening filing surge. Two drivers: (a) each filing PDF summary loads the PDF +
    base64 (~1.33x) AND `requests` copies the base64 again into the API body →
    ~50 MB transient per PDF, up to `MAX_SUMMARIES_PER_RUN`=10/cycle; (b) glibc
    malloc keeps freed heap in its arena, so over hours of the loop RSS creeps up
    (Python frees the objects but the pages never return to the OS). Fixes, all
    within 512 MB: `MAX_PDF_BYTES` 20→15 MB (still covers scanned results, caps the
    spike); `summarize_filing` now `del pdf_b64; gc.collect()` in a `finally` so
    summaries can't stack; and **`worker._reclaim_memory()` (gc.collect + glibc
    `malloc_trim(0)` via ctypes) after every NSE/BSE filings run**, which returns
    the freed pages to the kernel so RSS stays flat instead of climbing to OOM.
    `_NSE_RSS_CACHE` is also nulled after each run to release the ~2000-item feed.
    malloc_trim is a no-op off glibc (local Windows). The auto-restart self-heals
    and dedup means no double-alerts, so this was never data-loss — just downtime.
- Filing-summary classification: RESULTS filings now use a TYPED template
  (21-Jul-2026) — consolidated Revenue/EBITDA/PBT/PAT/EPS, each with QoQ + YoY %
  (plus an **EBITDA-margin line** — EBITDA/Revenue for all 3 periods — and a
  margin-bps-led "Take", Lakshmi 31-Jul-2026: rising margin + rising sales is his
  key combo; units cancel so it's unit-independent)
  (`alerts._summarize_results`/`_format_results`). Claude EXTRACTS raw line items
  from the PDF; Python computes EBITDA (=PBT+finance+depreciation), the %s, and
  unit→Cr — so no model-arithmetic error reaches a number (rule #2); unusable
  extraction falls back to the generic bullet gist. STILL generic for other
  types (order wins value/client/timeline, pledge/auditor 🚨 flags) — next.
  - **RESULTS column-mapping bug FIXED (31-Jul-2026, Lakshmi caught it on Clean
    Max):** the model mis-mapped columns on wide statements — pulling PBT/PAT/EPS
    from the PRECEDING-quarter column while revenue/finance/depn came from the
    current one (Clean Max Q1 showed PBT ₹75.3cr/PAT ₹45.4cr = LAST quarter's; real
    ₹94.0/₹55.2). Also flaky run-to-run. Rewrote `_summarize_results`: (a) model now
    transcribes the statement **COLUMN-BY-COLUMN** (`columns[]` each with period_end
    + is_quarter + ebitda_reported) so figures can't cross columns; (b) Python picks
    current/prev-Q/year-ago **by DATE + magnitude** — a period >3× the current
    quarter's revenue is a full-year/YTD column, NOT a quarter (Banswara lists 'year
    ended' as a middle column ~40× a quarter; is_quarter alone is unreliable because
    Q4 and FY both end 31-Mar); (c) **RE-ANCHOR + RECONCILE** PBT/PAT/EPS against a
    UNIVERSAL identity — every Ind-AS P&L has PBT = TotalIncome − TotalExpenses (or
    − finance − depreciation when shown separately, e.g. Clean Max). Per column, try
    both forms, match each column its best-fitting (pbt,pat,eps), keep the lower-
    residual identity → deterministically undoes any swap. If the CURRENT column
    still won't reconcile (>15% off), FALL BACK to the gist rather than emit a wrong
    number (rule #2). (The earlier EBITDA-line anchor was too rare — a 7-filing audit
    showed almost no statement prints an 'EBITDA' line; Total income/expenses are
    universal.) `_parse_stmt_date` handles 31-Mar-26 / 30.06.2026 / 'June 30, 2026'
    / ISO; results extraction max_tokens 900→2000 (the column JSON was truncating).
    Verified with the live API on Clean Max (swap), Banswara (Q4-vs-FY), and a
    7-filing audit (all reconciled or safely fell back). Test locally via
    `python dryrun.py filings-nse` with ANTHROPIC_API_KEY in secrets. KNOWN LIMIT:
    the reconciliation catches inter-column SWAPS, not a self-consistent wrong-SCALE
    mis-read (rare). Only the RESULTS table carries precise numbers — XBRL (acq/board
    changes) is structured tagged data and the generic gist is qualitative, so
    neither is subject to this table-column failure mode.
  - **RESULTS wrong-period bug FIXED (04-Aug-2026, Lakshmi caught it on Styrenix):**
    the current-quarter column is chosen by `max(period date)`, so a date-PARSE slip
    silently picks the wrong column. Styrenix printed its headers **'Jun, 30 2026'**
    (month-COMMA-day) and `_parse_stmt_date` read the DAY '30' as the YEAR → **2030**;
    'Mar, 31 2026' → 2031; so the March quarter out-sorted June and became 'current'.
    Result: the alert showed the PRECEDING quarter (₹826 Cr, −18% QoQ, everything
    collapsing) when the real Q1 was ₹1,010.9 Cr **+22% QoQ, PAT +88%, margin
    15.5→22.1%** — a scary-wrong flip (House Rule #2). Lakshmi read it as "standalone
    not consolidated"; it was actually consolidated data for the WRONG PERIOD (verified
    from the PDF: consolidated Jun rev ₹1010.9 Cr / standalone ₹768.0 Cr — our fixed
    output matches consolidated). Fix: `_parse_stmt_date` now has a **'MonName<sep>DD<sep>
    YYYY'** pattern (handles 'Jun, 30 2026' / 'June 30, 2026' / 'Mar 31, 2026') placed
    BEFORE the day-optional pattern that mis-fired. Verified live on the Styrenix PDF
    (now reads Jun-2026 consolidated) and against all prior date formats. **Lesson: the
    period selection is only as good as the date parse — any new header format is a
    silent wrong-column risk.**
  - **RESULTS exceptional-item distortion FIXED (06-Aug-2026, Lakshmi caught it on
    Advent Hotels):** EBITDA was `reported PBT + finance + depreciation`, so a large
    EXCEPTIONAL one-off sitting in reported PBT flowed straight into EBITDA. Advent's
    **year-ago quarter had a ₹41.6 Cr exceptional GAIN**, which faked an **84.7%
    year-ago EBITDA margin** and a "-58% EBITDA / margin pressure" Take — when the real
    OPERATING quarter was **UP ~8%, margin 32.8→35.5%** (operating PBT more than
    doubled). Fix: extract the **signed `exceptional_items`** line per column (+gain /
    −charge) and compute **EBITDA = PBT + finance + depreciation − exceptional**, so
    one-offs are stripped; a **⚠️ line flags any period with a material exceptional**
    (>3% of revenue) since the reported PBT/PAT/EPS YoY/QoQ are still distorted by it.
    **Format-agnostic (the key subtlety):** do NOT compute EBITDA as `TotalIncome −
    TotalExpenses + finance + depn` — whether TE *includes* finance/depn varies by
    company (Advent's TE includes them → TI−TE = pre-exceptional PBT; Styrenix's TE
    EXCLUDES them → TI−TE already = EBITDA), so that form double-counts and false-flags.
    `PBT + fin + depn − exceptional` is correct for both. `exceptional_items` rides the
    re-anchor pool (travels with its PBT if columns are reassigned) and defaults to 0,
    so the no-one-off majority is unchanged. Verified live: Advent (real operating story
    + flag) and Styrenix (unchanged ₹223.6 Cr / 22.1%, no false flag).
  - **RESULTS "margin pressure" mislabel FIXED (11-Aug-2026, Lakshmi caught it on
    Krishival):** the Take called a quarter with **revenue +80% YoY and EBITDA +50% YoY**
    "margin pressure", purely because the margin RATIO slipped 15.6→13.0% — while EBITDA
    in rupees grew ₹7.7cr→₹11.6cr and the SEQUENTIAL margin actually EXPANDED. Every
    number was arithmetically correct; the **verdict** was the opposite of the story.
    Lesson: a falling margin % is not automatically pressure — scaling hard almost always
    dilutes the ratio. Fix: the Take now weighs **EBITDA growth in RUPEES** (`eb_yoy`), so
    rev-up + margin-down + EBITDA-up reads *"growing, margin diluted"*, and only
    rev-up + EBITDA **flat/falling** is called *margin pressure*; when the QoQ margin
    disagrees with the YoY read it is appended ("but QoQ margin UP x→y%") since the
    sequential number is the fresher one.
    **Same alert also exposed run-to-run column flakiness:** the live run dropped the
    PRECEDING-QUARTER column entirely, so every QoQ read "—" (a re-run minutes later
    had it). `_RESULTS_JSON_PROMPT` now states that an Indian quarterly statement almost
    always prints FOUR columns, that two columns often share an end date (Q4 and FY both
    end 31-03) and BOTH must be returned, and that omitting the preceding quarter
    silently loses every QoQ. Side benefit: Advent now extracts its preceding quarter too
    and flags a ₹15.7cr exceptional there that was previously invisible.
    **ROOT CAUSE OF THE FLAKINESS FOUND AND FIXED — `temperature: 0`.**
    `_anthropic_pdf_call` set no temperature, so the API defaulted to **1.0 (maximum
    sampling randomness)** on what is a pure TRANSCRIPTION task. That is why the same PDF
    yielded different comparison-column figures on consecutive runs and why a column was
    sometimes dropped. Reading numbers off a table has exactly one right answer — there is
    nothing to sample. With `temperature: 0` the extraction is **deterministic**: verified
    by running Krishival 3x (byte-identical, same SHA) and Advent + Styrenix 2x each
    (identical). This applies to EVERY filing summary — results, XBRL and the generic
    gist — so the whole summary pipeline is now reproducible. Any future
    "the alert said something different last time" is a real bug, not sampling noise.
- **Filing-match bug FIXED (21-Jul-2026):** NSE filings for many holdings were
  silently never alerting. The NSE RSS `title` is the COMPANY NAME, not the
  symbol, but the code matched `^SYMBOL` against the title → 0 hits for any stock
  whose symbol ≠ first word of its name (e.g. South West Pinnacle / SOUTHWEST).
  Confirmed against the live feed. Fix: match the symbol parsed from the
  attachment link (`/corporate/SYMBOL_…pdf`), case-insensitive, company-name
  fallback. Also fixed: pubDate was parsed with the wrong format so every NSE
  filing had a BLANK date; and the RSS fetch now retries + tolerates a truncated
  feed (lenient per-`<item>` regex) instead of losing the whole run.
  - **Matching is EXACT, never substring** (portfolio-wide audit 21-Jul-2026):
    match on exact link-symbol OR exact normalised company name. A substring
    attempt false-fired badly ('EMS' inside 'R Systems'/'ZF…Systems' → other
    companies' filings mis-attributed). NSE's filing-link token is often NOT the
    trading symbol (NEWGEN→NEWGEN2, CENTENKA→CENTURYENKA, NORTHARC→NACL2020,
    VIYASH→SEQUENT1), so the exact company NAME is the reliable anchor.
  - **Filter is a BLACKLIST, not a whitelist (23-Jul-2026).** We used to alert
    only on `MATERIAL_KEYWORDS`; a whitelist can only catch what we thought of in
    advance, and it silently dropped Solara's **press release** and its **"Change
    in Directors/KMP/Auditor"** filing (Lakshmi spotted the miss). Now we alert on
    EVERYTHING except `ROUTINE_KEYWORDS` housekeeping (trading window, newspaper
    publication, monitoring agency, statement of deviation, ESOP "pursuant to
    exercise", …). `MATERIAL_KEYWORDS` survives only to ⭐-flag the high-signal
    ones. Measured effect on a normal day: 5 → 11 alerts across ~50 holdings.
    **Do not revert to a whitelist** — missing a filing is the cardinal sin here.
  - **`python alerts.py filings-audit`** (new): read-only, no Telegram — lists,
    for every NSE holding+watchlist name, which of today's filings the engine
    matches and how it treats each (⛔ SUPPRESSED / ⭐ starred / • alerts). Kept
    in lock-step with run_filings (both scope holdings+watchlist, both label via
    ROUTINE_KEYWORDS). Run it anytime to spot-check coverage — the scalable check.
  - **Hardening (23-Jul-2026, second review pass):** (a) all HTML-interpolated
    fields are `html.escape`d incl. the results-template `company`/`period` and
    the generic gist — an unescaped `&` (e.g. Sathlokhar "E&C") would 400 the
    whole Telegram chunk, and with write-after-send that filing would retry
    forever + burn summary calls. (b) `no_audience` fingerprints (Vishal-only
    stocks) are NOT marked seen, so if Lakshmi buys one while the filing is still
    live it still alerts. (c) `_fetch_all_rows` paginates the FULL-read tables
    (transactions→XIRR, realised→FY P&L) past the 1000-row cap — truncating
    those would be a House-#1→#2 wrong-number bug (not biting yet: ~90–135
    rows/pf). `filings_seen` dedup still uses newest-1000 via `_load_seen`.
    KNOWN LEFT: `trade_journal` read uses `.in_()` (31 rows, low stakes) — not
    paginated yet.
  - **Filings cadence (split by exchange):** NSE announcements run **every 15
    min** (`filings-nse`, `run_filings(nse_only=True)`, 08:30–23:15 IST) — the
    archives host is friendly, safe to poll often. BSE rides the **hourly full
    run** (`filings`, worker `BSE_FILINGS_INTERVAL=3600`). Was twice-daily; results/
    board outcomes drop in the EVENING and the NSE feed is only a ~1-day snapshot,
    so infrequent polling let them age off unseen. `MATERIAL_KEYWORDS` now
    includes "board meeting" (results are decided there).
  - **BSE filings had NEVER worked → fixed via Screener (04-Aug-2026, Vishal
    caught it):** `fetch_bse_announcements` hit BSE's `AnnGetData` API, which BSE
    rebuilt behind an **Akamai JS challenge**: it returns HTTP 200 + `"No Record
    Found!"` to any plain HTTP client — proven exhaustively (plain requests, TLS
    impersonation via `curl_cffi`, and a REAL headless Chrome all failed; the live
    ann page makes 0 XHR calls = server-rendered). Net effect: **0 BSE filings had
    ever reached `filings_seen`** (checked: 0 of 291 rows). NOTE BSE's *other* API
    endpoints (deals `fetch_bse_deals`, quote `ComHeader`) DO answer plain
    requests — only announcements is JS-walled. Fix: **scrape Screener.in**
    (`fetch_bse_announcements_screener`) — it aggregates BSE announcements, is
    reachable from a datacenter IP (our fundamentals scraper already proves it),
    and ingests a filing within **minutes** (measured a live filing at '4m' on
    Screener), so the hourly poll gives ~1h latency. Returns the SAME
    `{headline,date,url}` shape → drops into run_filings unchanged (dedup / routine
    filter / AI PDF summary / Telegram). **Parser gotchas (both fixed):** (a) SCOPE
    to the `<div class="documents flex-column"><h3>Announcements</h3>` block — the
    page's "Annual reports"/"Concalls" sections have identical `<li>` shape and
    leaked dateless "Financial Year 20XX" items (no date → past the cutoff) whose
    huge PDFs also blew the summary token limit; (b) the per-item timestamp is a
    `<span|div class="ink-600 smaller">` holding either "36m" or "DD Mon - <desc>"
    — parse the leading token, drop the desc. `_screener_reltime_to_iso` maps
    'Nm'/'Nh'→today, 'Nd'→today−N, 'DD Mon[ YYYY]'→that date (all resolve to the
    real filing DAY, so a later poll's shifted label still fingerprints the same →
    no duplicate). Politeness: 1.5s/name, back off on 429/403. **Dual-listed BSE
    names route to NSE instead** (`BSE_TO_NSE`, near-real-time via the working RSS):
    Kwality=KPL, Time Technoplast=TIMETECHNO (verified on NSE's master equity list;
    the other 7 — CWD, Hemant Surgical, True Colors, Shree Ganesh, Lehar, BMW
    Industries, TANFAC — are BSE-only, so Screener is their only source). The old
    `fetch_bse_announcements` is kept but SUPERSEDED/uncalled.
  - **Summaries were silently DEAD + XBRL link fix (31-Jul-2026, Lakshmi caught
    both):** ROOT bug — `_download_pdf_b64` referenced an **undefined `HEADERS`**,
    so it NameError'd inside its bare `except` and returned None for EVERY PDF →
    `summarize_filing` bailed before calling Claude → every filing had gone out
    **headline-only since that code landed**, regardless of the (present)
    ANTHROPIC_API_KEY. Fix: module-level `HEADERS` (browser UA) + logging on the
    missing-key / fetch-fail paths (were silent, rule #3). Content upgrades:
    generic gist now ends with a "Why it matters" line; the results template ends
    with a computed **"Take:"** line (margin story from the numbers, Python-derived
    — rule #2). `dryrun.py` now passes ANTHROPIC_API_KEY through so
    `python dryrun.py filings-nse` previews real summaries.
  - **XBRL intimations (`_filing_link`):** NSE board-meeting **prior intimations**
    are `/corporate/xbrl/…WebXMLFile_….xml` links that **404** and can't be
    summarized. So: the AI gist ONLY runs on `.pdf` attachments; non-PDF NSE
    filings stay headline-only (the full headline IS the payload) and their link
    is swapped for the stock's **NSE page** so it's never dead. Headline cap raised
    200→500 (was double-truncated, cutting "June"→"Ju").
  - **Structured XBRL now summarized (31-Jul-2026, `_summarize_xbrl`):** many
    material NSE filings are XBRL-only with a vague generic headline (e.g. "Change
    in Directors/KMP/SMP/Auditor/RTA"). Some XBRL WebXMLFiles ARE publicly fetchable
    (Change-in-Management `CIMG_*` returns 200; others like `Reg30` restructuring
    404). For fetchable ones we now PARSE the fields — **free, no LLM/token** — into
    who / appointment-vs-exit / effective date, e.g. "Appointment · KMP: Ms. Monika
    Bohara — Company Secretary (eff 03-Aug-2026), routine appointment(s)". A 🚨 flag
    fires when an **auditor or director is resigning/removed** (classic smallcap red
    flag). Wired into run_filings as the non-PDF branch (`.xml` → `_summarize_xbrl`);
    404/unknown types still fall back to headline + NSE-page link. Extend by adding
    a parser for another XBRL schema keyed off its tag set.
    - **Reg-30 acquisitions now parsed too (`_summarize_xbrl_reg30`):** target /
      consideration (₹ cr) / rationale / target financials / timeline, with a 🚨
      RELATED-PARTY flag. **KEY finding — the XBRL 404 is a PUBLISH DELAY, not a dead
      link:** NSE posts the WebXMLFile minutes-to-hours AFTER the RSS headline, so a
      3-min poll often 404s but the file appears later (verified: Banswara's Reg30 was
      404 fresh, 200 next day). CIMG tends to publish fast, Reg30 slow. The NSE
      announcements API DOES expose companion PDFs but is datacenter-IP-blocked
      (House Rule #1), so unusable from the worker.
    - **SOLVED via alert-now-then-follow-up (Lakshmi chose this 31-Jul-2026):** the
      headline goes out immediately (never delayed/missed); each later poll re-tries
      `_summarize_xbrl(url)` and, once NSE publishes the file, sends a ONE-TIME
      follow-up: **🔁 "<company> — filing update"** + an **"↳ earlier: <headline>"**
      echo so it's unmistakably linked to the original headline alert, then the
      parsed summary. Dedup uses a SECOND fingerprint
      `_fingerprint(sym, headline, date, "xbrlsummary")` so there's **no schema
      change** and no duplicate: first-sighting-with-summary records BOTH fps (no
      follow-up); headline-only records just `fp`, leaving `fp_sum` open for the
      follow-up. `alerts_by_group` items are now `(fps_list, body)`. Verified live in
      dry-run on a Jitf "Change in Directors" filing (4 director appointments).
      - **BACKLOG (deferred 31-Jul-2026, only if Lakshmi wants the visual):** native
        Telegram threaded reply (`reply_to_message_id`) instead of the 🔁+echo text
        link. Decided AGAINST for now — it's MORE work not less (needs `send_telegram`
        to return the message_id + a schema column to persist it across polls), and it
        clashes with our BATCHED sends (a msg id points at a chunk of many filings, so
        the reply wouldn't target the specific headline unless we un-batch → more
        messages). No cost/bandwidth upside (Telegram is free; same # messages; summary
        cost identical). The 🔁 + "↳ earlier:" echo gives the linkage with none of that.
- Bulk/block deals: BUILT for NSE + BSE (21-Jul-2026) — `alerts.run_deals`.
  NSE: `fetch_nse_deals` reads daily bulk.csv/block.csv (friendly archives host),
  matched by trading SYMBOL. BSE: `fetch_bse_deals` hits BulkDeal_Beta/BlockDeal_Beta
  (routes found by inspecting bseindia.com's JS bundle — the naive BulkDeals/w
  guesses were invalid routes; verified vs live 200/JSON), matched by SCRIP_CODE
  so BSE-only SME names (CWD/HSIL/etc.) are covered. Evening `deals` job, 21:15
  IST (EOD data). Dedup reuses filings_seen. Insider/promoter + pledge come via
  the filings feed keywords (insider/encumbr/acquisition of shares/disposal of
  shares) — the dedicated NSE PIT API returns empty/blocked.
  STILL TODO: a richer typed insider format (who/how many shares).
- Resend email digest DELIVERS to Vishal's address (confirmed 01-Aug-2026 — he
  gets it); domain verification may still be pending for a branded from-address,
  but it is NOT blocking delivery. **The Friday digest is now TWO SEPARATE EMAILS
  (Vishal 07-Aug-2026): his own book in one, the Lakshmi+Abinaya book in another**
  — `run_digest` filters holdings by group and calls `_digest_for` twice (each
  self-contained: its own header count, EXIT box, and all-holdings states table),
  so neither email carries the other's long states list (Vishal's complaint: the
  combined email got too long). `_digest_for(..., label=, telegram=)` — `label`
  names the email (subject + header), `telegram=False` suppresses the teaser for
  the email-only Vishal digest. The Telegram TEASER rides ONLY the Lakshmi/Abinaya
  email (Vishal opted out). Each call stores its portfolios' `digest_history`
  snapshots, so pf1's week-over-week / vs-index scorecard builds from the next
  Friday on. NOTE: email creds (`RESEND_API_KEY`/`DIGEST_EMAILS`) live on
  GitHub/Render, NOT in local secrets — a real send only happens from the Friday
  cron or the `send_digest_now` dispatch, not from a local run.
- **Digest-vs-dashboard VALUATION CROSS-CHECK (08-Aug-2026, Vishal — after two
  wrong-money bugs in one day).** The digest prices holdings via
  `signals.current_state(...)["close"]` (weekly bars) while the dashboard uses
  `app.fetch_live_prices` (live quote / daily bar / bhavcopy). Two code paths, one
  question — and they drifted twice: (a) the weekly-close reconcile knocked ~10%
  off **PGIL** (₹2,221 vs the true ₹2,464), and (b) **young listings** (<45wk, state
  INSUFFICIENT DATA) returned NO close, so the digest fell back to **PURCHASE COST**
  — hiding a 25% loss on Advent Hotels (cost ₹197 vs market ₹148) and understating
  young winners. Both looked plausible and tripped nothing; only Vishal reading the
  PDF against the dashboard caught them. **Root lesson: valuing at cost is the most
  dangerous kind of wrong — it reads as "no gain/loss" exactly where the truth
  matters, and a blank would have raised questions on day one (rule #2).**
  Guard: `_valuation_mismatches` re-prices EVERY holding through an independent
  daily path (`_independent_price` → `signals._fetch_daily`, never the weekly bars)
  and flags two kinds — **COST** (digest has no price → cost fallback) and **DRIFT**
  (both priced, >`RECONCILE_PRICE_TOL_PCT` 2% apart) — plus a portfolio-total gap
  (`RECONCILE_VALUE_TOL_PCT` 0.5%) so many small gaps can't slip under the per-
  holding bar. It runs in `_digest_for` **BEFORE the per-portfolio loop**, because
  that loop WRITES the `digest_history` snapshot — blocking there means nothing wrong
  is emailed *or* stored (a bad snapshot would poison next week's WoW too).
  `DIGEST_RECONCILE_MODE`: **block** (default — refuse to send, log why),
  `warn` (send with a red ⚠️ MISMATCH banner listing the offending holdings), `off`.
  Both historical bugs were replayed against it and are CAUGHT with the stock named.
  **REFERENCE IS NOW NSE'S OWN BHAVCOPY FILE (22-Aug-2026), not the Yahoo daily path.**
  The guard BLOCKED both digests over a 15% gap on Welspun — and the digest was RIGHT:
  NSE's file said Rs 2,311.90, exactly what the digest used, while the independent
  Yahoo *daily* series was missing Friday entirely (it ended Thursday) even though the
  *weekly* series had it. Same "Yahoo drops the latest session" gremlin as the false
  HFCL EXIT and the wrong Today's P&L, landing on a third consumer. `_independent_price`
  now resolves NSE names from `_bhav_reference()` — ONE bhavcopy download per run covers
  every symbol (~3.5k), it is the exchange's published file rather than a redistribution
  (House Rules #1 + #5), and it costs one request instead of N quote calls. Falls back to
  the daily path for SME/BSE names and if the file is unavailable. After the change the
  same run reconciles to **Rs 0 gap for Vishal and Abinaya, Rs 8,793 (0.03%) for Lakshmi**
  (the residual is BSE/SME names not in the NSE file). **Lesson: a guard whose own
  reference can go stale will eventually block the real thing — and a digest silently not
  sent is worse than the mismatch it was protecting against.**
  Spot-check anytime, read-only: **`python alerts.py reconcile`** (or
  `python dryrun.py reconcile`) — prints per-portfolio digest-vs-independent totals.
  Both paths settle to the same Friday close on healthy data and the digest only runs
  after the close, so a real gap means one side is stale — don't "fix" it by widening
  the tolerance.
- **52-week-high bug FIXED (27-Jul-2026, Lakshmi caught it):** the watchlist/holdings
  "52W High" (and "% vs 52WH") read badly low — up to ~20% — because
  `signals._fetch_daily`'s Yahoo branch fetched only `period="6mo"`, so
  `high_52w = close.tail(252).max()` silently took a **6-MONTH CLOSE** high (`.tail(252)`
  on ~126 bars returns everything). Fixed: fetch **2y**, carry the intraday **`high`**
  column, and compute `high_52w` as the max intraday high over a **rolling 365-CALENDAR-day**
  window (matches Kite/screener). **Split safety VERIFIED** on Websol's 10:1 split — Yahoo's
  `auto_adjust=False` OHLC are already split-adjusted (only dividends aren't), so no raw
  pre-split price leaks in; House-Rule-#10 is a bhavcopy-only concern and that path is
  adjusted-on-read. **GUARD (rule #7):** returns `None` → cell shows "—" when <~1yr of
  history (young listing / partial bhavcopy backfill, since `MIN_BHAV_DAILY_ROWS=60` can win
  the source), so the 52W window can't silently differ per row. `high_52w` is **display-only**
  — no alert path consumes it. The sibling `peak` (risk-stop, 126d) is deliberately 6-month
  and was NOT touched.
- **Today's P&L intraday bug FIXED (31-Jul-2026, Vishal caught it):** during market
  hours `app.fetch_live_prices` took the "bar (settled close)" branch because Yahoo's
  DAILY endpoint returns today's IN-PROGRESS bar (its date >= expected close) and that
  daily series frequently OMITS the prior session — so `b_prev` (`iloc[-2]`) was TWO
  sessions stale and **every holding's day-change was computed against a 2-day-old
  close**. Dashboard showed Today's P&L **-Rs 13,282** on a day the broker showed
  **~+Rs 20k**; e.g. ADF Foods rendered -8.4% (276 vs a stale 301.8 from two days back)
  when it was really +5.4% (276 vs the true prior close 262.1). Fix: the settled-bar
  branch now also requires **`not market_is_open()`** — intraday the live **quote**
  wins (its `previous_close` is the correct reference). After the fix Today's P&L read
  +Rs 29,929, matching the broker. **Display-only** (KPI + Day Change % column); no
  alert path consumes it. **Caveat (unchanged):** SME/bhavcopy names have no live feed,
  so intraday their "day change" is still the last EOD session's move — structural.
- **False state-change alert FIXED (03-Aug-2026, Lakshmi caught it):** the SAME Yahoo
  "drops the latest session" gremlin as the Today's-P&L bug, now on the WEEKLY flowchart.
  Yahoo's 1-wk bar silently omitted Friday's session, so HFCL's just-completed weekly
  close came back as **Thursday's ₹184.69 (below the 10wEMA) instead of Friday's true
  ₹193.92 (above it)** → fired a false **🟣 MOMENTUM FADING** when the real state was
  **MAINTAIN/ADD** (stock was actually +4% making new highs). The stale numbers
  reproduced the alert EXACTLY (184.69 / 10wEMA 188.9 / 20wEMA 162.4). Fix:
  `signals._reconcile_last_week` — `current_state` (the single chokepoint BOTH the
  dashboard and `alerts.run_states` use) now cross-checks the last weekly close against
  a FRESH `_fetch_daily` for the **same ISO week** and corrects it if they disagree.
  Only trusts the daily side when it's **at least as fresh as the weekly frame**
  (`dd.index.max() >= wk_ts`), so it can NEVER move the close in the staler direction;
  the bar's high/low are widened to stay internally consistent; correction fires only on
  a >0.5% gap and logs WHY (rule #3). SME names resolve both sides from the same bhavcopy
  table → agree → untouched. Verified: stale frame corrects 184.69→193.92 /
  FADING→MAINTAIN/ADD, healthy frame left unchanged, live `states` dry-run reads
  MAINTAIN/ADD. **Third lesson that a wrong number is worse than a blank (rule #2)** —
  and the second consumer of Yahoo dailies to need its own freshness guard (the guard
  doesn't port automatically — you must add it at each new consumer, cf. the `_sane_quotes`
  lesson). Self-heals: next states run sends "HFCL → MAINTAIN/ADD (was MOMENTUM FADING)".

## Stage-2 thesis scorecard (`thesis.py`, Lakshmi 16-Aug-2026)
**The problem in his words:** he *"is not able to spend time to get context on all these
different companies — that's the reason he is not able to maximise success rate"* picking
stage-2 names. So when a stock goes on the watchlist, this builds the context for him.

**7 pillars, 0-2 each, /14** — RQ Results Quality · DU Durability · MG Margin Direction ·
VA Valuation · SM Smart Money · GV Governance · TE Technical.
Tiers: **12-14 STRONG SETUP / 9-11 BUILD SLOWLY / 6-8 WATCHLIST / 0-5 AVOID.**
**HARD RULE: GV=0 caps the verdict at WATCHLIST** no matter the total (verified in test:
12/14 with GV=0 → WATCHLIST). The cap only ever LOWERS a verdict.

**THE DESIGN DECISION — the model does not touch the numbers.** The workshop prompt this
came from has Claude research everything, including financials. We invert that:
| Pillar | Source |
|---|---|
| RQ, MG, VA | `screener_data.py` — parsed quarterly table, OPM bps deltas, P/E, P/B, ROCE, ROE, CAGR |
| TE | `signals.py` — **scored in Python** (`thesis.score_technical`), never asked of the model |
| DU, SM, GV | Claude Opus 5 + `web_search_20260209`, must cite a primary source |
Python also owns the pillar total, the tier lookup and the GV cap. **Rationale: every
wrong-number bug this project has shipped came from a model transcribing or doing
arithmetic it should have been handed** (Clean Max columns, Styrenix date, Advent
exceptional item, Krishival's "margin pressure" verdict). Here the model does judgment
only. Because TE comes from the same `signals` functions the alerts use, **a thesis and a
5/21-DMA alert can never disagree about the same stock.**

**FAIL-SAFE:** with no parseable quarterly financials it returns `INSUFFICIENT_DATA` and
publishes NOTHING. Scoring RQ/MG/VA as 0 would read as "bad company" and drag a good one
to AVOID — a blank beats a wrong number (House Rule #2).

**API specifics (verified live 16-Aug-2026 — do not "fix" these from memory):**
- Model `claude-opus-5`. **`temperature` is REJECTED with a 400 on Opus 5** — so this
  cannot reuse `alerts._anthropic_pdf_call` (which correctly pins `temperature: 0` for
  the Haiku *transcription* work). Different job, different call.
- Web search tool type is **`web_search_20260209`** (not the `_20250305` variant).
- Structured JSON via `output_config={"format": {"type":"json_schema","schema":…}}`;
  effort rides the same object. Schema needs `additionalProperties: false` everywhere.
- Uses the official `anthropic` SDK (added to requirements.txt), **streamed** — Opus 5
  thinks by default and these calls are slow enough to trip a plain HTTP timeout.
- **`pause_turn` must be resumed** (`thesis._final_message`): server-side web search runs
  its own loop and stops mid-turn at the iteration cap. Not resuming returns a partial
  answer that looks complete (House Rule #7).
- **Research is cached to `.cache/thesis/<ticker>_<date>.research.txt` (gitignored).**
  Learned the hard way on the first live run: research finished (17 tool calls, 18k chars)
  and was thrown away when the next call failed, so a re-run paid for all of it again.
- **COST — measured, not estimated (Vishal caught this 16-Aug-2026).** The first
  live run cost **~$1.00 for ONE stock**, against an estimate of $0.35-0.50. The miss
  was the INPUT side: every web-search round re-sends all results gathered so far, so
  17 rounds compound input tokens far faster than the visible answer grows (~120k input
  tokens). Fixed by (a) tuning defaults DOWN — research runs at **effort `medium`**
  (it's search-and-summarise, not reasoning), **max 6 searches**, 6k max_tokens; scoring
  keeps effort `high` because that's the actual judgement call; and (b) **every run now
  PRINTS its real token count and dollar cost** per phase and in total, so a cost
  surprise can't recur. Overridable: `THESIS_RESEARCH_EFFORT`, `THESIS_MAX_SEARCHES`,
  `THESIS_RESEARCH_TOKENS`, `THESIS_SCORE_EFFORT`, `THESIS_SCORE_TOKENS`.
  Re-running the same stock the same day is **$0.00** (research is cached).
  Only runs when invoked manually — nothing schedules it. **Lesson: price the INPUT
  growth of a tool-loop, not just the output length.**
- Windows only: the CLI reconfigures stdout to UTF-8. The model writes `₹`, and cp1252
  raises `UnicodeEncodeError` at the *print* step — after the API has been paid for.

**NEEDS a one-time schema change** (schema isn't in the repo, per the Files-table note):
```sql
alter table watchlist
  add column if not exists thesis_score   int,
  add column if not exists thesis_verdict text,
  add column if not exists thesis_pillars jsonb,
  add column if not exists thesis_md      text,
  add column if not exists thesis_at      date;
```
The dashboard watchlist shows **Score** and **Verdict** as two columns next to the name
(`app.tab_watchlist`); both render **"—" until a thesis has been run** — a blank means
"not researched yet", which must never look like 0/14.

**Delivery:** Telegram — short verdict message + the full note as an attached `.md`
(`notify.send_telegram_document`). Lakshmi asked for a WhatsApp PDF; WhatsApp needs the
Business API (Meta business account, verified number, template approval, per-message
cost), so this uses the pipe we already own. Flagged, not silently substituted.

**Test:** `python dryrun.py thesis "Krishival Foods (XNSE:KRISHIVAL)"` — prints the note,
sends nothing, stores nothing.

## Trading scorecard (`metrics.py` + `app.tab_scorecard`, Vishal 22-Aug-2026)
*"If you can't measure, you can't improve"* — scores DECISIONS, not the market, from
CLOSED trades only. Lives in its own module so the dashboard and the digest can never
disagree (the two-code-paths problem that split the digest and dashboard twice).

**Headline pairing is win rate + payoff ratio; neither means anything alone.** Measured
22-Aug-2026: Lakshmi wins only **29%** of trades yet compounds, because winners average
**+49.3%** and losers **−7.4%** (payoff **4.31:1**), and he holds winners **136 days vs
56** for losers. That is textbook trend-following and the opposite of the retail norm —
without measuring it he would call himself a poor stock-picker; he is actually a good
loss-cutter. Vishal 53% / 1.73:1, Abinaya 47% / 1.77:1.

**DATA GOTCHA: `realised.pct_gain_loss` is stored as a FRACTION** (0.090 = 9.05%). The
dashboard's `"{:+.2%}"` format multiplies by 100 so it renders right there, but ANY plain
arithmetic on that column is 100x out. `metrics._clean` therefore recomputes the % from
`gain_loss / amount_invested` and never trusts the stored column.
Synthetic rows (the FY "Opening balance" entry) are excluded from every trade statistic —
otherwise a ₹9.1L non-trade shows up as the best trade ever made.

**Drawdown caveat:** computed from weekly `digest_history` value snapshots, so deposits,
withdrawals AND data corrections move it. Lakshmi's early 20-Jul snapshot (₹3.76 Cr vs
₹3.30 Cr now) makes the current −12% read look like a loss it isn't. Labelled on screen
as lived experience, not a pure return series; it gets more trustworthy as clean weeks
accumulate.

**XIRR IS NOW NET OF ESTIMATED CHARGES (`charges.py`, 22-Aug-2026), pre-tax.** We store
no charges, and the broker accounts are behind a login — but the RATE CARDS are public,
and for delivery equity the cost is almost entirely statutory (STT, stamp duty, exchange,
SEBI, GST are identical at every broker; only brokerage and DP fees differ). So each
transaction's cost is reconstructed from its amount, side and the broker's published
schedule. Buys are grossed UP and sells netted DOWN before the XIRR solve — not a flat %
shaved off the answer.
Rates verified 22-Aug-2026 from zerodha.com/charges, upstox.com/brokerage-charges and the
INDmoney card. Broker per portfolio: **pf1 INDmoney · pf2 Zerodha/Kite · pf3 Upstox**
(`charges.PF_BROKER`). Measured impact — **~0.12% of turnover, ~1 point of XIRR**:
Vishal 37.31→36.26, Lakshmi 45.03→43.85, Abinaya 53.24→52.43.
The dashboard shows the NET figure with gross in the tooltip. **It is an ESTIMATE and is
labelled as one everywhere** — current rates applied to past trades (statutory rates do
change), and it assumes delivery. To make it EXACT, import the broker P&L/tradebook
export, which itemises real charges.

## Exchange rule: NSE unless the company is BSE-ONLY (Lakshmi 16-Aug-2026)
*"Only add NSE since its updates are faster, unless a company is only listed in BSE."*
This is a DATA-QUALITY rule, not a preference, and the codebase already proves it:
NSE announcements come off the RSS feed **every 3 min**; BSE's own API is dead behind
an Akamai challenge, so BSE-only names are scraped from Screener **hourly** (~1h
latency). BSE live quotes are also what produced a bogus price and fired false stop
alerts (Kwality, 22-Jul-2026), and BSE/SME names need bhavcopy + corporate-action
adjustment that mainboard NSE doesn't.
**When adding: check screener.in — if the page shows an NSE symbol, use it.** The
"➕ Add to watchlist" form warns when BSE is selected. `alerts.BSE_TO_NSE` already
routes dual-listed names' FILINGS to NSE (Kwality→KPL, Time Technoplast→TIMETECHNO).
**Audit 16-Aug-2026:** watchlist is 100% NSE (13/13). Holdings carry 5 BSE entries —
CWD, Hemant Surgical, True Colors are genuinely BSE-only (correct), but **Shukra
Pharmaceuticals is dual-listed and trades on NSE as SHUKRAPHAR** while being held
under XBOM by BOTH Lakshmi (`XBOM:SHUKRAPHAR`) and Vishal (`XBOM:524632`) — a
candidate to move to NSE, though changing a holding's ticker also changes its price
history source, so do it deliberately, not casually.

## Lakshmi's exact rules (verbatim intent — don't paraphrase away the specifics)
- **Benchmark rule**: "If we are not beating [the index] by at least 2-5%,
  there's no point doing all this, we should just stop." This is WHY the
  digest has a colored verdict (green ≥5pts / amber 2-5pts / red <2pts) —
  it's answering his literal stop/continue question every week, not just
  showing a number.
- **Staged entry system ("TheWrap")**: weekly 10w/20w/40w EMA flowchart.
  States include EXIT, BE CAUTIOUS, MOMENTUM FADING, MAINTAIN/ADD, BULLISH
  SIGNAL, WAIT/WATCH, INSUFFICIENT DATA (<45wk history). Entries staged:
  10-DMA = tranche 1 (partial), 21-DMA = tranche 2 (final/full position).
- **Weekly vs index scorecard (23-Jul-2026, Lakshmi's preferred measure)**: the
  digest now shows, per portfolio, **our week's return (Δunrealised + profit
  booked that week) ÷ last week's value** vs the index over the SAME dates, and
  the weekly alpha. **Bar = `WEEKLY_ALPHA_BAR` 0.14 pts** ("what the best fund
  managers do"): ≥0.14 = must continue ✅, 0–0.14 = under the bar, <0 = behind.
  WHY it exists: he does not fully trust XIRR because it depends on buy dates he
  part-guessed — and we measured that (a ±30-day date error swings his XIRR
  50%→117%; Abinaya's alpha can even flip GREEN→RED). The weekly comparison is
  **independent of entry dates**, so it's the honest read. Keep BOTH; XIRR stays
  for the long view. Caveat: it compares snapshot-to-snapshot, so if a holding
  was unpriced in one week and priced the next, that shows up in the delta.
- **WoW picks the newest snapshot that is ≥`MIN_WOW_DAYS`(4) old — it does NOT just
  take the latest one (22-Aug-2026).** The 08-Aug sub-week guard originally NULLED a
  too-recent snapshot, which broke every off-cadence run: regenerating the digest the
  morning after it had already run showed "baseline set this week" on every metric even
  though the previous week's snapshot sat one row below in the table (Vishal: *"you have
  the snapshot of last week, why can't you compare?"* — he was right). The query now
  pulls the last 8 snapshots and walks back to the first real week, logging which one it
  chose. Friday's scheduled run is unaffected (its newest prior IS ~7 days old); ad-hoc
  re-runs now compare properly instead of silently degrading to a baseline.
- **Profit booking framework**: tiers at +50% / +100% / +150% — digest
  flags when a stock CROSSES a tier this week (not just "is above").
- **YTD realised P&L on the Indian FY (Lakshmi 23-Jul-2026)**: the dashboard's
  "Realised" KPI and the Realised tab show profit booked **1 Apr–31 Mar TO DATE**
  (`signals.fy_bounds`), auto-resetting each 1 April; all-time realised moves to
  the tooltip / a secondary line. Window is [FY-start, today] — capped at today so
  a future-dated sale (there's one bad row, 2026-12-01) can't inflate "to date".
  NOTE: FY-to-date can exceed all-time when earlier FYs were net-negative (true
  for Vishal). Also shown per-portfolio in the weekly digest ("Realised · FYxx-yy
  (to date)"). Data hygiene: Vishal's portfolio has 4 FUTURE-dated realised rows
  (ids 9–12, sale_date Sep/Dec 2026) — entry typos; the "to date" cap excludes
  them, but they should be corrected in the realised table.
- **FRIDAY SNAPSHOT IS RE-STAMPED SATURDAY 06:30 IST (`alerts.run_snapshot_refresh`,
  Vishal 22-Aug-2026).** He enters the week's trades LATE on Friday night — after the
  21:00 digest has already run and stored its snapshot. That stale snapshot then became
  next Friday's comparison baseline, so a whole week got measured against a book that
  was already out of date when saved. His words: *"That is the close of the week,
  Friday. If I am doing it late, then it needs to be updated so that that number can be
  used to compare next Friday's close."*
  The Saturday job re-computes and upserts the row with **snap_date = FRIDAY** (conflict
  key is (portfolio_id, snap_date), so it REPLACES rather than adds) — the
  Friday-to-Friday cadence stays exactly 7 days and never becomes Sat→Fri. Verified
  22-Aug-2026: pf1's Friday row went ₹22,44,755/27 holdings → ₹23,53,794/30 holdings,
  and next Friday still reads 7 days. Manual tick-box: `refresh_snapshot_now`.
  **It reuses `run_digest`'s own computation with `SNAPSHOT_DATE_OVERRIDE` set, rather
  than re-deriving the money numbers** — two code paths answering the same money
  question is exactly how the digest and dashboard drifted apart twice.
  **Do NOT "fix" this by storing a Saturday-dated snapshot** — that yields a 6-day
  Sat→Fri window, which is the bug this replaced.
- **Weekly review habit**: reviews the portfolio Saturday mornings — this
  is WHY the digest moved from Sunday 10am to Friday 9pm (after Friday's
  close, so Saturday's review uses fresh data, not day-old).
- Wants delivery % as CONTEXT ONLY — it must never gate/change a flowchart
  state, only displayed alongside for the human to weigh.

## Feature request backlog, in Lakshmi's own framing
- **Volume spike alerts**: wanted something like a ScoutQuest alert he
  showed ("Capacite Infraprojects — 2.02x average volume") — built as
  pace-adjusted (scales partial-week volume vs a full week's average, so
  a Monday-morning spike doesn't need to wait till Friday to trigger).
  Threshold 2.0x, one alert per stock per group per day.
- **Watchlist entry alerts**: system was silently NOT alerting on
  watchlist stocks (only holdings) — a real gap Vishal caught by asking
  "how does the watchlist work in our system?". Now fires on 10/21-DMA
  zone touches and personal target-price hits (per-person, not a min() —
  an early bug suppressed one person's alert because of the other's
  deeper target; fixed to fire on ANY member's target).
- **Portfolio add-zone alerts + instant entry zones (21-Jul-2026, Lakshmi)**:
  (a) held stocks now get a 21-DMA (final add) alert — `alerts.check_holding_adds`,
  dedup kind ADD21, portfolio-scoped like everything else. (Lakshmi, 21-Jul-2026:
  holdings need ONLY the 21-DMA add signal, not the 10-DMA — that tranche-1 level
  stays a watchlist-only concern. Holdings table likewise shows 21-DMA only.)
  (b) the 10/21-DMA entry zones show automatically on both tabs (watchlist shows
  both 10+21-DMA; holdings shows 21-DMA) via cached `app.fetch_entry_zones` — the
  old watchlist "Check entry zones" button is gone. NOTE the "instant" Lakshmi
  wanted was for the TELEGRAM ALERTS (act fast to buy/sell), NOT dashboard load —
  the dashboard is review-only, a brief compute-on-load is fine; (c) CRUCIAL fix underneath: `signals.daily_entry_state` daily EMA math
  is now **bhavcopy-first** (`signals._fetch_daily`), Yahoo only for mainboard —
  the old Yahoo-only path silently skipped every SME name (same blind spot as
  rule #1). SME daily EMAs ride the split-adjusted bhavcopy read (House Rule 10).
- **AI filing summaries**: inspired by ScoutQuest's format (headline +
  bullet gist). Built using Claude reading the filing PDF NATIVELY (base64
  document block) rather than text-extraction — handles scanned/image PDFs
  that a text-extraction pipeline (pypdf) could not. Cost-capped at
  10 summaries/run, 20MB/PDF max.
- **NOT YET BUILT — discussed 20-Jul-2026**: Vishal found a competitor
  (myalerts.in) whose results-filing summaries use a TYPED template
  (Revenue/EBITDA/EBITDA-margin/PAT/EPS, each with YoY AND QoQ deltas,
  segment-level breakdown) rather than generic bullets. Agreed direction:
  classify each filing by TYPE first (results / order win / fund-raise /
  pledge change / acquisition / auditor-resignation / capex / dividend),
  then apply a type-specific extraction template. Results filings contain
  the prior-period comparison columns already, so YoY/QoQ can be computed
  from the single PDF Claude already reads — no historical DB needed.
  Pledge changes and auditor/CFO resignations should get an urgent
  emoji/flag (classic smallcap red flags). Separately, myalerts.in also
  does bulk/block-deal and insider-trading alerts — DIFFERENT data feeds
  entirely (not filings), same bhavcopy-style daily-file pattern, not
  started. This whole item is the natural "next session" build.

## Detailed debugging war stories (context for why the rules above exist)
- **Bogus live quote fired false stops (22-Jul-2026)**: Yahoo's `fast_info`
  returned ₹498.65 for Kwality (`539997.BO`) while the stock was really ~₹2,689.
  That single wrong number fired BOTH a false loss-stop ("-42% below cost") and a
  false trailing-stop ("-82% from peak") on a holding that is actually UP ~210%.
  The peak and cost were fine — only the live CMP was garbage. Root cause: the
  DASHBOARD had a plausibility guard (`MAX_PLAUSIBLE_MOVE`) but the ALERT engine
  did not, so `_live_quotes` was trusted blindly. Fix: `alerts._sane_quotes`
  rejects any live quote >25% from the last daily close (Indian circuits are
  5/10/20%, so beyond that it's garbage, not a move) and logs WHY; the ticker is
  simply skipped that cycle. **Lesson: every NEW consumer of a flaky source needs
  its own sanity check — porting the logic isn't enough, you must port the guard.**
  Follow-up worth doing: BSE-numeric holdings (539997 Kwality, 542669, 532856…)
  are priced by YAHOO, not bhavcopy — adding them to `SME_STOCKS` would own that
  data properly (rule #1) instead of trusting Yahoo for BSE names.
- **SME pricing bug**: Yahoo served STALE/WRONG prices for 5 of 6 SME
  stocks (e.g. CWD showed a fake ₹1,180 vs real ₹311) while PORTFOLIO
  TOTALS coincidentally still matched Kite — only caught via PER-STOCK
  reconciliation against the broker statement. Totals can lie by
  coincidence; always reconcile stock-by-stock, not just in aggregate.
- **The 5.5-hour backfill**: adding ONE new fragile fetch (index CSV)
  inside the existing per-day loop of a ~490-day backfill meant EVERY
  iteration paid a 20s hang on top of normal work. Lesson became rule #4
  above. The fix was a dedicated fast `index-backfill` mode isolated from
  the main backfill, tested via `check` mode first.
- **NSE benchmark hunt**: tried nsearchives host (503s) → tried nseindia
  legacy `ind_close_all` CSV (uniform 404 across 500+ dates, confirmed
  discontinued around their July-2024 format changes) → Yahoo `^CNXSC`
  (empty, then a useless 1-day stub that produced a nonsensical "-0.0%"
  index return and a fake "alpha = entire XIRR") → landed on
  HDFCSML250.NS/MOSMALL250.NS ETF proxies priced via the ALREADY-WORKING
  daily bhavcopy job. Total resolution took 3 real days across several
  false starts — the lesson each time was "verify against the actual file
  response, don't trust that a fix worked just because code deployed
  without errors."
- **Global-declaration SyntaxError**: `ast.parse()` did NOT catch a
  Python "name assigned before global declaration" error — only a full
  `compile()` did. Now use `compile(src, filename, "exec")` as the
  stricter verification step for any file with `global` statements.
- **Filing feed rewrite**: the original per-symbol NSE announcement fetch
  hit `www.nseindia.com`'s API, which stonewalled Actions' IP with 60/60
  read-timeouts (the whole filings feature was silently dead for a
  stretch). Rewrote to pull the ENTIRE feed once via
  `nsearchives.nseindia.com/content/RSS/Online_announcements.xml` (RSS,
  same friendly host bhavcopy uses) and filter client-side by symbol,
  title-anchored (`^SYMBOL\b`) to avoid short-symbol collisions (e.g.
  "TCL" matching inside unrelated text). BSE announcements needed an
  explicit date range param (empty dates now return "No Record Found!"
  even for valid scrip codes) and numeric-scrip-code mapping (XBOM
  symbols aren't valid API params, only their numeric codes are).
- **Chunked Telegram dispatch**: original filings sender capped output at
  `alerts[:15]` — silently dropping any 16th+ filing on a busy day AND
  risking exceeding Telegram's 4096-char message limit on fat AI-gist
  messages (could have lost an ENTIRE day's batch, not just excess).
  Replaced with character-budget-based chunking (~3500 chars/message,
  splits into as many messages as needed, nothing ever dropped).

## Environment / access notes
- Vishal is non-technical, Windows, now using **VS Code + the official
  Claude Code extension** (chosen over Claude Desktop's Code tab and over
  Google Antigravity — decided 21-Jul-2026 for staying on native Claude
  with a visual file tree).
- Local repo cloned via GitHub Desktop to a folder on his PC; this
  CLAUDE.md sits at that folder's root alongside app.py.
- Git workflow going forward: Source Control panel in VS Code (stage →
  commit message → Commit → Sync/Push) — replaces the manual
  copy-paste-upload-to-GitHub dance used throughout this chat's history.

## Testing alerts safely (no Telegram spam) — `ALERTS_DRY_RUN`
Set env `ALERTS_DRY_RUN=1` and (a) `notify.send_telegram`/`send_email` PRINT the
message instead of delivering, and (b) `alerts.sb()` returns a READ-ONLY client
so NO dedup/state row is written (a real dry-run must not mark items "seen" or
the next scheduled run would skip them). Two ways to use it:
- **Locally:** `python dryrun.py <mode>` (reads .streamlit/secrets.toml, sets the
  flag). Tests logic + data fetches + exact message text from your own IP.
- **Real Actions env:** the `workflow_dispatch` **dry_run** tick-box sets
  ALERTS_DRY_RUN for that run — proves the datacenter-IP data fetches (BSE/Yahoo)
  and secrets without sending. Scheduled runs are never dry (input is empty).

When something fails: (1) check the log says WHY, not just THAT it failed
— if it doesn't, that's a logging gap to fix first; (2) verify against the
real exchange file/API response before trusting docs or search results;
(3) prefer a 2-minute diagnostic (`bhavcopy.py check`) over a long backfill
when testing something new; (4) "did the file I think is deployed actually
get uploaded" is always worth checking before deeper debugging — several
bugs this session were stale-file issues, not logic issues.