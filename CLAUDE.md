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
- **Hosting**: Render, two Web Services from the same repo, distinguished by
  `APP_TENANT` env var ("vishal" / "lakshmi"). Free tier — watch for
  memory/perf limits (see Known Issues).
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
| `signals.py` | Weekly EMA flowchart states, entry-zone (DMA) math |
| `alerts.py` | The alert engine — states, volume spikes, watchlist entries, filings, digest |
| `bhavcopy.py` | Official NSE/BSE daily price files — the Yahoo-blind-spot fix |
| `delivery.py` | NSE + BSE daily delivery % pipeline |
| `fundamentals.py` | Screener.in scraper — Market Cap/PE/Book Value (Yahoo `.info` is blocked) |
| `exit_audit.py` | 30/60/90-day post-exit price checks |
| `corporate_actions.py` | Split/bonus adjustment for bhavcopy prices + unadjusted-gap detector (see House Rule 10) |
| `dryrun.py` | Test-run any alert mode against live data, PRINTING what would be sent — no Telegram, no DB writes (`python dryrun.py deals\|eod-entries\|filings-nse\|states\|fast-poll\|digest`) |
| `.github/workflows/alerts.yml` | The single CI workflow — see Schedule below |
| `*_schema.sql` | One-time Supabase schema additions, already applied |

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

## Architecture: portfolio-scoping
Every table has `portfolio_id`. Alerts aggregate per (group, ticker) so a
stock BOTH Lakshmi and Abinaya hold gets ONE Telegram message tagged
`[Both]`, not two. Dedup (state changes, volume spikes, entry zones, filings)
is always keyed to include portfolio/group scope — never just ticker alone,
or one person's alert can suppress another's.

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

## Schedule (`.github/workflows/alerts.yml`)
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
- Dashboard shows **% distance** to the DMAs/10wEMA, never the ₹ level (Lakshmi
  22-Jul-2026: "how far from the zone" is the decision; the rupee value isn't).
- **Risk / stop alerts (`alerts.check_risk_stops`, Lakshmi 21-Jul-2026)**: fires
  when a HOLDING is ≥10% below cost (loss stop, per each holder's own cost) OR
  ≥17% off its ~6-month peak (trailing stop; peak = `signals.daily_entry_levels`
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
  natively — handles scanned docs; capped at 10 summaries/run, 8MB/PDF)
- Daily 20:00 IST: bhavcopy + delivery + fundamentals
- Daily 20:30 IST: exit audit (30/60/90-day post-sale price checks)
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
- EV/EBITDA not available (screener.in free page doesn't reliably expose it
  — would need a balance-sheet parser)
- Young listings show "INSUFFICIENT DATA" state until 45+ weeks of price
  history exist — self-heals, no action needed
- Render free-tier stability under real load — watch for exit-139 crashes;
  root-caused once already to Yahoo retry storms (fixed by excluding SME
  tickers from Yahoo calls and reducing quote-fetch threads 8→4)
- Filing-summary classification: RESULTS filings now use a TYPED template
  (21-Jul-2026) — consolidated Revenue/EBITDA/PBT/PAT/EPS, each with QoQ + YoY %
  (`alerts._summarize_results`/`_format_results`). Claude EXTRACTS raw line items
  from the PDF; Python computes EBITDA (=PBT+finance+depreciation), the %s, and
  unit→Cr — so no model-arithmetic error reaches a number (rule #2); unusable
  extraction falls back to the generic bullet gist. STILL generic for other
  types (order wins value/client/timeline, pledge/auditor 🚨 flags) — next.
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
  - **`python alerts.py filings-audit`** (new): read-only, no Telegram — lists,
    for every NSE holding, which of today's filings the engine matches. Run it
    anytime to spot-check coverage across the WHOLE portfolio (built after the
    one-example-at-a-time problem — this is the scalable check).
  - **Filings cadence (split by exchange):** NSE announcements run **every 15
    min** (`filings-nse`, `run_filings(nse_only=True)`, 08:30–23:15 IST) — the
    archives host is friendly, safe to poll often. BSE stays on the **2-hourly
    full run** (`filings`) — its API is bot-hostile and shares the runner IP with
    the daily SME bhavcopy, so it must NOT be hammered. Was twice-daily; results/
    board outcomes drop in the EVENING and the NSE feed is only a ~1-day snapshot,
    so infrequent polling let them age off unseen. `MATERIAL_KEYWORDS` now
    includes "board meeting" (results are decided there).
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
- Resend domain verification still pending on Vishal's side (digest email
  deliverability)

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
- **Profit booking framework**: tiers at +50% / +100% / +150% — digest
  flags when a stock CROSSES a tier this week (not just "is above").
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
  10 summaries/run, 8MB/PDF max.
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