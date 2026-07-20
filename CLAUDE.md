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
- Hourly, 9:45–15:45 IST, Mon-Fri: flowchart states + volume spikes (2x
  pace-adjusted) + watchlist entry-zone/target alerts
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
- Filing-summary classification is currently generic (2-4 bullets for any
  filing type). Discussed building TYPED templates (results filings get
  Revenue/EBITDA/PAT/EPS with YoY/QoQ, order wins get value/client/timeline,
  pledge changes get a 🚨 flag, etc.) — not yet built, next natural feature
- Bulk/block deals and insider-trading alerts (separate NSE/BSE daily data
  feeds, same bhavcopy-style pattern) — discussed, not yet built
- Resend domain verification still pending on Vishal's side (digest email
  deliverability)

## Debugging philosophy for this project
When something fails: (1) check the log says WHY, not just THAT it failed
— if it doesn't, that's a logging gap to fix first; (2) verify against the
real exchange file/API response before trusting docs or search results;
(3) prefer a 2-minute diagnostic (`bhavcopy.py check`) over a long backfill
when testing something new; (4) "did the file I think is deployed actually
get uploaded" is always worth checking before deeper debugging — several
bugs this session were stale-file issues, not logic issues.