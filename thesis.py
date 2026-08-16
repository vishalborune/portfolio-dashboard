"""
thesis.py — the Stage-2 thesis scorecard.

WHY THIS EXISTS (Lakshmi, 16-Aug-2026)
Lakshmi picks stage-2 breakout candidates faster than he can research them. He
said it plainly: he *can't spend the time to get context on all these different
companies*, and that's why his hit-rate on them is poor. So when a name goes on
the watchlist, this produces the context for him: a 7-pillar score out of 14, a
verdict, the bear case, and a dated action tied to the 5/21-DMA tranches he
already trades.

THE DESIGN DECISION THAT MATTERS
The workshop prompt this grew out of has the model research *everything*,
including the financials. We don't. Anything we can prove, we prove:

    RQ  results quality   <- screener_data.py (parsed quarterly table)
    MG  margin direction  <- screener_data.py (OPM trend, in bps)
    VA  valuation         <- screener_data.py (P/E, P/B, ROCE, ROE, CAGR)
    TE  technical         <- signals.py — SCORED IN PYTHON, not by the model

The model researches only what no table can tell us — durability (DU), smart
money (SM) and governance (GV) — and it must cite a source for each. It never
computes a number and never assigns the technical score. Python owns every
percentage, the pillar total, and the verdict tier.

That is not a stylistic preference. Every wrong-number bug this project has
shipped came from a model transcribing or arithmetic-ing something it should
have been handed (Clean Max's swapped columns, Styrenix's misparsed date,
Advent's exceptional item, Krishival's "margin pressure" verdict). Here the
model's job is judgment; the numbers arrive already correct. And because TE
comes from the same `signals` functions the alerts use, a thesis and a
5/21-DMA alert can never disagree about the same stock.

FAIL-SAFE: with no verified financials we do NOT publish a score. An unscored
"INSUFFICIENT DATA" is honest; a 0 on RQ would read as "bad company" and push a
good one to AVOID. A blank beats a wrong number (House Rule #2).

Usage:
    python thesis.py "Krishival Foods (XNSE:KRISHIVAL)"
    python thesis.py "HFCL Ltd (XNSE:HFCL)" --reason "order book + margin turn"
    python thesis.py ... --dry        # print, don't send or store

Env: ANTHROPIC_API_KEY (required), SUPABASE_* (only to store)
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date

import screener_data
import signals

MODEL = "claude-opus-5"

# --- cost controls ---------------------------------------------------------
# Measured 16-Aug-2026: the FIRST live run cost ~$1 for ONE stock, against my
# estimate of $0.35-0.50. The miss was the input side, not the output: each web
# search round re-sends every result gathered so far, so 17 rounds compound the
# input tokens far faster than the visible answer grows. Defaults are therefore
# tuned DOWN and every run now prints what it actually spent (below) — a cost
# surprise should never happen twice.
#
# Research is search-and-summarise, not deep reasoning, so it does not need
# high effort. Scoring is the judgement call and keeps it.
RESEARCH_EFFORT = os.environ.get("THESIS_RESEARCH_EFFORT", "medium")
SCORE_EFFORT = os.environ.get("THESIS_SCORE_EFFORT", "high")
MAX_SEARCHES = int(os.environ.get("THESIS_MAX_SEARCHES", "6"))
RESEARCH_MAX_TOKENS = int(os.environ.get("THESIS_RESEARCH_TOKENS", "6000"))
SCORE_MAX_TOKENS = int(os.environ.get("THESIS_SCORE_TOKENS", "8000"))

# Opus 5 list prices, $ per token. Web search is $10 per 1,000 requests.
_PRICE_IN = 5.0 / 1_000_000
_PRICE_OUT = 25.0 / 1_000_000
_PRICE_CACHE_READ = 0.50 / 1_000_000
_PRICE_CACHE_WRITE = 6.25 / 1_000_000
_PRICE_SEARCH = 10.0 / 1_000

# --- scoring constants -----------------------------------------------------
PILLARS = [
    ("RQ", "Results Quality"),
    ("DU", "Durability"),
    ("MG", "Margin Direction"),
    ("VA", "Valuation"),
    ("SM", "Smart Money"),
    ("GV", "Governance"),
    ("TE", "Technical"),
]
MODEL_PILLARS = ["RQ", "DU", "MG", "VA", "SM", "GV"]   # TE is computed here

TIERS = [(12, "STRONG SETUP"), (9, "BUILD SLOWLY"), (6, "WATCHLIST"), (0, "AVOID")]
VERDICT_ICON = {"STRONG SETUP": "🟢", "BUILD SLOWLY": "🟡",
                "WATCHLIST": "🔵", "AVOID": "🔴"}

# How close to a DMA still counts as "at the line" for the technical pillar.
# Same spirit as alerts.MORNING_NEAR_PCT (1.5%) but looser, because this is a
# thesis-level read ("is it buyable near support") rather than an order trigger.
TE_NEAR_PCT = 4.0

# States that are structurally NOT stage-2 — no technical credit regardless of
# where price sits relative to the DMAs.
TE_BLOCKING_STATES = {"EXIT", "BE CAUTIOUS"}
TE_WEAK_STATES = {"MOMENTUM FADING", "WAIT/WATCH"}


def _dry() -> bool:
    return os.environ.get("ALERTS_DRY_RUN") == "1"


# ---------------------------------------------------------------------------
# 1. the data pack — everything we can prove, before any model is involved
# ---------------------------------------------------------------------------

def _pct_from(level, close):
    if not level or not close:
        return None
    return round((close / level - 1) * 100, 2)


def technical_pack(ticker: str) -> dict:
    """Our own trend/level read, from the SAME functions the alerts use."""
    pack = {"ticker": ticker}
    try:
        st = signals.current_state(ticker) or {}
    except Exception as e:
        print(f"  [thesis] current_state({ticker}) failed: {type(e).__name__}: {e}")
        st = {}
    pack["state"] = st.get("state")
    pack["weekly_close"] = st.get("close")
    for k in ("ema10", "ema20", "ema40"):
        if st.get(k) is not None:
            pack[f"w{k}"] = round(float(st[k]), 2)

    try:
        lv = signals.daily_entry_levels(ticker) or {}
    except Exception as e:
        print(f"  [thesis] daily_entry_levels({ticker}) failed: {type(e).__name__}: {e}")
        lv = {}
    close = lv.get("ref_close")
    pack.update({
        "close": close,
        "dma5": lv.get("ema5"), "dma10": lv.get("ema10"), "dma21": lv.get("ema21"),
        "peak_6m": lv.get("peak"), "high_52w": lv.get("high_52w"),
        "pct_vs_5dma": _pct_from(lv.get("ema5"), close),
        "pct_vs_10dma": _pct_from(lv.get("ema10"), close),
        "pct_vs_21dma": _pct_from(lv.get("ema21"), close),
        "pct_off_52w_high": _pct_from(lv.get("high_52w"), close),
    })
    try:
        pack["support"] = signals.support_levels(ticker) or {}
    except Exception:
        pack["support"] = {}
    return pack


def score_technical(tp: dict) -> dict:
    """TE, 0-2, computed in Python — never asked of the model.

    2 = confirmed uptrend AND price is at/near a tranche line (buyable now)
    1 = uptrend intact but extended from the lines (wait for a pullback)
    0 = not in a stage-2 uptrend, or we can't prove one

    The 'or we can't prove one' case matters: a young listing with no 40-week
    EMA is UNVERIFIED, not bullish, and it must not collect technical credit."""
    state = tp.get("state")
    close, d21 = tp.get("close"), tp.get("dma21")

    if not state or state in ("INSUFFICIENT DATA", "NO DATA"):
        return {"score": 0, "reason": f"no verifiable weekly trend yet (state: {state or 'unavailable'}) "
                                      f"— too little price history to confirm stage 2"}
    if state in TE_BLOCKING_STATES:
        return {"score": 0, "reason": f"weekly flowchart says {state} — not a stage-2 uptrend"}
    if close is None or d21 is None:
        return {"score": 0, "reason": "daily EMA levels unavailable — cannot verify the entry zone"}
    if close < d21:
        return {"score": 0, "reason": f"close Rs {close:,.2f} is BELOW the 21-DMA Rs {d21:,.2f} — no uptrend to buy"}

    near = min([abs(x) for x in (tp.get("pct_vs_10dma"), tp.get("pct_vs_21dma")) if x is not None] or [999])
    if state in TE_WEAK_STATES:
        return {"score": 1, "reason": f"above the 21-DMA but the weekly state is {state} — "
                                      f"trend is not confirmed, so half credit at best"}
    if near <= TE_NEAR_PCT:
        return {"score": 2, "reason": f"{state} and price is {near:.1f}% from a tranche line "
                                      f"(10-DMA Rs {tp['dma10']:,.2f} / 21-DMA Rs {d21:,.2f}) — buyable here"}
    return {"score": 1, "reason": f"{state} but extended {near:.1f}% above the tranche lines — "
                                  f"trend is fine, the entry is not"}


def build_pack(stock_name: str, ticker: str) -> dict:
    """Verified facts only. No model has touched anything in here."""
    company = re.sub(r"\s*\([^)]*\)\s*$", "", str(stock_name)).strip()
    print(f"[thesis] building data pack for {company} ({ticker})…")
    fundamentals = screener_data.snapshot(ticker, company)
    tech = technical_pack(ticker)
    return {
        "company": company,
        "stock_name": stock_name,
        "ticker": ticker,
        "as_of": date.today().isoformat(),
        "fundamentals": fundamentals,
        "technical": tech,
        "technical_score": score_technical(tech),
    }


# ---------------------------------------------------------------------------
# 2. research — the ONLY step allowed to reach outside our own data
# ---------------------------------------------------------------------------

RESEARCH_SYSTEM = """You are a sell-side equity analyst researching an Indian smallcap \
for a private investor who already owns a concentrated smallcap book.

You will be given VERIFIED FINANCIAL AND TECHNICAL DATA that has already been parsed \
from primary sources by the caller's own pipeline. Treat those numbers as settled fact. \
Do NOT re-derive, re-check, restate with different values, or "correct" them. Your \
research exists to explain and stress-test what those numbers cannot show.

Research ONLY these three questions, in this order of importance:

1. DURABILITY — is the recent growth repeatable, or a one-off? Look for: management \
guidance, order book / order wins, capacity expansion and its commissioning date, \
capex, new client wins, and whether an easy base is flattering the growth rate. \
Concall transcripts and investor presentations are the best sources.

2. SMART MONEY — is anyone informed buying? Look for: promoter purchases from the open \
market, preferential allotments, buybacks, QIPs, new institutional/FII/DII entry, and \
bulk or block deals. Distinguish a promoter BUYING from a promoter merely holding.

3. GOVERNANCE — what would make this uninvestable? Look for: auditor resignation or a \
qualified opinion, promoter share pledging, related-party transactions, cash flow from \
operations that does not track reported profit, receivable or inventory build-up, \
regulatory or tax action, and any history of missed guidance.

RULES, IN ORDER OF PRIORITY:
- NEVER invent, estimate, round or infer a figure. If you did not read it in a source, \
you do not have it.
- Prefer primary sources: exchange filings (NSE/BSE), the annual report, concall \
transcripts, investor presentations. A news article summarising a filing is second best. \
Anonymous blogs, forum posts and social media are NOT acceptable evidence.
- Run at least 3 distinct searches before concluding. If a claim cannot be sourced, \
write it under "UNVERIFIED" and say what you looked for and failed to find. An honest \
gap is far more useful to this reader than a confident guess.
- Absence of evidence is not evidence of absence. "No pledge disclosed on screener" is \
NOT "no pledge" — say which you actually established.
- Note the DATE of anything you cite. Stale guidance is a different fact from current \
guidance.

Write plain prose notes under the three headings, then an UNVERIFIED section, then a \
SOURCES list with full URLs. No score, no recommendation — that comes later."""


def _client():
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("the `anthropic` package is not installed — `pip install anthropic` "
                           "(it is in requirements.txt)")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY is not set — cannot build a thesis")
    return anthropic.Anthropic()


def _add_usage(tally: dict, usage) -> dict:
    """Accumulate one request's usage. MUST be summed across pause_turn resumes:
    each resume is a separate billed request, so reading only the final message's
    usage under-reports a long research run — which is exactly how the first
    cost estimate came out low."""
    for attr, key in (("input_tokens", "in"), ("output_tokens", "out"),
                      ("cache_read_input_tokens", "cache_read"),
                      ("cache_creation_input_tokens", "cache_write")):
        tally[key] = tally.get(key, 0) + int(getattr(usage, attr, 0) or 0)
    stu = getattr(usage, "server_tool_use", None)
    tally["searches"] = tally.get("searches", 0) + int(
        getattr(stu, "web_search_requests", 0) or 0)
    return tally


def _cost(tally: dict) -> float:
    return (tally.get("in", 0) * _PRICE_IN
            + tally.get("out", 0) * _PRICE_OUT
            + tally.get("cache_read", 0) * _PRICE_CACHE_READ
            + tally.get("cache_write", 0) * _PRICE_CACHE_WRITE
            + tally.get("searches", 0) * _PRICE_SEARCH)


def _report_cost(label: str, tally: dict):
    print(f"  [thesis] {label}: {tally.get('in', 0):,} in + {tally.get('out', 0):,} out "
          f"+ {tally.get('searches', 0)} searches  ->  ~${_cost(tally):.2f}")


def _final_message(client, **kw):
    """One streamed request, resuming across `pause_turn`.

    Server-side web search runs its own sampling loop and stops with
    `pause_turn` when it hits the per-turn iteration cap; the turn is NOT
    finished. Without this loop a long research run silently returns a partial
    answer and looks like a complete one (House Rule #7 — a non-empty response
    is not automatically a usable one). Streaming because Opus 5 thinks by
    default, which makes these calls slow enough to trip HTTP timeouts."""
    messages = list(kw.pop("messages"))
    tally = {}
    for attempt in range(6):
        with client.messages.stream(messages=messages, **kw) as stream:
            msg = stream.get_final_message()
        _add_usage(tally, msg.usage)
        if msg.stop_reason != "pause_turn":
            if msg.stop_reason == "refusal":
                raise RuntimeError("the model declined this request (stop_reason=refusal)")
            return msg, tally
        # Resume: hand the paused turn straight back, no extra user message.
        messages = messages + [{"role": "assistant", "content": msg.content}]
        print(f"  [thesis] server tools paused mid-turn — resuming ({attempt + 1}, "
              f"~${_cost(tally):.2f} so far)")
    raise RuntimeError("web research did not finish after 6 resumes")


def _text_of(msg) -> str:
    return "\n".join(b.text for b in msg.content if b.type == "text").strip()


CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "thesis")


def _cache_path(ticker: str, as_of: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", f"{ticker}_{as_of}")
    return os.path.join(CACHE_DIR, f"{safe}.research.txt")


def research(pack: dict, reason: str = "", use_cache: bool = True) -> dict:
    """Web research for DU / SM / GV. Returns {notes, searches}.

    CACHED FOR THE DAY. Research is the expensive half of a thesis — a dozen-plus
    web searches plus a long Opus generation — and it is the half that does NOT
    change between two runs on the same afternoon. The first live run proved why
    this is needed: research completed (17 searches, 18k chars) and was then
    thrown away when the scoring call failed, so re-running meant paying for all
    of it again. Same lesson as never bolting a fragile fetch into a long job
    (House Rule #4): make the expensive step resumable."""
    path = _cache_path(pack["ticker"], pack["as_of"])
    if use_cache and os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                notes = fh.read()
            if notes.strip():
                print(f"  [thesis] reusing today's cached research ({len(notes)} chars) "
                      f"— $0.00, no API call")
                return {"notes": notes, "usage": {}, "cost": 0.0, "cached": True}
        except Exception as e:
            print(f"  [thesis] cache read failed ({type(e).__name__}) — researching fresh")

    client = _client()
    why = (f"\n\nWhy the investor flagged this stock (his words): \"{reason.strip()}\"\n"
           f"Treat that as a hypothesis to TEST, not a conclusion to support."
           if reason and reason.strip() else "")
    prompt = (
        f"Company: {pack['company']}  |  Ticker: {pack['ticker']}  |  "
        f"Today: {pack['as_of']}\n\n"
        f"VERIFIED DATA ALREADY ESTABLISHED (do not re-derive):\n"
        f"```json\n{json.dumps(pack['fundamentals'], indent=1, default=str)}\n```\n"
        f"```json\n{json.dumps(pack['technical'], indent=1, default=str)}\n```"
        f"{why}\n\nResearch durability, smart money and governance for this company now."
    )
    print(f"[thesis] researching {pack['company']} "
          f"(web search, max {MAX_SEARCHES}, effort {RESEARCH_EFFORT})…")
    msg, tally = _final_message(
        client,
        model=MODEL,
        max_tokens=RESEARCH_MAX_TOKENS,
        system=RESEARCH_SYSTEM,
        tools=[{"type": "web_search_20260209", "name": "web_search",
                "max_uses": MAX_SEARCHES}],
        output_config={"effort": RESEARCH_EFFORT},
        messages=[{"role": "user", "content": prompt}],
    )
    notes = _text_of(msg)
    _report_cost("research", tally)
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(notes)
    except Exception as e:
        print(f"  [thesis] could not cache research ({type(e).__name__}: {e}) — "
              f"a re-run today will pay for it again")
    return {"notes": notes, "usage": tally, "cost": _cost(tally), "cached": False}


# ---------------------------------------------------------------------------
# 3. scoring — model judges, Python arithmetics
# ---------------------------------------------------------------------------

RUBRIC = """Score each pillar 0, 1 or 2, using these definitions exactly.

RQ  Results Quality      latest quarter: strong+clean 2 · strong+noisy 1 · weak/messy 0
DU  Durability           structural (capacity/orders/guidance record) 2 ·
                         partly cyclical/base-effect 1 · mostly base-effect/peak 0
MG  Margin Direction     expanding WITH GUIDANCE 2 · expanding unguided or stable 1 ·
                         compressing 0
VA  Valuation vs
    DELIVERED growth     cheap vs DELIVERED growth 2 · fair 1 · priced for perfection 0
SM  Smart-Money          quality investors building 2 · discussed, mixed 1 ·
                         no serious coverage 0
GV  Governance           clean 2 · watch-items 1 · real flags 0

HOW THIS RUBRIC IS MEANT TO BITE — read these before scoring:

• "strong+NOISY" is the common RQ case, not the rare one. A quarter is noisy when an
  acquisition, an ESOP charge, an exceptional item or a tiny year-ago base is doing part
  of the work. Say which, and score 1. Reserve 2 for growth that is clean on its own legs.
  Conversely, if REPORTED profit looks flat only because of a non-cash charge, say so and
  score the ADJUSTED reality — a real 14th-consecutive-record quarter is a 2 even if the
  headline PAT is flat.

• VA is valuation against growth ALREADY DELIVERED, never against guided or hoped-for
  growth. A 2 needs a multiple that is cheap against what the company has actually done —
  quote the P/E or P/B against the delivered CAGR. "Priced for perfection" (0) means the
  multiple already assumes the growth continues.

• MG 2 REQUIRES management guidance on margin. Expanding margins with no guided path is a
  1, however good the bps look. And a falling margin PERCENTAGE while operating profit in
  RUPEES grows strongly is scaling dilution, NOT compression — that is a 1, not a 0. Read
  the bps deltas and the rupee growth together.

• DU 0 is "mostly base-effect/peak". Commodity and cyclical businesses specialise in making
  peak earnings look cheap. If the year-ago quarter was tiny, or the input cycle is doing
  the work, say so plainly and score accordingly.

• SM 0 is "no serious coverage" — nothing verifiable, not merely "no promoter buying".
  A 2 needs quality money actually building: promoter open-market purchase, a preferential
  issue (note the price vs CMP), a buyback, or a named institution entering.

• GV: profits that do not become cash are a REAL FLAG, not a watch-item. The caller has
  already computed the cash-flow, receivable, debt and pledge checks and hands them to you
  under QUALITY FLAGS — a 🚨 there is a 0 or 1 on this pillar and you must reference it
  explicitly. Do not score GV 2 while a 🚨 flag is outstanding.

• Watch for narrative that the disclosed numbers do not support: if the reason the stock is
  interesting is a theme (data centres, EVs, defence) and the company's own disclosed
  segment revenue does not include it, name that gap. It is one of the most valuable things
  this report can tell the reader.

HARD RULE: if GV is 0, the verdict is capped at WATCHLIST no matter how high the total.
The caller enforces this in code, so score GV on its own merits and do not soften it to
protect the total.

CALIBRATION — real scores from this reader's own past batch, to anchor the scale:
  Windlas Biotech 11/14 (RQ2 DU2 MG1 VA1 SM2 GV2 TE1) — 14th consecutive record quarter;
    reported PAT flat ONLY on a non-cash ESOP charge, adjusted PAT +37%; Plant-6 dated and
    funded; buyback + dividend completed. MG only 1 because margin expansion was unguided.
  GIPCL 11/14 (RQ2 DU2 MG2 VA2 SM1 GV1 TE1) — PAT +175% clean; 500MW commissioning dated
    to a named quarter with EBITDA guided in rupees; ~13x P/E and 0.78x book vs peer 23x.
    VA=2 is rare and earned by being cheap against DELIVERED numbers.
  Mukka Proteins 7/14 (RQ2 DU1 MG1 VA1 SM0 GV1 TE1) — revenue +186% YoY but the year-ago
    base was tiny and an acquisition adds inorganic lift, so DU is 1 not 2; SM 0 = no
    serious coverage found.
  Susan Electricals 3/14 (RQ1 DU0 MG1 VA1 SM0 GV0 TE0) — spectacular pre-IPO P&L, but
    operating cash flow NEGATIVE three years running while receivables ballooned. GV=0
    on cash alone. This is the case your QUALITY FLAGS section exists to catch."""

SCORE_SYSTEM = """You are scoring an Indian smallcap for a seasoned private investor who \
runs a concentrated smallcap book and enters in tranches at the 10-day and 21-day EMAs.

You are given (a) VERIFIED financial and technical data parsed from primary sources, and \
(b) research notes gathered from the web. Score the six pillars asked of you.

ABSOLUTE RULES:
- Every number you state must come from the verified data or be quoted from a source in \
the research notes. Do not compute new percentages — the ones you were given are already \
correct and already checked.
- Do NOT score the technical pillar. It has already been scored from the caller's own \
trend engine and is given to you as fixed; reflect it in your narrative, never contradict it.
- If the research notes could not verify something, that is a LOW score and it belongs in \
the unverified list. Never upgrade a score to be encouraging.
- The bear case must be the strongest genuine argument against buying, written as though \
you were short the stock. A weak bear case is a failure of this task.
- The seasoned read is one paragraph of judgment for someone who has seen many of these: \
what this really is, what would change your mind, and what most people will get wrong. \
Pattern-match to how similar setups have historically resolved. Say plainly whether this \
is an investment or a momentum trade — "own it with the chart, exit with the chart, and \
never let a narrative convert a trade into a hold" is a legitimate and useful conclusion.
- ACTION follows one principle: THE SCORECARD DECIDES IF, THE CHART DECIDES WHEN. Name the \
rupee tranche levels you were given. STRONG SETUP = tranche 1 when the chart allows. \
BUILD SLOWLY = tranche only at a DMA touch. WATCHLIST = re-score after the next quarterly \
print, and say what would upgrade it. AVOID = say what specific evidence would justify a \
re-look. Never tell the reader to chase a stock that has just run.
- Write for someone who will risk real money on this. Be direct, be specific, and put the \
uncomfortable fact first if there is one."""


def _pillar_schema():
    return {
        "type": "object",
        "properties": {
            "score": {"type": "integer", "enum": [0, 1, 2]},
            "reason": {"type": "string",
                       "description": "One sentence. Why this score, citing the specific fact."},
            "evidence": {"type": "string",
                         "description": "The concrete datapoint or source behind it. "
                                        "'not verified' if none."},
        },
        "required": ["score", "reason", "evidence"],
        "additionalProperties": False,
    }


THESIS_SCHEMA = {
    "type": "object",
    "properties": {
        "pillars": {
            "type": "object",
            "properties": {p: _pillar_schema() for p in MODEL_PILLARS},
            "required": MODEL_PILLARS,
            "additionalProperties": False,
        },
        "facts": {"type": "array",
                  "items": {"type": "string"},
                  "description": "3-4 bullets: the facts that decide this case. Each "
                                 "must carry specific figures with periods, in the style "
                                 "'Q1 FY27 EBITDA margin +143bps YoY; mgmt guides 5% floor'."},
        "watch_next_quarter": {
            "type": "array", "items": {"type": "string"},
            "description": "2-3 specific, checkable things the NEXT quarterly print must "
                           "show for this thesis to hold — the reader re-scores on these."},
        "bear_case": {"type": "string",
                      "description": "The strongest genuine argument against buying."},
        "seasoned_read": {"type": "string",
                          "description": "One paragraph of judgment."},
        "action": {"type": "string",
                   "description": "What to do, tied to the 10/21-DMA tranches and the "
                                  "levels given. Name the rupee levels."},
        "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
        "unverified": {"type": "array", "items": {"type": "string"},
                       "description": "What could not be established, and what was looked for."},
        "sources": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"label": {"type": "string"}, "url": {"type": "string"}},
                "required": ["label", "url"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["pillars", "facts", "watch_next_quarter", "bear_case",
                 "seasoned_read", "action", "confidence", "unverified", "sources"],
    "additionalProperties": False,
}


def score(pack: dict, notes: str) -> dict:
    client = _client()
    te = pack["technical_score"]
    flags = (pack["fundamentals"].get("quality_flags") or [])
    flag_block = ("\n".join(f"  {f}" for f in flags) if flags
                  else "  (none — the computed cash-flow, receivable, debt, pledge and "
                       "base-effect checks all came back clean)")
    prompt = (
        f"Company: {pack['company']} ({pack['ticker']})   As of: {pack['as_of']}\n\n"
        f"=== QUALITY FLAGS — computed in code from the financials, already verified ===\n"
        f"{flag_block}\n"
        f"A 🚨 here is a REAL FLAG for the governance pillar and must be addressed\n"
        f"explicitly in your GV reason and in the bear case.\n\n"
        f"=== VERIFIED FUNDAMENTALS (parsed by our pipeline — already correct) ===\n"
        f"```json\n{json.dumps(pack['fundamentals'], indent=1, default=str)}\n```\n\n"
        f"=== VERIFIED TECHNICALS (our own trend engine) ===\n"
        f"```json\n{json.dumps(pack['technical'], indent=1, default=str)}\n```\n\n"
        f"=== TECHNICAL PILLAR — ALREADY SCORED, DO NOT CHANGE ===\n"
        f"TE = {te['score']}/2 — {te['reason']}\n\n"
        f"=== WEB RESEARCH NOTES ===\n{notes or '(research produced nothing)'}\n\n"
        f"{RUBRIC}\n\nScore the six pillars and write the thesis."
    )
    print(f"[thesis] scoring {pack['company']} (effort {SCORE_EFFORT})…")
    msg, tally = _final_message(
        client,
        model=MODEL,
        max_tokens=SCORE_MAX_TOKENS,
        system=SCORE_SYSTEM,
        output_config={"effort": SCORE_EFFORT,
                       "format": {"type": "json_schema", "schema": THESIS_SCHEMA}},
        messages=[{"role": "user", "content": prompt}],
    )
    _report_cost("scoring ", tally)
    out = json.loads(_text_of(msg))
    out["_usage"] = tally
    out["_cost"] = _cost(tally)
    return out


# ---------------------------------------------------------------------------
# 4. the verdict — arithmetic and the GV cap, in Python
# ---------------------------------------------------------------------------

def finalise(pack: dict, judged: dict) -> dict:
    """Total the pillars and pick the tier HERE.

    Deliberately not the model's job: a tier is a lookup and a sum, and a model
    that adds seven small integers wrong once puts a wrong verdict in front of a
    real trade. The GV=0 cap is enforced the same way — it is a rule, not a
    judgement call, so it does not get to be persuaded."""
    pillars = dict(judged["pillars"])
    pillars["TE"] = {"score": pack["technical_score"]["score"],
                     "reason": pack["technical_score"]["reason"],
                     "evidence": "computed from our own weekly flowchart + daily EMAs"}

    total = sum(int(pillars[p]["score"]) for p, _ in PILLARS)
    verdict = next(name for floor, name in TIERS if total >= floor)

    capped = False
    if int(pillars["GV"]["score"]) == 0 and verdict in ("STRONG SETUP", "BUILD SLOWLY"):
        verdict, capped = "WATCHLIST", True

    return {
        **judged,
        "pillars": pillars,
        "score": total,
        "max_score": 2 * len(PILLARS),
        "verdict": verdict,
        "gv_capped": capped,
        "company": pack["company"],
        "ticker": pack["ticker"],
        "as_of": pack["as_of"],
        "quality_flags": (pack["fundamentals"].get("quality_flags") or []),
        "levels": {k: pack["technical"].get(k)
                   for k in ("close", "dma5", "dma10", "dma21", "high_52w")},
        "support": pack["technical"].get("support") or {},
    }


# ---------------------------------------------------------------------------
# 5. rendering
# ---------------------------------------------------------------------------

def _rupees(v):
    return f"Rs {v:,.2f}" if isinstance(v, (int, float)) else "—"


def render_markdown(t: dict) -> str:
    L = t["levels"]
    strip = "  ".join(f"{k} {t['pillars'][k]['score']}" for k, _ in PILLARS)
    out = [
        f"# {t['company']} — {t['verdict']} ({t['score']}/{t['max_score']})",
        f"*{t['ticker']} · as of {t['as_of']} · confidence {t['confidence']}*",
        "",
        f"**Score strip:** {strip}",
    ]
    if t["gv_capped"]:
        out += ["", "> **Capped at WATCHLIST by the governance rule** — GV scored 0, so the "
                    "verdict cannot go higher regardless of the total."]
    out += ["", "## The facts that decide it"]
    out += [f"- {f}" for f in t["facts"]]
    out += ["", "## Pillars", "", "| | Pillar | Score | Why |", "|---|---|---|---|"]
    for k, label in PILLARS:
        p = t["pillars"][k]
        out.append(f"| {k} | {label} | {p['score']}/2 | {p['reason']} |")
    if t.get("watch_next_quarter"):
        out += ["", "## Watch in the next quarterly print"]
        out += [f"- {w}" for w in t["watch_next_quarter"]]
    if t.get("quality_flags"):
        out += ["", "## Computed quality checks",
                "*Calculated in code from the financials — not written by the model.*", ""]
        out += [f"- {f}" for f in t["quality_flags"]]
    out += [
        "", "## The bear case", t["bear_case"],
        "", "## Seasoned read", t["seasoned_read"],
        "", "## Action",
        f"{t['action']}",
        "",
        f"*Levels — CMP {_rupees(L.get('close'))} · 5-DMA {_rupees(L.get('dma5'))} · "
        f"10-DMA {_rupees(L.get('dma10'))} (tranche 1) · 21-DMA {_rupees(L.get('dma21'))} "
        f"(tranche 2) · 52w high {_rupees(L.get('high_52w'))}*",
    ]
    sup = t.get("support") or {}
    if sup:
        # Labelled AUTO on purpose: since 16-Aug-2026 the support levels that
        # actually fire alerts are the ones Lakshmi types on the watchlist. These
        # are the computed suggestion, and the note must not let them be mistaken
        # for his own levels.
        out.append(f"*Support (auto-derived, not his entered levels) — "
                   f"minor {_rupees(sup.get('minor'))} · major {_rupees(sup.get('major'))}*")
    if t.get("unverified"):
        out += ["", "## Could not verify"] + [f"- {u}" for u in t["unverified"]]
    if t.get("sources"):
        out += ["", "## Sources"] + [f"- [{s['label']}]({s['url']})" for s in t["sources"]]
    out += ["", "---", "*Scored from parsed exchange/Screener data + web research. "
                      "Financial figures and the technical score are computed by the "
                      "dashboard, not written by the model. Not investment advice.*"]
    return "\n".join(out)


def render_telegram(t: dict) -> str:
    """Short enough to read on a phone; the full note goes out as the document."""
    import html as _h
    e = _h.escape
    L = t["levels"]
    strip = " ".join(f"{k}{t['pillars'][k]['score']}" for k, _ in PILLARS)
    lines = [
        f"{VERDICT_ICON.get(t['verdict'], '📋')} <b>{e(t['company'])}</b> — "
        f"<b>{e(t['verdict'])}</b> {t['score']}/{t['max_score']}",
        f"<code>{e(strip)}</code> · confidence {e(t['confidence'])}",
    ]
    if t["gv_capped"]:
        lines.append("⚠️ <i>capped at WATCHLIST — governance scored 0</i>")
    lines.append("")
    for f in t["facts"][:4]:
        lines.append(f"• {e(f)}")
    lines += [
        "",
        f"<b>Bear:</b> {e(t['bear_case'][:400])}",
        "",
        f"<b>Action:</b> {e(t['action'][:400])}",
        "",
        f"CMP {_rupees(L.get('close'))} · 10DMA {_rupees(L.get('dma10'))} · "
        f"21DMA {_rupees(L.get('dma21'))}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 6. orchestration
# ---------------------------------------------------------------------------

def generate(stock_name: str, ticker: str = None, reason: str = "") -> dict:
    """Full pipeline. Returns the thesis dict, or {'status': 'INSUFFICIENT_DATA'}."""
    if not ticker:
        m = re.search(r"\((X(?:NSE|BOM)):([^)]+)\)", str(stock_name))
        if not m:
            return {"status": "INSUFFICIENT_DATA",
                    "why": f"no (XNSE:SYM)/(XBOM:CODE) ticker in '{stock_name}'"}
        ticker = f"{m.group(2).strip()}.NS" if m.group(1) == "XNSE" else f"{m.group(2).strip()}.BO"

    pack = build_pack(stock_name, ticker)
    quarters = (pack["fundamentals"].get("quarterly") or [])
    if not quarters:
        # Refuse rather than score blind. Zeroing RQ/MG/VA here would read as
        # "bad company" and drag a perfectly good one to AVOID — the exact
        # wrong-number-worse-than-blank failure (House Rule #2).
        return {"status": "INSUFFICIENT_DATA", "company": pack["company"], "ticker": ticker,
                "why": "no quarterly financials could be parsed for this ticker "
                       "(check the screener slug — see screener_data.SLUG_OVERRIDES). "
                       "Refusing to score rather than publish an unfounded verdict."}

    res = research(pack, reason)
    judged = score(pack, res.get("notes", ""))
    t = finalise(pack, judged)
    t["status"] = "OK"
    t["cost_usd"] = round(res.get("cost", 0.0) + judged.get("_cost", 0.0), 3)
    print(f"[thesis] TOTAL for {pack['company']}: ~${t['cost_usd']:.2f}")
    t["reason_given"] = reason
    t["markdown"] = render_markdown(t)
    return t


def send(t: dict) -> bool:
    """Push the thesis to Telegram: a short verdict message everyone reads on the
    phone, plus the full note as an attached .md they can open when they want it.

    Lakshmi asked for this as a WhatsApp PDF. We deliver on Telegram because that
    is the pipe this system already owns end-to-end — a WhatsApp send needs the
    Business API (a Meta business account, a verified number, template approval
    and per-message cost) for the same content. Flagged to Vishal rather than
    quietly substituted."""
    import notify
    ok = notify.send_telegram(render_telegram(t))
    fname = re.sub(r"[^A-Za-z0-9]+", "_", t["company"]).strip("_")
    ok_doc = notify.send_telegram_document(
        f"{fname}_thesis_{t['as_of']}.md", t["markdown"],
        caption=f"Full thesis — {t['company']} ({t['verdict']}, {t['score']}/{t['max_score']})")
    return ok and ok_doc


def store(client, t: dict, watchlist_id: int = None):
    """Persist the score onto the watchlist row (needs the new columns — see the
    schema note in CLAUDE.md). Never runs in a dry run."""
    if _dry():
        print("  [thesis] DRY-RUN — not storing")
        return
    payload = {
        "thesis_score": int(t["score"]),
        "thesis_verdict": t["verdict"],
        "thesis_pillars": json.dumps({k: int(v["score"]) for k, v in t["pillars"].items()}),
        "thesis_md": t["markdown"],
        "thesis_at": date.today().isoformat(),
    }
    q = client.table("watchlist").update(payload)
    q = q.eq("id", int(watchlist_id)) if watchlist_id else q.eq("stock_name", t.get("stock_name"))
    q.execute()


if __name__ == "__main__":
    # The model writes rupee signs; Windows' default cp1252 console raises
    # UnicodeEncodeError on the first one and kills the run AFTER the API has
    # been paid for. Same failure that swallowed the '->' log line in
    # signals._reconcile_last_week. Render is UTF-8, so this is local-only —
    # but a crash at the print step is still a lost thesis.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if "--dry" in sys.argv:
        os.environ["ALERTS_DRY_RUN"] = "1"
    why = ""
    if "--reason" in sys.argv:
        i = sys.argv.index("--reason")
        why = sys.argv[i + 1] if len(sys.argv) > i + 1 else ""
    if not args:
        print(__doc__)
        sys.exit(1)
    res = generate(args[0], reason=why)
    if res.get("status") != "OK":
        print(f"\nINSUFFICIENT DATA — {res.get('why')}")
        sys.exit(2)
    print("\n" + res["markdown"])
    if "--send" in sys.argv:
        print(f"\n[thesis] sending to Telegram… ok={send(res)}")
