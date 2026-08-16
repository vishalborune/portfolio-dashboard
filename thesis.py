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


def _final_message(client, **kw):
    """One streamed request, resuming across `pause_turn`.

    Server-side web search runs its own sampling loop and stops with
    `pause_turn` when it hits the per-turn iteration cap; the turn is NOT
    finished. Without this loop a long research run silently returns a partial
    answer and looks like a complete one (House Rule #7 — a non-empty response
    is not automatically a usable one). Streaming because Opus 5 thinks by
    default, which makes these calls slow enough to trip HTTP timeouts."""
    messages = list(kw.pop("messages"))
    for attempt in range(6):
        with client.messages.stream(messages=messages, **kw) as stream:
            msg = stream.get_final_message()
        if msg.stop_reason != "pause_turn":
            if msg.stop_reason == "refusal":
                raise RuntimeError("the model declined this request (stop_reason=refusal)")
            return msg
        # Resume: hand the paused turn straight back, no extra user message.
        messages = messages + [{"role": "assistant", "content": msg.content}]
        print(f"  [thesis] server tools paused mid-turn — resuming ({attempt + 1})")
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
                print(f"  [thesis] reusing today's cached research ({len(notes)} chars)")
                return {"notes": notes, "searches": 0, "cached": True}
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
    print(f"[thesis] researching {pack['company']} (web search)…")
    msg = _final_message(
        client,
        model=MODEL,
        max_tokens=12000,
        system=RESEARCH_SYSTEM,
        tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": 12}],
        output_config={"effort": "high"},
        messages=[{"role": "user", "content": prompt}],
    )
    searches = sum(1 for b in msg.content if getattr(b, "type", "") == "server_tool_use")
    notes = _text_of(msg)
    print(f"  [thesis] research done — {searches} tool calls, {len(notes)} chars")
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(notes)
    except Exception as e:
        print(f"  [thesis] could not cache research ({type(e).__name__}: {e}) — "
              f"a re-run today will pay for it again")
    return {"notes": notes, "searches": searches, "cached": False}


# ---------------------------------------------------------------------------
# 3. scoring — model judges, Python arithmetics
# ---------------------------------------------------------------------------

RUBRIC = """Score each pillar 0, 1 or 2. Be strict: 2 means the evidence is strong AND \
sourced, 1 means mixed or partly sourced, 0 means weak, absent or unverifiable. \
"I could not find out" is a 0 or 1, never a 2 — an unverified positive is not a positive.

RQ — Results Quality
  2 = latest quarter shows revenue AND operating profit growth YoY, with no one-off flattering it
  1 = growth in one of the two, or growth that leans on an easy base or a one-off
  0 = flat/declining, or the quarter is distorted by an exceptional item

DU — Durability
  2 = a specific, sourced reason the growth continues (order book, guidance, dated capacity)
  1 = plausible but thinly sourced, or a partly-consumed runway
  0 = no visible driver beyond the last quarter, or growth is flattered by a weak base

MG — Margin Direction
  2 = operating margin expanding YoY AND sequentially
  1 = broadly stable, or expanding on one axis and slipping on the other
  0 = compressing on both, with operating profit in RUPEES flat or falling
  NOTE: a falling margin PERCENTAGE while operating profit in RUPEES grows strongly is
  scaling dilution, NOT margin pressure. Score that 1, not 0. Read the bps deltas and
  the rupee growth together before you judge this pillar.

VA — Valuation
  2 = re-rating room: multiple undemanding versus the growth and the sector
  1 = fair — priced roughly for the growth being delivered
  0 = the multiple already assumes the growth continues; price has run ahead of earnings

SM — Smart Money
  2 = sourced informed buying (promoter open-market purchase, preferential issue, buyback, new institutional entry)
  1 = stable promoter holding with some institutional interest, nothing decisive
  0 = promoter selling, dilution at a discount, or nothing verifiable either way

GV — Governance
  2 = clean: no pledge, no auditor issue, cash flow tracks profit, no adverse related-party findings
  1 = minor or unresolved flags worth watching
  0 = a serious red flag — auditor resignation or qualification, material pledging,
      profit not backed by operating cash flow, or adverse related-party transactions

HARD RULE: if GV is 0, the verdict is capped at WATCHLIST no matter how high the total.
The caller enforces this in code, so score GV on its own merits and do not soften it to
protect the total."""

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
what this really is, what would change your mind, and what most people will get wrong.
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
                  "description": "3-4 bullets: the facts that decide this case."},
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
    "required": ["pillars", "facts", "bear_case", "seasoned_read", "action",
                 "confidence", "unverified", "sources"],
    "additionalProperties": False,
}


def score(pack: dict, notes: str) -> dict:
    client = _client()
    te = pack["technical_score"]
    prompt = (
        f"Company: {pack['company']} ({pack['ticker']})   As of: {pack['as_of']}\n\n"
        f"=== VERIFIED FUNDAMENTALS (parsed by our pipeline — already correct) ===\n"
        f"```json\n{json.dumps(pack['fundamentals'], indent=1, default=str)}\n```\n\n"
        f"=== VERIFIED TECHNICALS (our own trend engine) ===\n"
        f"```json\n{json.dumps(pack['technical'], indent=1, default=str)}\n```\n\n"
        f"=== TECHNICAL PILLAR — ALREADY SCORED, DO NOT CHANGE ===\n"
        f"TE = {te['score']}/2 — {te['reason']}\n\n"
        f"=== WEB RESEARCH NOTES ===\n{notes or '(research produced nothing)'}\n\n"
        f"{RUBRIC}\n\nScore the six pillars and write the thesis."
    )
    print(f"[thesis] scoring {pack['company']}…")
    msg = _final_message(
        client,
        model=MODEL,
        max_tokens=10000,
        system=SCORE_SYSTEM,
        output_config={"effort": "high",
                       "format": {"type": "json_schema", "schema": THESIS_SCHEMA}},
        messages=[{"role": "user", "content": prompt}],
    )
    return json.loads(_text_of(msg))


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
        out.append(f"*Support — minor {_rupees(sup.get('minor'))} · "
                   f"major {_rupees(sup.get('major'))}*")
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

    notes = research(pack, reason).get("notes", "")
    judged = score(pack, notes)
    t = finalise(pack, judged)
    t["status"] = "OK"
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
