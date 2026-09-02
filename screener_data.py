"""
screener_data.py — deterministic company fundamentals from screener.in.

WHY THIS EXISTS (16-Aug-2026)
`fundamentals.py` already proves screener.in answers us from a datacenter IP
(House Rule #1: own the data). But it only scrapes three top-of-page numbers.
The Stage-2 thesis scorecard needs the QUARTERLY TRACK RECORD — revenue, operating
profit, OPM%, PAT, EPS across the last ~8 quarters — plus ROCE/ROE and the promoter
shareholding trend.

The whole point is that these numbers are PARSED, not asked-for. The thesis engine
hands them to Claude as verified facts; Claude never re-derives them. That removes
the single highest-risk failure mode in an LLM-written investment note — a
confidently wrong number (House Rule #2) — and it means a thesis and a results
ALERT can never disagree about the same quarter.

WHAT MAKES THIS SAFE
  • screener tags every results column with data-date-key="YYYY-MM-DD", so a value
    can never drift onto the wrong period the way it did when a model transcribed
    a wide PDF (the Clean Max / Styrenix column bugs, 31-Jul & 04-Aug-2026).
  • The page's <h1> must match the company we asked for, else the whole fetch is
    REJECTED (same identity gate as fundamentals.fetch_one — screener slugs collide).
  • Every row is length-checked against the header count; a mismatched row is
    dropped rather than zipped into a plausible-looking lie.
  • Missing data returns None and says so. Nothing is defaulted to 0.
  • ALL arithmetic (QoQ, YoY, margin deltas) happens HERE in Python.

Usage:  python screener_data.py HFCL
        python screener_data.py 543378 "CWD Ltd"
"""
from __future__ import annotations

import html as _html
import re
import sys

import requests

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
}
BASE = "https://www.screener.in/company/{slug}/{view}"
TIMEOUT = 20

# Slugs where the trading symbol isn't the right URL. Kept in sync with
# fundamentals.SLUG_OVERRIDES (BSE-only names resolve by scrip code).
SLUG_OVERRIDES = {
    "CWD-MS.BO": "543378",
    "HSIL-MT.BO": "543916",
    "TRUECOLORS.BO": "544531",
    "LEHAR.BO": "532829",
    "SGRL.BO": "540737",
    # Technocraft Industries (India): screener has no TIIL slug, only the BSE
    # scrip code. Verified 16-Aug-2026 — /company/532804/ is "Technocraft
    # Industries (India) Ltd" and its page names NSE: TIIL. (Note NSE also lists
    # a separate TECHNOCRAF symbol, which is a DIFFERENT company — adding that
    # one would have quietly watched the wrong stock.)
    "TIIL.NS": "532804",
}

_STOPWORDS = {"LIMITED", "LTD", "INDIA", "INDIAN", "THE", "AND", "&", "COMPANY"}


def slug_for(ticker: str) -> str:
    """screener slug for a dashboard ticker ('HFCL.NS' -> 'HFCL')."""
    if ticker in SLUG_OVERRIDES:
        return SLUG_OVERRIDES[ticker]
    return re.sub(r"\.(NS|BO)$", "", str(ticker).strip().upper())


# ---------------------------------------------------------------------------
# low-level HTML helpers
# ---------------------------------------------------------------------------

def _strip(fragment: str) -> str:
    txt = _html.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))
    return re.sub(r"\s+", " ", txt.replace("\xa0", " ")).strip()


def _num(raw: str):
    """'1,234' -> 1234.0 | '18%' -> 18.0 | '' -> None | '-' -> None.
    Returns None (never 0.0) for anything unparseable — a blank cell must stay
    blank all the way through, because 0 would silently become a real datapoint
    in a growth calculation (House Rule #8: NaN-as-0 is how totals start lying)."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace("%", "").replace("\xa0", "")
    if s in ("", "-", "—", "–"):
        return None
    neg = s.startswith("-")
    s = s.lstrip("+-")
    if not re.fullmatch(r"\d*\.?\d+", s):
        return None
    v = float(s)
    return -v if neg else v


def _section(page: str, section_id: str) -> str:
    """The HTML of one screener <section>, up to the end of its first table."""
    i = page.find(f'id="{section_id}"')
    if i < 0:
        return ""
    j = page.find("</table>", i)
    return page[i:j] if j > 0 else page[i:i + 60000]


def _headers(segment: str) -> list:
    """Column labels, in page order, one per data column.

    Walk the <th> tags rather than harvesting data-date-key globally: the ANNUAL
    P&L table ends in a **TTM** column that carries NO date key, so a key-only
    scan returns one fewer label than there are cells and the length check below
    then (correctly) throws the whole table away. Prefer each th's ISO date key,
    fall back to its visible text ('TTM', 'Mar 2026'), and drop the empty
    top-left corner cell."""
    head = segment[:segment.find("</thead>")] if "</thead>" in segment else segment
    out = []
    for th in re.findall(r"<th[^>]*>.*?</th>", head, re.S):
        m = re.search(r'data-date-key="([0-9]{4}-[0-9]{2}-[0-9]{2})"', th)
        label = m.group(1) if m else _strip(th)
        if label:
            out.append(label)
    return out


def _rows(segment: str) -> dict:
    """{row label: [cell strings]} for one screener table body."""
    body = segment[segment.find("<tbody>"):] if "<tbody>" in segment else segment
    out = {}
    for tr in re.split(r"<tr[^>]*>", body)[1:]:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
        if len(cells) < 2:
            continue
        label = _strip(cells[0]).replace("+", "").strip()
        if not label:
            continue
        out[label] = [_strip(c) for c in cells[1:]]
    return out


def _series(rows: dict, cols: list, label: str) -> list:
    """One row aligned to the column headers, as numbers. LENGTH-CHECKED: if the
    row has a different cell count than there are columns, we DROP it rather than
    zip a value onto the wrong period — that misalignment is exactly the bug class
    that put last quarter's PBT under this quarter's heading (Clean Max, 31-Jul)."""
    vals = rows.get(label)
    if vals is None:
        return [None] * len(cols)
    if len(vals) != len(cols):
        print(f"  [screener] row '{label}' has {len(vals)} cells vs {len(cols)} "
              f"columns — dropped (won't guess the alignment)")
        return [None] * len(cols)
    return [_num(v) for v in vals]


# ---------------------------------------------------------------------------
# page fetch + identity gate
# ---------------------------------------------------------------------------

def _identity_ok(page: str, expected_name: str) -> bool:
    if not expected_name:
        return True
    m = re.search(r"<h1[^>]*>([^<]+)</h1>", page)
    page_name = _strip(m.group(1) if m else "").upper()
    want = {w for w in re.split(r"[^A-Z0-9]+", expected_name.upper())
            if len(w) >= 3 and w not in _STOPWORDS}
    # WHOLE-WORD match, never substring. A plain `w in page_name` accepted
    # "Innovana Thinklabs" as "Innova Captab" (INNOVA sits inside INNOVANA) —
    # caught 16-Aug-2026 while verifying new watchlist symbols. That is the same
    # failure that mis-attributed filings when 'EMS' matched inside 'R Systems',
    # and here it would silently store a DIFFERENT company's financials against
    # our stock, which is the exact thing this gate exists to prevent.
    page_words = set(re.split(r"[^A-Z0-9]+", page_name))
    if want and not (want & page_words):
        print(f"  [screener] identity MISMATCH: page is '{page_name.title()}', "
              f"expected ~'{expected_name}' — REJECTED (add a SLUG_OVERRIDES entry)")
        return False
    return True


# A quarterly reporter should have a column no more than ~2 quarters old; SME
# names reporting half-yearly, plus filing lag, can legitimately reach ~6 months.
# Beyond this the page is stale, not slow.
MAX_STALE_DAYS = 270


def _latest_quarter_date(page: str):
    """Newest quarterly column as a date, or None if the table is empty."""
    from datetime import date as _date
    best = None
    for key in _headers(_section(page, "quarters")):
        try:
            y, m, d = (int(x) for x in key.split("-"))
        except (ValueError, AttributeError):
            continue          # 'TTM' and friends
        dt = _date(y, m, d)
        if best is None or dt > best:
            best = dt
    return best


def fetch_page(slug: str, expected_name: str = "") -> tuple:
    """(html, basis) — the FRESHEST usable view. ('', '') on failure, always logged.

    Both views are evaluated and the one with the most recent quarterly column
    wins. Checking merely that the string 'id="quarters"' appears is NOT enough:
    GIPCL's consolidated page is a stale stub — it still carries every section id
    but its quarterly table has ZERO columns and its annuals stop at 2019 (the
    company presumably stopped reporting consolidated). That page returns HTTP
    200 and looked perfectly healthy to the old guard, so a thesis would have
    been scored on seven-year-old financials. A non-empty response is not
    automatically a usable one (House Rule #7)."""
    from datetime import date as _date
    candidates = []
    for view, basis in (("consolidated/", "consolidated"), ("", "standalone")):
        url = BASE.format(slug=slug, view=view)
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        except Exception as e:
            print(f"  [screener] {url} -> {type(e).__name__}: {e}")
            continue
        if r.status_code != 200:
            print(f"  [screener] {url} -> HTTP {r.status_code} ({len(r.content)} bytes)")
            continue
        if not _identity_ok(r.text, expected_name):
            return "", ""
        latest = _latest_quarter_date(r.text)
        if latest is None:
            print(f"  [screener] {basis}: quarterly table is EMPTY — unusable, "
                  f"trying the other view")
            continue
        candidates.append((latest, basis, r.text))
        # SHORT-CIRCUIT on a fresh first view. Fetching both views for every
        # ticker doubled our request rate and screener started answering 429 on
        # a 66-ticker run (01-Sep-2026). The staleness guard exists to catch a
        # DEAD view (GIPCL's consolidated stub ends in 2019) — once a view is
        # demonstrably current there is nothing for the second fetch to improve,
        # so stop. Politeness is part of owning this source.
        if (_date.today() - latest).days <= MAX_STALE_DAYS:
            return r.text, basis

    if not candidates:
        print(f"  [screener] no usable page for slug '{slug}' (no view had a "
              f"quarterly table)")
        return "", ""

    candidates.sort(key=lambda c: c[0], reverse=True)
    latest, basis, page = candidates[0]
    age = (_date.today() - latest).days
    if age > MAX_STALE_DAYS:
        print(f"  [screener] freshest data for '{slug}' is {basis} to {latest} "
              f"({age} days old) — STALE, refusing. Scoring a thesis on data this "
              f"old is worse than not scoring one.")
        return "", ""
    if len(candidates) > 1 and candidates[0][1] != "consolidated":
        print(f"  [screener] using {basis} (to {latest}) — it is fresher than the "
              f"consolidated view")
    return page, basis


# ---------------------------------------------------------------------------
# section parsers
# ---------------------------------------------------------------------------

# screener's quarterly row labels -> our keys. 'Operating Profit' is Sales minus
# Expenses, i.e. EBITDA before other income — the number Lakshmi actually reads,
# and (unlike a filing PDF's) it's already normalised across companies.
_Q_MAP = {
    "Sales": "sales", "Revenue": "sales", "Income": "sales",
    "Expenses": "expenses",
    "Operating Profit": "op_profit",
    "OPM %": "opm",
    "Other Income": "other_income",
    "Interest": "interest",
    "Depreciation": "depreciation",
    "Profit before tax": "pbt",
    "Tax %": "tax_pct",
    "Net Profit": "pat",
    "EPS in Rs": "eps",
}


def _periodic(segment: str, keep: int) -> list:
    """A screener results table -> [{period, sales, op_profit, opm, pat, ...}]
    oldest first, trimmed to the last `keep` columns."""
    cols = _headers(segment)
    if not cols:
        return []
    rows = _rows(segment)
    series = {}
    for label, key in _Q_MAP.items():
        if label in rows and key not in series:
            series[key] = _series(rows, cols, label)
    out = []
    for i, period in enumerate(cols):
        rec = {"period": period}
        for key, vals in series.items():
            rec[key] = vals[i]
        out.append(rec)
    return out[-keep:]


def quarterly(page: str, keep: int = 8) -> list:
    return _periodic(_section(page, "quarters"), keep)


def annual(page: str, keep: int = 6) -> list:
    return _periodic(_section(page, "profit-loss"), keep)


# Cash flow is the governance pillar's most important input and the one thing a
# dressed-up P&L cannot fake. Lakshmi's own batch report AVOIDed Susan
# Electricals almost entirely on this line: "operating cash flow was NEGATIVE all
# three years — profits never became cash; receivables ballooned Rs 13cr -> 46cr".
_CF_MAP = {
    "Cash from Operating Activity": "cfo",
    "Cash from Investing Activity": "cfi",
    "Cash from Financing Activity": "cff",
    "Net Cash Flow": "net_cash",
    "Free Cash Flow": "fcf",
    "CFO/OP": "cfo_over_op_pct",
}

_BS_MAP = {
    "Equity Capital": "equity_capital", "Reserves": "reserves",
    "Borrowings": "borrowings", "Other Liabilities": "other_liabilities",
    "Total Liabilities": "total_liabilities", "Fixed Assets": "fixed_assets",
    "CWIP": "cwip", "Investments": "investments", "Other Assets": "other_assets",
}

_RATIO_MAP = {
    "Debtor Days": "debtor_days", "Inventory Days": "inventory_days",
    "Days Payable": "days_payable", "Cash Conversion Cycle": "cash_conversion_cycle",
    "Working Capital Days": "working_capital_days", "ROCE %": "roce_pct",
}


def _mapped(segment: str, mapping: dict, keep: int) -> list:
    cols = _headers(segment)
    if not cols:
        return []
    rows = _rows(segment)
    series = {key: _series(rows, cols, label)
              for label, key in mapping.items() if label in rows}
    out = []
    for i, period in enumerate(cols):
        rec = {"period": period}
        for key, vals in series.items():
            rec[key] = vals[i]
        out.append(rec)
    return out[-keep:]


def cash_flow(page: str, keep: int = 5) -> list:
    return _mapped(_section(page, "cash-flow"), _CF_MAP, keep)


def balance_sheet(page: str, keep: int = 5) -> list:
    return _mapped(_section(page, "balance-sheet"), _BS_MAP, keep)


def efficiency(page: str, keep: int = 5) -> list:
    """Working-capital quality: debtor/inventory days and the cash conversion
    cycle. Receivables ballooning faster than sales is how 'growth' that never
    becomes cash shows up BEFORE the auditor says anything."""
    return _mapped(_section(page, "ratios"), _RATIO_MAP, keep)


_TOP_LABELS = {
    "Market Cap": "market_cap_cr", "Current Price": "cmp", "Stock P/E": "pe",
    "Book Value": "book_value", "Dividend Yield": "dividend_yield",
    "ROCE": "roce", "ROE": "roe", "Face Value": "face_value",
}


def top_ratios(page: str) -> dict:
    """The header ratio strip. Same tempered regex fundamentals.py uses: the
    number must appear before the next </li>, or a company with a BLANK ratio
    silently inherits the NEXT ratio's value (caught live on Cockerill's P/E,
    15-Jul-2026)."""
    out = {}
    for label, key in _TOP_LABELS.items():
        pat = (rf'"name"[^>]*>\s*{re.escape(label)}\s*</span>'
               rf'(?:(?!</li>).)*?"number"[^>]*>\s*([\d,\.]+)')
        m = re.search(pat, page, re.DOTALL)
        out[key] = _num(m.group(1)) if m else None
    m = re.search(r'"Broad Sector"[^>]*>([^<]+)<', page)
    out["sector"] = _strip(m.group(1)) if m else None
    m = re.search(r'"Industry"[^>]*>([^<]+)<', page)
    out["industry"] = _strip(m.group(1)) if m else None
    return out


def shareholding(page: str, keep: int = 6) -> dict:
    """Promoter / FII / DII trend and pledge.

    PLEDGE: screener prints a 'Pledged percentage' row ONLY when it is non-zero.
    Absent therefore means 'not disclosed here', NOT 'zero' — we return None and
    let the research step confirm. Reporting an unverified 0% on a governance
    pillar would be precisely the wrong-number-beats-blank failure (House Rule #2)."""
    seg = _section(page, "shareholding")
    if not seg:
        return {}
    cols = _headers(seg)
    rows = _rows(seg)
    if not cols:
        return {}

    def pick(*labels):
        for lab in labels:
            if lab in rows:
                return _series(rows, cols, lab)[-keep:]
        return None

    pledge = pick("Pledged percentage", "Pledged Percentage", "Pledged")
    return {
        "periods": cols[-keep:],
        "promoters": pick("Promoters"),
        "fiis": pick("FIIs"),
        "diis": pick("DIIs"),
        "public": pick("Public"),
        "pledge_pct": pledge,
        "pledge_disclosed": pledge is not None,
    }


def growth_table(page: str) -> dict:
    """screener's compounded-growth strips (Sales/Profit CAGR, Stock CAGR, ROE)
    that sit under the P&L as small 'ranges-table' blocks."""
    out = {}
    for m in re.finditer(r"<table class=\"ranges-table\">(.*?)</table>", page, re.S):
        block = m.group(1)
        title = _strip(re.search(r"<th[^>]*>(.*?)</th>", block, re.S).group(1)) if "<th" in block else ""
        if not title:
            continue
        entries = {}
        for tr in re.split(r"<tr[^>]*>", block)[1:]:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
            if len(cells) >= 2:
                k = _strip(cells[0]).rstrip(":")
                v = _num(_strip(cells[1]))
                if k and v is not None:
                    entries[k] = v
        if entries:
            out[title] = entries
    return out


# ---------------------------------------------------------------------------
# derived metrics — every % computed HERE, never by a model
# ---------------------------------------------------------------------------

def _pct(new, old):
    """Growth % — None unless BOTH sides exist and the base is meaningfully
    positive. A swing off a near-zero or negative base is not a percentage
    anyone can act on, so we return None instead of a giant meaningless number."""
    if new is None or old is None or old <= 0:
        return None
    return round((new / old - 1) * 100, 1)


def derive(quarters: list) -> dict:
    """QoQ / YoY for the latest quarter, plus the margin trend.

    YoY needs the quarter 4 columns back — screener's columns are consecutive
    calendar quarters, and we verify that by DATE rather than assuming position,
    so a company with a missing/irregular column can't silently compare the wrong
    two periods (the mistake that made a scaling quarter look like a collapse)."""
    if len(quarters) < 2:
        return {}
    cur = quarters[-1]
    prev = quarters[-2]
    yoy = None
    if len(quarters) >= 5:
        cand = quarters[-5]
        if _months_apart(cand.get("period"), cur.get("period")) == 12:
            yoy = cand
    if yoy is None:
        for q in quarters[:-1]:
            if _months_apart(q.get("period"), cur.get("period")) == 12:
                yoy = q
                break

    d = {
        "current_period": cur.get("period"),
        "prev_period": prev.get("period"),
        "yoy_period": yoy.get("period") if yoy else None,
        "sales_qoq_pct": _pct(cur.get("sales"), prev.get("sales")),
        "op_profit_qoq_pct": _pct(cur.get("op_profit"), prev.get("op_profit")),
        "pat_qoq_pct": _pct(cur.get("pat"), prev.get("pat")),
        "opm_now": cur.get("opm"),
        "opm_prev_q": prev.get("opm"),
        "opm_year_ago": yoy.get("opm") if yoy else None,
    }
    if yoy:
        d["sales_yoy_pct"] = _pct(cur.get("sales"), yoy.get("sales"))
        d["op_profit_yoy_pct"] = _pct(cur.get("op_profit"), yoy.get("op_profit"))
        d["pat_yoy_pct"] = _pct(cur.get("pat"), yoy.get("pat"))
    else:
        d["sales_yoy_pct"] = d["op_profit_yoy_pct"] = d["pat_yoy_pct"] = None

    if d["opm_now"] is not None and d["opm_year_ago"] is not None:
        d["opm_delta_yoy_bps"] = round((d["opm_now"] - d["opm_year_ago"]) * 100)
    if d["opm_now"] is not None and d["opm_prev_q"] is not None:
        d["opm_delta_qoq_bps"] = round((d["opm_now"] - d["opm_prev_q"]) * 100)

    # A null % must never be read as "flat". _pct returns None whenever the base
    # is <=0, which most often means the year-ago period was a LOSS — i.e. the
    # single most bullish case there is. Say so explicitly in words, with both
    # rupee figures, so nothing downstream can score a turnaround as no-growth.
    d["notes"] = []
    for key, label in (("pat", "PAT"), ("op_profit", "Operating profit"), ("sales", "Revenue")):
        new, old = cur.get(key), (yoy or {}).get(key)
        if new is None or old is None or old > 0:
            continue
        direction = "TURNAROUND to a profit" if new > 0 else "still loss-making"
        d["notes"].append(
            f"{label}: year-ago ({d['yoy_period']}) was Rs {old:,.2f} cr, so a YoY % "
            f"is meaningless — this is a {direction} at Rs {new:,.2f} cr.")

    # How many of the last 4 quarters grew revenue sequentially — a crude but
    # honest consistency read the model doesn't have to eyeball off a table.
    ups = 0
    seq = [q.get("sales") for q in quarters[-5:]]
    for a, b in zip(seq, seq[1:]):
        if a is not None and b is not None and b > a:
            ups += 1
    d["sequential_sales_ups_last4"] = ups
    return d


def _months_apart(a: str, b: str):
    """Whole months between two 'YYYY-MM-DD' column keys, or None."""
    try:
        ay, am = int(a[:4]), int(a[5:7])
        by, bm = int(b[:4]), int(b[5:7])
    except (TypeError, ValueError, IndexError):
        return None
    return (by - ay) * 12 + (bm - am)


def quality_flags(snap: dict) -> list:
    """Hard, checkable quality/governance flags — computed HERE, not spotted by a
    model. Each is a sentence with the numbers already in it.

    These are the checks that separated AVOID from BUILD SLOWLY in Lakshmi's own
    batch report (negative CFO, receivables funding 'growth', debt build). Asking
    a model to notice them in a JSON blob is strictly worse than handing them
    over already computed: a missed red flag is the most expensive failure this
    scorecard can have."""
    flags = []
    cf = snap.get("cash_flow") or []
    ann = snap.get("annual") or []

    # 1. Profit that never becomes cash — the single most important smallcap check.
    yrs = [c for c in cf if c.get("period") != "TTM" and c.get("cfo") is not None]
    neg = [c for c in yrs if c["cfo"] < 0]
    if yrs and len(neg) >= 2:
        detail = ", ".join(f"{c['period'][:4]}: Rs {c['cfo']:,.0f} cr" for c in yrs[-4:])
        flags.append(
            f"🚨 OPERATING CASH FLOW NEGATIVE in {len(neg)} of the last {len(yrs)} "
            f"years ({detail}). Reported profit is not converting to cash.")
    elif yrs and yrs[-1]["cfo"] < 0:
        flags.append(
            f"⚠️ Operating cash flow was NEGATIVE in the latest year "
            f"({yrs[-1]['period'][:4]}: Rs {yrs[-1]['cfo']:,.0f} cr) despite the "
            f"reported P&L. Check working capital before trusting the growth.")

    # 2. Cumulative CFO vs cumulative PAT over the same years.
    pat_by_yr = {a["period"]: a.get("pat") for a in ann if a.get("period") != "TTM"}
    pair = [(c["cfo"], pat_by_yr.get(c["period"])) for c in yrs
            if pat_by_yr.get(c["period"]) is not None]
    if len(pair) >= 3:
        tot_cfo = sum(p[0] for p in pair)
        tot_pat = sum(p[1] for p in pair)
        if tot_pat > 0:
            ratio = tot_cfo / tot_pat
            if ratio < 0.5:
                flags.append(
                    f"🚨 Over the last {len(pair)} years cumulative operating cash flow "
                    f"is Rs {tot_cfo:,.0f} cr against cumulative PAT of Rs {tot_pat:,.0f} cr "
                    f"({ratio*100:.0f}% conversion). Profits are largely on paper.")
            elif ratio < 0.8:
                flags.append(
                    f"⚠️ Cash conversion is weak: {ratio*100:.0f}% of the last "
                    f"{len(pair)} years' PAT arrived as operating cash flow.")

    # 3. Receivables stretching — how uncollected 'growth' shows up early.
    eff = [e for e in (snap.get("efficiency") or []) if e.get("debtor_days") is not None]
    if len(eff) >= 2:
        first, last = eff[0], eff[-1]
        if last["debtor_days"] > first["debtor_days"] * 1.5 and last["debtor_days"] > 60:
            flags.append(
                f"⚠️ Debtor days rose {first['debtor_days']:.0f} -> {last['debtor_days']:.0f} "
                f"({first['period'][:4]} to {last['period'][:4]}) — receivables are "
                f"growing faster than sales.")

    # 4. Debt build.
    bs = [b for b in (snap.get("balance_sheet") or [])
          if b.get("period") != "TTM" and b.get("borrowings") is not None]
    if len(bs) >= 2 and bs[0]["borrowings"] > 0:
        g = bs[-1]["borrowings"] / bs[0]["borrowings"]
        if g >= 2.0:
            flags.append(
                f"⚠️ Borrowings rose Rs {bs[0]['borrowings']:,.0f} cr -> "
                f"Rs {bs[-1]['borrowings']:,.0f} cr ({g:.1f}x) over "
                f"{bs[0]['period'][:4]}-{bs[-1]['period'][:4]}.")

    # 5. Promoter selling down / pledge.
    shp = snap.get("shareholding") or {}
    pr = [p for p in (shp.get("promoters") or []) if p is not None]
    if len(pr) >= 2 and pr[0] - pr[-1] >= 2.0:
        flags.append(
            f"⚠️ Promoter holding fell {pr[0]:.2f}% -> {pr[-1]:.2f}% over the last "
            f"{len(pr)} disclosed quarters.")
    pledge = [p for p in (shp.get("pledge_pct") or []) if p]
    if pledge:
        flags.append(f"🚨 PLEDGED promoter shares disclosed — latest {pledge[-1]:.2f}%.")
    elif shp:
        flags.append("ℹ️ No pledge row on Screener (it prints one only when pledging is "
                     "non-zero) — treat as 'not disclosed here', not as a verified zero.")

    # 6. Base effect — a tiny year-ago quarter flattering the YoY %.
    d = snap.get("derived") or {}
    qs = snap.get("quarterly") or []
    if d.get("yoy_period") and qs:
        cur = qs[-1].get("sales")
        ago = next((q.get("sales") for q in qs if q.get("period") == d["yoy_period"]), None)
        if cur and ago and ago > 0 and cur / ago >= 2.0:
            flags.append(
                f"⚠️ BASE EFFECT: the year-ago quarter ({d['yoy_period']}) had revenue of "
                f"only Rs {ago:,.0f} cr vs Rs {cur:,.0f} cr now — the YoY % is flattered "
                f"by a small base, so judge durability on the sequential trend.")
    return flags


def ttm_metrics(snap: dict) -> dict:
    """Trailing-twelve-month revenue, EBITDA and operating margin.

    Prefers screener's own **TTM** column on the annual P&L — it is already the
    right 12 months and, crucially, it is correct for HALF-YEARLY reporters (most
    BSE SME names) where naively summing "the last 4 quarters" would actually sum
    two years. Falls back to summing the trailing quarters only when there is no
    TTM column, and only when those quarters really do span ~12 months.

    'Operating Profit' on screener is Sales minus Expenses — EBITDA before other
    income, which is the number Lakshmi reads and the one the results alerts
    already use. Returns {} rather than a guess when it cannot be established."""
    ann = snap.get("annual") or []
    ttm = next((a for a in ann if str(a.get("period")).strip().upper() == "TTM"), None)
    if ttm and ttm.get("op_profit") is not None:
        rev, eb = ttm.get("sales"), ttm.get("op_profit")
        return {"revenue_ttm_cr": rev, "ebitda_ttm_cr": eb,
                "opm_ttm_pct": (round(eb / rev * 100, 2) if rev else ttm.get("opm")),
                "ttm_basis": "screener TTM column"}

    qs = [q for q in (snap.get("quarterly") or [])
          if q.get("sales") is not None and q.get("op_profit") is not None]
    if len(qs) < 2:
        return {}
    # How many periods make a year here? Quarterly -> 4, half-yearly -> 2.
    gap = _months_apart(qs[-2].get("period"), qs[-1].get("period"))
    n = 4 if gap == 3 else (2 if gap == 6 else None)
    if not n or len(qs) < n:
        return {}
    window = qs[-n:]
    span = _months_apart(window[0].get("period"), window[-1].get("period"))
    if span != 12 - gap:          # the window must really cover ~12 months
        return {}
    rev = sum(q["sales"] for q in window)
    eb = sum(q["op_profit"] for q in window)
    return {"revenue_ttm_cr": round(rev, 2), "ebitda_ttm_cr": round(eb, 2),
            "opm_ttm_pct": (round(eb / rev * 100, 2) if rev else None),
            "ttm_basis": f"sum of last {n} periods"}


def snapshot(ticker: str, expected_name: str = "") -> dict:
    """Everything we can prove about one company from screener, in one call.
    {} when the page is unusable — callers must treat that as 'unknown', never
    as 'nothing to report'."""
    slug = slug_for(ticker)
    page, basis = fetch_page(slug, expected_name)
    if not page:
        return {}
    qs = quarterly(page)
    snap = {
        "source": f"screener.in/company/{slug}",
        "basis": basis,
        "ratios": top_ratios(page),
        "quarterly": qs,
        "annual": annual(page),
        "cash_flow": cash_flow(page),
        "balance_sheet": balance_sheet(page),
        "efficiency": efficiency(page),
        "shareholding": shareholding(page),
        "growth": growth_table(page),
        "derived": derive(qs),
    }
    r = snap["ratios"]
    if r.get("cmp") and r.get("book_value"):
        r["pb"] = round(r["cmp"] / r["book_value"], 2)
    snap["quality_flags"] = quality_flags(snap)
    return snap


if __name__ == "__main__":
    tk = sys.argv[1] if len(sys.argv) > 1 else "HFCL"
    nm = sys.argv[2] if len(sys.argv) > 2 else ""
    import json
    s = snapshot(tk, nm)
    if not s:
        print("no data")
        sys.exit(1)
    print(json.dumps(s, indent=2, default=str))
