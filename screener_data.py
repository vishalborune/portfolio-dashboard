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
    if want and not any(w in page_name for w in want):
        print(f"  [screener] identity MISMATCH: page is '{page_name.title()}', "
              f"expected ~'{expected_name}' — REJECTED (add a SLUG_OVERRIDES entry)")
        return False
    return True


def fetch_page(slug: str, expected_name: str = "") -> tuple:
    """(html, basis) where basis is 'consolidated' or 'standalone'. ('', '') on
    failure — and the reason is always logged (House Rule #3)."""
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
        if 'id="quarters"' not in r.text:
            print(f"  [screener] {url} -> 200 but no quarterly table "
                  f"({len(r.text)} bytes) — trying the other view")
            continue
        return r.text, basis
    print(f"  [screener] no usable page for slug '{slug}'")
    return "", ""


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
        "shareholding": shareholding(page),
        "growth": growth_table(page),
        "derived": derive(qs),
    }
    r = snap["ratios"]
    if r.get("cmp") and r.get("book_value"):
        r["pb"] = round(r["cmp"] / r["book_value"], 2)
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
