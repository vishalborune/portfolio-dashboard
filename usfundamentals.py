"""
usfundamentals.py — US fundamentals from SEC XBRL "companyfacts" (official, free).

The US twin of screener_data.py / fundamentals.py, with the same design rule: NO
model touches a number. https://data.sec.gov/api/xbrl/companyfacts/CIK##.json
returns every tagged line item a company has ever filed; Python picks the
quarterly values, derives Q4 (= fiscal year − Q1..Q3, since 10-Ks report only
the year), computes QoQ/YoY, margins, TTM, P/E, market cap, P/B, ROE, ROCE, and
stores them into `fundamentals_daily` — the SAME table the dashboard already
reads — so the US book's Market Cap / P/E / P/B / Revenue / EBITDA / OPM / ROCE /
ROE columns light up with no app changes.

UNITS: the table's *_cr columns hold RUPEE CRORE for India. For US names they
hold US$ BILLIONS (market_cap_cr, revenue_ttm_cr, ebitda_ttm_cr); book_value is
$ per share. The dashboard captions this on the US book. A separate table would
have been cleaner; reusing the columns means one read path and zero UI drift.

GOTCHAS (probed 26-Sep-2026):
  • Concept names vary AND go stale: MSFT's `Revenues` ends in 2010 (it now files
    RevenueFromContractWithCustomerExcludingAssessedTax). So a concept is chosen
    by the RECENCY of its data, never by list order.
  • Entries without a `frame` are year-to-date cumulatives — quarterly values are
    the 80–100-day-duration entries; annual are 350–380 days.
  • Fiscal years differ (NVDA ends Jan, MSFT June): everything keys off END DATES,
    never calendar quarters or the `fy`/`fp` labels.
  • EBITDA = operating income + D&A; when no D&A concept is filed, EBITDA is left
    BLANK (a blank beats a wrong number — House Rule #2).
  • Identity check: the entityName returned must resemble the SEC title for that
    CIK, else the snapshot is refused (fundamentals.py's page-title lesson).

CLI:  python usfundamentals.py NVDA        print the quarterly table + ratios
      python usfundamentals.py update      store every US name into fundamentals_daily
"""
from __future__ import annotations
import json, os, re, sys, time
from datetime import date, datetime, timedelta, timezone

import requests

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

SEC_UA = os.environ.get("SEC_USER_AGENT", "PortfolioDashboard research@portfolio-dashboard.app")
H = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}
CACHE_DIR = ".cache"
CACHE_TTL = 20 * 3600
B = 1e9

CONCEPTS = {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
                "RevenueFromContractWithCustomerIncludingAssessedTax"],
    "op_income": ["OperatingIncomeLoss"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "eps": ["EarningsPerShareDiluted", "EarningsPerShareBasic"],
    "gross_profit": ["GrossProfit"],
    "da": ["DepreciationDepletionAndAmortization", "DepreciationAndAmortization",
           "DepreciationAmortizationAndAccretionNet", "DepreciationAmortizationAndOther"],
    "equity": ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
    "cash": ["CashAndCashEquivalentsAtCarryingValue"],
    "lt_debt": ["LongTermDebtNoncurrent", "LongTermDebt"],
}
FLOW = {"revenue", "op_income", "net_income", "eps", "gross_profit", "da"}   # duration items
UNITS = {"eps": "USD/shares"}


def _get(url, timeout=60):
    time.sleep(0.15)
    r = requests.get(url, headers=H, timeout=timeout)
    if r.status_code != 200:
        print(f"  [usfund] HTTP {r.status_code} {len(r.content)}B for {url[:80]}")
    return r


def companyfacts(cik: int) -> dict:
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"cf_{cik}.json")
    try:
        if os.path.exists(path) and time.time() - os.path.getmtime(path) < CACHE_TTL:
            return json.load(open(path, encoding="utf-8"))
    except Exception:
        pass
    r = _get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json")
    if r.status_code != 200:
        return {}
    j = r.json()
    try:
        json.dump(j, open(path, "w", encoding="utf-8"))
    except Exception:
        pass
    return j


def _dur(e) -> int:
    try:
        return (date.fromisoformat(e["end"]) - date.fromisoformat(e["start"])).days
    except Exception:
        return -1


def _pick(gaap: dict, names: list[str], unit: str = "USD"):
    """The candidate concept whose data is most RECENT (not the first present)."""
    best, best_end = None, ""
    for n in names:
        ents = (gaap.get(n) or {}).get("units", {}).get(unit) or []
        ents = [e for e in ents if e.get("form") in ("10-Q", "10-K", "10-Q/A", "10-K/A", "20-F", "40-F")]
        if not ents:
            continue
        end = max(e["end"] for e in ents)
        if end > best_end:
            best, best_end = ents, end
    return best or []


def quarterly(ents: list) -> dict:
    """{end_date: value} for QUARTERS. Reported 80-100-day entries first; then Q4s
    derived from annual (350-380-day) entries minus the three quarters inside
    that fiscal year. Later filings win (restatements)."""
    q, a, cum = {}, {}, {}
    for e in sorted(ents, key=lambda x: x.get("filed", "")):
        d = _dur(e)
        if 80 <= d <= 100:
            q[e["end"]] = float(e["val"])
        elif 350 <= d <= 380:
            a[e["end"]] = (e["start"], float(e["val"]))
        if d > 0 and e.get("start"):
            cum.setdefault(e["start"], {})[e["end"]] = float(e["val"])
    # Cash-flow items (D&A, capex) are filed YEAR-TO-DATE only — 3/6/9/12 months
    # from the same fiscal start — so Q2..Q4 exist only as differences between
    # consecutive cumulatives. Reported standalone quarters always win.
    for start, ends in cum.items():
        prev_end, prev_val = None, 0.0
        for end in sorted(ends):
            val = ends[end]
            step = (date.fromisoformat(end) - date.fromisoformat(prev_end)).days if prev_end else _dur({"start": start, "end": end})
            if 80 <= step <= 100 and end not in q:
                q[end] = val - prev_val
            prev_end, prev_val = end, val
    for end, (start, val) in a.items():
        if end in q:
            continue
        inside = [v for k, v in q.items() if start < k < end]
        if len(inside) == 3:
            q[end] = val - sum(inside)
    return dict(sorted(q.items()))


def instant(ents: list) -> tuple[str, float] | None:
    """Latest balance-sheet (instant) value: (date, value)."""
    best = None
    for e in ents:
        if "start" in e and e.get("start"):
            continue
        if best is None or (e["end"], e.get("filed", "")) > (best[0], best[2]):
            best = (e["end"], float(e["val"]), e.get("filed", ""))
    return (best[0], best[1]) if best else None


def _pct(a, b):
    if a is None or b in (None, 0):
        return None
    return (a / b - 1) * 100 if b > 0 else None


def snapshot(sym: str, cik: int, price: float | None, expect_title: str | None = None) -> dict:
    j = companyfacts(cik)
    if not j:
        return {"symbol": sym, "error": "companyfacts unavailable"}
    ent = str(j.get("entityName") or "")
    if expect_title:
        a = re.sub(r"[^a-z0-9]", "", ent.lower())[:6]
        b = re.sub(r"[^a-z0-9]", "", expect_title.lower())[:6]
        if a and b and a != b:
            return {"symbol": sym, "error": f"identity mismatch: SEC entity '{ent}' vs expected '{expect_title}'"}
    gaap = j.get("facts", {}).get("us-gaap", {})
    dei = j.get("facts", {}).get("dei", {})
    series = {}
    for key in FLOW:
        series[key] = quarterly(_pick(gaap, CONCEPTS[key], UNITS.get(key, "USD")))
    rev = series["revenue"]
    if len(rev) < 2:
        return {"symbol": sym, "entity": ent, "error": "no quarterly revenue in companyfacts"}
    # D&A: MSFT / GOOGL / AVGO file NO combined D&A tag (AVGO's last one is from
    # 2018) — they tag Depreciation and AmortizationOfIntangibleAssets separately.
    # A combined tag older than the revenue series by >200 days is stale; rebuild
    # from the parts. Depreciation alone (when amortisation is missing for a
    # quarter) UNDERSTATES EBITDA — the conservative direction.
    newest_rev = list(rev)[-1]
    da_ok = series["da"] and (date.fromisoformat(newest_rev) - date.fromisoformat(list(series["da"])[-1])).days <= 200
    if not da_ok:
        dep = quarterly(_pick(gaap, ["Depreciation", "DepreciationNonproduction"]))
        amo = quarterly(_pick(gaap, ["AmortizationOfIntangibleAssets"]))
        series["da"] = {k: v + amo.get(k, 0.0) for k, v in dep.items()} if dep else {}
    ends = list(rev)[-9:]
    rows = []
    for i, end in enumerate(ends):
        r = {"end": end, "revenue": rev.get(end), "op_income": series["op_income"].get(end),
             "net_income": series["net_income"].get(end), "eps": series["eps"].get(end),
             "gross_profit": series["gross_profit"].get(end), "da": series["da"].get(end)}
        r["opm_pct"] = (r["op_income"] / r["revenue"] * 100) if (r["revenue"] and r["op_income"] is not None) else None
        r["gm_pct"] = (r["gross_profit"] / r["revenue"] * 100) if (r["revenue"] and r["gross_profit"] is not None) else None
        rows.append(r)
    for i, r in enumerate(rows):
        prev = rows[i - 1] if i >= 1 else None
        yago = rows[i - 4] if i >= 4 else None
        for k in ("revenue", "op_income", "net_income", "eps"):
            r[f"{k}_qoq"] = _pct(r[k], prev[k]) if prev else None
            r[f"{k}_yoy"] = _pct(r[k], yago[k]) if yago else None
        r["opm_yoy_bps"] = ((r["opm_pct"] - yago["opm_pct"]) * 100) if (yago and r["opm_pct"] is not None and yago["opm_pct"] is not None) else None
    last4 = rows[-4:]
    def ttm(k):
        vals = [r[k] for r in last4]
        return sum(vals) if len(vals) == 4 and all(v is not None for v in vals) else None
    out = {"symbol": sym, "entity": ent, "quarters": rows, "latest_end": ends[-1],
           "revenue_ttm": ttm("revenue"), "op_income_ttm": ttm("op_income"),
           "net_income_ttm": ttm("net_income"), "eps_ttm": ttm("eps"), "da_ttm": ttm("da")}
    out["ebitda_ttm"] = (out["op_income_ttm"] + out["da_ttm"]) if (out["op_income_ttm"] is not None and out["da_ttm"] is not None) else None
    out["opm_ttm_pct"] = (out["op_income_ttm"] / out["revenue_ttm"] * 100) if (out["revenue_ttm"] and out["op_income_ttm"] is not None) else None
    eq = instant(_pick(gaap, CONCEPTS["equity"]))
    debt = instant(_pick(gaap, CONCEPTS["lt_debt"]))
    cash = instant(_pick(gaap, CONCEPTS["cash"]))
    sh = instant((dei.get("EntityCommonStockSharesOutstanding") or {}).get("units", {}).get("shares") or [])
    if not sh:
        # Multi-class issuers (Alphabet) carry no dei total in companyfacts; the
        # balance-sheet CommonStockSharesOutstanding is the combined count.
        sh = instant(_pick(gaap, ["CommonStockSharesOutstanding"], "shares"))
    if not sh:
        wa = quarterly(_pick(gaap, ["WeightedAverageNumberOfDilutedSharesOutstanding"], "shares"))
        sh = (list(wa)[-1], wa[list(wa)[-1]]) if wa else None
    out["equity"], out["lt_debt"], out["cash"] = (eq[1] if eq else None), (debt[1] if debt else None), (cash[1] if cash else None)
    out["shares"] = sh[1] if sh else None
    out["price"] = price
    out["market_cap"] = (price * out["shares"]) if (price and out["shares"]) else None
    out["pe"] = (price / out["eps_ttm"]) if (price and out["eps_ttm"] and out["eps_ttm"] > 0) else None
    out["book_value_ps"] = (out["equity"] / out["shares"]) if (out["equity"] and out["shares"]) else None
    out["pb"] = (price / out["book_value_ps"]) if (price and out["book_value_ps"] and out["book_value_ps"] > 0) else None
    out["roe_pct"] = (out["net_income_ttm"] / out["equity"] * 100) if (out["net_income_ttm"] is not None and out["equity"]) else None
    cap_emp = (out["equity"] or 0) + (out["lt_debt"] or 0)
    out["roce_pct"] = (out["op_income_ttm"] / cap_emp * 100) if (out["op_income_ttm"] is not None and cap_emp > 0) else None
    return out


def _fmt_b(v):
    return "—" if v is None else f"{v / B:,.2f}B"


def _fmt_p(v):
    return "—" if v is None else f"{v:+.1f}%"


def print_table(s: dict):
    if s.get("error"):
        print(f"{s['symbol']}: ⚠️ {s['error']}")
        return
    print(f"{s['symbol']} — {s['entity']}  (latest quarter ends {s['latest_end']})")
    print(f"{'quarter':<11}{'revenue':>10}{'QoQ':>8}{'YoY':>8}{'op inc':>10}{'OPM%':>7}{'net inc':>10}{'YoY':>8}{'EPS':>7}{'YoY':>8}")
    for r in s["quarters"][-6:]:
        opm = "—" if r["opm_pct"] is None else f"{r['opm_pct']:.1f}"
        eps = "—" if r["eps"] is None else f"{r['eps']:.2f}"
        print(f"{r['end']:<11}{_fmt_b(r['revenue']):>10}{_fmt_p(r['revenue_qoq']):>8}{_fmt_p(r['revenue_yoy']):>8}"
              f"{_fmt_b(r['op_income']):>10}{opm:>7}{_fmt_b(r['net_income']):>10}{_fmt_p(r['net_income_yoy']):>8}{eps:>7}{_fmt_p(r['eps_yoy']):>8}")
    def num(v, spec, suffix=""):
        return "—" if v is None else format(v, spec) + suffix
    print(f"TTM: revenue {_fmt_b(s['revenue_ttm'])} · EBITDA {_fmt_b(s['ebitda_ttm'])} · "
          f"OPM {num(s['opm_ttm_pct'], '.1f', '%')} · EPS {num(s['eps_ttm'], '.2f')}")
    print(f"price ${s['price'] or 0:,.2f} · mkt cap {_fmt_b(s['market_cap'])} · P/E {num(s['pe'], '.1f')} · "
          f"P/B {num(s['pb'], '.1f')} · ROE {num(s['roe_pct'], '.1f', '%')} · ROCE {num(s['roce_pct'], '.1f', '%')}")


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------

def _latest_close(client, sym: str):
    try:
        r = (client.table("sme_daily_prices").select("close,price_date").eq("ticker", sym)
             .order("price_date", desc=True).limit(1).execute().data or [])
        return float(r[0]["close"]) if r else None
    except Exception:
        return None


def update_all(client) -> int:
    import usfilings
    uni = usfilings.universe(client)
    n = 0
    for sym, info in uni.items():
        s = snapshot(sym, info["cik"], _latest_close(client, sym), expect_title=info["title"])
        if s.get("error"):
            print(f"  [usfund] {sym}: {s['error']} — not stored")
            continue
        f = lambda v: None if v is None else float(v)
        row = {"ticker": sym, "fetched_at": datetime.now(timezone.utc).isoformat(),
               "market_cap_cr": f(s["market_cap"] / B if s["market_cap"] else None),   # $ BILLIONS for US
               "pe": f(s["pe"]), "pb": f(s["pb"]), "book_value": f(s["book_value_ps"]),
               "roe": f(s["roe_pct"]), "roce": f(s["roce_pct"]),
               "revenue_ttm_cr": f(s["revenue_ttm"] / B if s["revenue_ttm"] else None),
               "ebitda_ttm_cr": f(s["ebitda_ttm"] / B if s["ebitda_ttm"] is not None else None),
               "opm_ttm_pct": f(s["opm_ttm_pct"])}
        try:
            client.table("fundamentals_daily").upsert(row, on_conflict="ticker").execute()
            n += 1
        except Exception as e:
            print(f"  [usfund] store failed {sym}: {e}")
    print(f"  [usfund] stored {n}/{len(uni)} US names")
    return n


if __name__ == "__main__":
    import usprices, usfilings
    c = usprices._client()
    arg = sys.argv[1] if len(sys.argv) > 1 else "update"
    if arg == "update":
        update_all(c)
    else:
        sym = arg.upper()
        cm = usfilings.cik_map()
        hit = cm.get(sym)
        if not hit:
            print(f"{sym}: not in SEC ticker list")
        else:
            print_table(snapshot(sym, hit[0], _latest_close(c, sym), expect_title=hit[1]))
