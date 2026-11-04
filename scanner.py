"""Stock-picking scanner (Vishal 01-Nov-2026) — every NSE stock, our own data.

Five years of NSE's official daily files (sec_bhavdata_full, from Nov-2021) are
cached under .cache/scan/, split/bonus-adjusted from the exchange's OWN numbers,
and scanned for entry setups:

  multiyear  price at / through a ceiling of up to 5 years (weekly closes)
  breakout   price clears the top of a base
  pivot      breaks a tight consolidation sitting on the rising 21-EMA
  pullback   touches the 21 or 50 EMA in an uptrend and bounces
  shakeout   a quick undercut of support, then a reclaim
  gapup      gaps above the prior day's high on volume and holds

ADJUSTMENT (House Rule #10): bhavcopy prices are RAW, so a split/bonus leaves
old high prices beside new low ones — every multi-year "ceiling" would be fake.
Each split/bonus/consolidation comes from NSE's own corporate-actions record
(data/corp_actions.json; ratio read from the subject text, cross-checked against
the actual ex-date price step). Prices before the ex-date are divided by the
factor, volumes multiplied. A >40% overnight move with no recorded action skips
that stock entirely — its chart can't be trusted.

Universe: series EQ/BE, NOT in the Nifty 100 (large caps excluded — Vishal:
growth throttles), 20-day average turnover >= MIN_TURNOVER_CR. Each name is
labelled Mid / Small / Micro from NSE's index lists ("—" = outside all three).

The daily-bar setups (breakout … gapup) count an event on any session of the
latest week that still holds at the week's close — one list per week, matching
the weekly-close rhythm. CLI: python scanner.py history | panel | corp-actions |
scan [<setup> ...] | run [weekly|daily] [--force]   (run = the Telegram job below)
"""
from __future__ import annotations

import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests

CACHE = os.path.join(".cache", "scan")
DAYS_DIR = os.path.join(CACHE, "days")
PANEL = os.path.join(CACHE, "panel.parquet")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
START = date(2021, 11, 1)
SERIES = ("EQ", "BE")
MIN_TURNOVER_CR = 1.0          # 20-day average daily turnover floor
LOOKBACK_WEEKS = 260           # multi-year ceiling: up to 5 years …
MIN_WEEKS = 52                 # … but a 1-year-old listing still counts
RECENT_WEEKS = 13              # the ceiling must have been set before this
NEAR_PCT = 5.0                 # "near" = within 5% below the level
INDEX_LISTS = {"Large": "ind_nifty100list", "Mid": "ind_niftymidcap150list",
               "Small": "ind_niftysmallcap250list", "Micro": "ind_niftymicrocap250_list"}
BENCH = "HDFCSML250"           # Smallcap 250 ETF, for relative strength


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------

def _fetch_day(d: date):
    """One day's EQ/BE rows → cached parquet. Returns (date, status)."""
    path = os.path.join(DAYS_DIR, f"{d:%Y%m%d}.parquet")
    miss = path + ".none"
    if os.path.exists(path) or os.path.exists(miss):
        return d, "cached"
    for host in ("nsearchives.nseindia.com", "archives.nseindia.com"):
        url = f"https://{host}/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
        except Exception as e:
            last = f"{type(e).__name__}"
            continue
        if r.status_code == 404:
            last = "404"
            continue
        if r.status_code == 200 and r.content[:2] == b"PK":
            # NSE published 08-Aug-2022 as an Excel workbook under the .csv name
            df = pd.read_excel(io.BytesIO(r.content))
        elif r.status_code != 200 or "SYMBOL" not in r.text[:300]:
            last = f"HTTP {r.status_code} {len(r.content)}B"
            continue
        else:
            df = pd.read_csv(io.StringIO(r.text), skipinitialspace=True)
        df.columns = [c.strip() for c in df.columns]
        df["SERIES"] = df["SERIES"].astype(str).str.strip()
        df = df[df["SERIES"].isin(SERIES)].copy()
        stamped = pd.to_datetime(df["DATE1"].astype(str).str.strip(), format="%d-%b-%Y",
                                 errors="coerce").dt.date
        if len(df) and (stamped != d).all():      # holiday republish of the day before
            open(miss, "w").write("republish")
            return d, "republish"
        out = pd.DataFrame({
            "date": pd.Timestamp(d), "symbol": df["SYMBOL"].astype(str).str.strip(),
            "series": df["SERIES"],
            "prev_close": pd.to_numeric(df["PREV_CLOSE"], errors="coerce"),
            "open": pd.to_numeric(df["OPEN_PRICE"], errors="coerce"),
            "high": pd.to_numeric(df["HIGH_PRICE"], errors="coerce"),
            "low": pd.to_numeric(df["LOW_PRICE"], errors="coerce"),
            "close": pd.to_numeric(df["CLOSE_PRICE"], errors="coerce"),
            "volume": pd.to_numeric(df["TTL_TRD_QNTY"], errors="coerce"),
            "turnover_cr": pd.to_numeric(df["TURNOVER_LACS"], errors="coerce") / 100.0,
            "deliv_pct": pd.to_numeric(df["DELIV_PER"], errors="coerce"),
        })
        out.to_parquet(path, index=False)
        return d, "ok"
    if last == "404":
        open(miss, "w").write("404")       # weekend/holiday: no file
    return d, last


def build_history(end: date | None = None, workers: int = 6) -> None:
    """Download every missing trading day since START (cached, incremental)."""
    os.makedirs(DAYS_DIR, exist_ok=True)
    end = end or date.today()
    days, d = [], START
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    # the newest few days may 404 only because they aren't published yet:
    # never cache a miss for the last 4 days
    for d in days[-4:]:
        p = os.path.join(DAYS_DIR, f"{d:%Y%m%d}.parquet.none")
        if os.path.exists(p):
            os.remove(p)
    t0, counts, failed = time.time(), {}, []
    with ThreadPoolExecutor(workers) as ex:
        for i, (d, st) in enumerate(ex.map(_fetch_day, days), 1):
            counts[st] = counts.get(st, 0) + 1
            if st not in ("ok", "cached", "404", "republish"):
                failed.append((d, st))
            if i % 200 == 0:
                print(f"  {i}/{len(days)} days · {counts} · {time.time() - t0:.0f}s", flush=True)
    print(f"[scan] history: {counts} in {time.time() - t0:.0f}s")
    for d, st in failed[:10]:
        print(f"  ⚠️ {d}: {st}")
    _write_panel()


def _write_panel() -> None:
    files = sorted(f for f in os.listdir(DAYS_DIR) if f.endswith(".parquet"))
    panel = pd.concat((pd.read_parquet(os.path.join(DAYS_DIR, f)) for f in files), ignore_index=True)
    panel = adjust(panel)
    panel.to_parquet(PANEL, index=False)
    print(f"[scan] panel: {len(panel):,} rows · {panel['symbol'].nunique():,} symbols · "
          f"{panel['date'].min():%d %b %Y} → {panel['date'].max():%d %b %Y}")


CA_FILE = os.path.join("data", "corp_actions.json")   # committed: NSE's API blocks GitHub's IPs
GAP_FLAG = 0.40               # an overnight move this big with no corporate action = suspect


def corporate_actions(refresh: bool = False) -> list:
    """Every split / bonus / consolidation since START, from NSE's own
    corporate-actions record; the ratio is READ from the subject text.

    The list is committed in data/ because NSE's API answers a home IP but not
    GitHub/Render. A refresh fetches the last 120 days and merges; when NSE
    refuses (as it does from a datacenter) the saved list is used and the miss
    is logged. A split newer than the saved list then shows as an unexplained
    overnight gap, and scan() skips that stock rather than read a fake chart."""
    import json
    have = json.load(open(CA_FILE, encoding="utf-8")) if os.path.exists(CA_FILE) else []
    if have and not refresh:
        return have
    frm = date.today() - timedelta(days=120) if have else START
    try:
        new = _fetch_corporate_actions(frm)
    except Exception as e:
        newest = max((x["ex"] for x in have), default="none")
        print(f"  ⚠️ corporate actions refresh failed ({type(e).__name__}: {str(e)[:120]}) — "
              f"using the saved list ({len(have)} events, newest ex-date {newest})")
        return have
    merged = {(x["symbol"], x["ex"], x["subject"]): x for x in have + new}
    out = sorted(merged.values(), key=lambda x: (x["ex"], x["symbol"]))
    os.makedirs(os.path.dirname(CA_FILE), exist_ok=True)
    json.dump(out, open(CA_FILE, "w", encoding="utf-8"), indent=0)
    print(f"[scan] corporate actions: {len(new)} fetched since {frm:%d %b %Y}, "
          f"{len(out) - len(have)} new → {len(out)} saved")
    return out


def _fetch_corporate_actions(frm: date, to: date | None = None) -> list:
    import json
    import re
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
    s = requests.Session()
    s.headers.update({"User-Agent": ua})
    ref = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
    s.get(ref, timeout=20)
    to = to or date.today()
    rows, y = [], frm.year
    while y <= to.year:
        a = max(frm, date(y, 1, 1))
        b = min(to, date(y, 12, 31))
        r = s.get(f"https://www.nseindia.com/api/corporates-corporateActions?index=equities"
                  f"&from_date={a:%d-%m-%Y}&to_date={b:%d-%m-%Y}", headers={"Referer": ref}, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"NSE corporate actions {y}: HTTP {r.status_code} {len(r.content)}B")
        rows += json.loads(r.text)
        y += 1
    out = []
    for x in rows:
        subj = str(x.get("subject", ""))
        f = None
        m = re.search(r"from\s+r[se]\.?\s*([\d.]+).*?to\s+r[se]\.?\s*([\d.]+)", subj, re.I)
        if m and re.search(r"split|sub-?div|consolidat", subj, re.I):
            f = float(m.group(1)) / float(m.group(2))
        m = re.search(r"bonus\s*(\d+)\s*:\s*(\d+)", subj, re.I)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            f = (a + b) / b
        if f and f != 1:
            out.append({"symbol": x["symbol"], "ex": pd.to_datetime(x["exDate"], format="%d-%b-%Y").strftime("%Y-%m-%d"),
                        "factor": f, "subject": subj})
    return out


def adjust(panel: pd.DataFrame) -> pd.DataFrame:
    """Split/bonus-adjust from NSE's corporate-actions record (House Rule #10):
    prices before each ex-date ÷ factor, volumes × factor. Raw daily files stay
    untouched on disk. Every event is cross-checked against the actual ex-date
    price step; any overnight move > GAP_FLAG with NO action is flagged
    (`gap_flag`), because that chart can't be trusted for a multi-year level."""
    panel = panel.sort_values(["symbol", "date"]).reset_index(drop=True)
    panel["adj"] = 1.0
    events = corporate_actions()
    # Same-day actions combine (Bajaj Finserv 13-Sep-2022: 5x split + 1:1 bonus = 10x,
    # actual step 9.77x) — check the combined factor, not each action alone.
    by_sym, seen = {}, set()
    for e in events:
        k = (e["symbol"], e["ex"], e["subject"])
        if k in seen:
            continue
        seen.add(k)
        d = by_sym.setdefault(e["symbol"], {})
        d[pd.Timestamp(e["ex"])] = d.get(pd.Timestamp(e["ex"]), 1.0) * e["factor"]
    by_sym = {s: sorted(d.items()) for s, d in by_sym.items()}
    idx = panel.groupby("symbol").indices
    mismatched = []
    for sym, evs in by_sym.items():
        if sym not in idx:
            continue
        rows = idx[sym]
        dates = panel["date"].values[rows]
        for ex, fac in evs:
            before = rows[dates < np.datetime64(ex)]
            after = rows[dates >= np.datetime64(ex)]
            if not len(before) or not len(after):
                continue
            panel.loc[before, "adj"] /= fac
            step = panel.at[before[-1], "close"] / panel.at[after[0], "open"]
            if not (fac / 1.6 <= step <= fac * 1.6):
                mismatched.append((sym, ex.date(), fac, round(step, 2)))
    for c in ("open", "high", "low", "close"):
        panel[c] = panel[c] * panel["adj"]
    panel["volume"] = panel["volume"] / panel["adj"]
    panel["n_adj"] = (panel["adj"] != 1.0).groupby(panel["symbol"]).transform("any").astype(int)
    prev = panel.groupby("symbol")["close"].shift(1)
    jump = (panel["open"] / prev - 1).abs()
    panel["gap_day"] = (jump > GAP_FLAG).astype("int8")
    panel["gap_flag"] = panel["gap_day"].groupby(panel["symbol"]).transform("any").astype(int)
    print(f"[scan] adjusted {len(events)} actions · {len(mismatched)} whose price step "
          f"disagrees with the ratio · {panel.loc[panel.gap_flag == 1, 'symbol'].nunique()} "
          f"symbols with an unexplained >{GAP_FLAG:.0%} overnight move")
    for m in mismatched[:15]:
        print("   step mismatch:", m)
    return panel


# ---------------------------------------------------------------------------
# universe
# ---------------------------------------------------------------------------

def cap_labels() -> dict:
    out = {}
    for label, name in INDEX_LISTS.items():
        try:
            r = requests.get(f"https://niftyindices.com/IndexConstituent/{name}.csv",
                             headers=HEADERS, timeout=30)
            for sym in pd.read_csv(io.StringIO(r.text))["Symbol"].astype(str).str.strip():
                out.setdefault(sym, label)
        except Exception as e:
            print(f"  ⚠️ index list {name} failed: {type(e).__name__}: {e}")
    return out


def company_names() -> dict:
    try:
        r = requests.get("https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
                         headers=HEADERS, timeout=30)
        df = pd.read_csv(io.StringIO(r.content.decode("utf-8-sig", "replace")))
        df.columns = [c.strip() for c in df.columns]
        return dict(zip(df["SYMBOL"].astype(str).str.strip(), df["NAME OF COMPANY"].astype(str).str.strip()))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# scans
# ---------------------------------------------------------------------------

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def _weekly(g: pd.DataFrame) -> pd.DataFrame:
    w = g.set_index("date").resample("W-FRI").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna(subset=["close"])
    return w


ALL_SETUPS = ("multiyear", "breakout", "pivot", "pullback", "shakeout", "gapup", "forming")


def scan(which=ALL_SETUPS, daily: bool = False, panel=None, caps=None, names=None) -> dict:
    """WEEKLY mode (default): events on any session of the latest week that still
    hold at its last close. DAILY mode: events on the latest session only, and the
    multi-year ceiling counts a provisional daily-close cross (it confirms at the
    week's close). `forming` = tight bases on a rising 21-EMA not yet broken — the
    watch list the daily triggers fire on."""
    panel = pd.read_parquet(PANEL) if panel is None else panel
    last = panel["date"].max()
    week_start = last - pd.Timedelta(days=last.weekday())          # Monday of the latest week
    start = last if daily else week_start
    caps = cap_labels() if caps is None else caps
    names = company_names() if names is None else names
    bench = panel[panel["symbol"] == BENCH].set_index("date")["close"]
    b6 = (bench.iloc[-1] / bench.iloc[-127] - 1) * 100 if len(bench) > 127 else None
    res = {k: [] for k in which}
    n_univ = n_gap = 0
    for sym, g in panel.groupby("symbol", sort=False):
        g = g.sort_values("date").reset_index(drop=True)
        if g["date"].iloc[-1] != last or caps.get(sym) == "Large" or sym == BENCH:
            continue
        if g["series"].iloc[-1] not in SERIES or len(g) < 60:
            continue
        if names and sym not in names:                 # ETFs, liquid funds, bonds
            continue
        if "gap_day" in g and g["gap_day"].iloc[-260:].any():
            # an unexplained >40% overnight move in the last year — almost always a
            # split/bonus NSE's saved list doesn't have yet. Its EMAs, base and 52-week
            # high are fiction, so it gets no setup at all (a blank beats a wrong one)
            n_gap += 1
            continue
        to20 = g["turnover_cr"].tail(20).mean()
        if not to20 or to20 < MIN_TURNOVER_CR:
            continue
        n_univ += 1
        c, h, l, o, v = g["close"], g["high"], g["low"], g["open"], g["volume"]
        e21, e50 = _ema(c, 21), _ema(c, 50)
        sma50 = c.rolling(50).mean()
        sma200 = c.rolling(200).mean()
        hi52 = h.tail(250).max()
        r6 = (c.iloc[-1] / c.iloc[-127] - 1) * 100 if len(c) > 127 else None
        vol50 = v.rolling(50).mean().shift(1)
        leader = (c.iloc[-1] > sma50.iloc[-1]
                  and (pd.isna(sma200.iloc[-1]) or sma50.iloc[-1] > sma200.iloc[-1])
                  and c.iloc[-1] >= 0.75 * hi52
                  and (r6 is None or b6 is None or r6 > b6))
        base = {"symbol": sym, "name": names.get(sym, ""), "cap": caps.get(sym, "—"),
                "close": round(float(c.iloc[-1]), 2), "turnover_cr": round(float(to20), 1),
                "deliv_pct": round(float(g["deliv_pct"].iloc[-1]), 1) if pd.notna(g["deliv_pct"].iloc[-1]) else None,
                "pct_52wh": round(float((c.iloc[-1] / hi52 - 1) * 100), 1),
                "rs6m": round(r6 - b6, 1) if r6 is not None and b6 is not None else None,
                "adjusted": int(g["n_adj"].iloc[-1])}
        wk_idx = g.index[g["date"] >= start]                         # sessions in the window

        # ---- multi-year ceiling (weekly closes) ----------------------------
        # Ceiling R = the highest weekly close of up to 5 years BEFORE this week,
        # and it must have stood unbroken for >= MIN_WEEKS (a year) — a 3-month-old
        # high is a recent high, not a multi-year one. A chart with an unexplained
        # >40% overnight jump is skipped: its old prices can't be trusted.
        if "multiyear" in which and not int(g["gap_flag"].iloc[-1]):
            w = _weekly(g)
            if len(w) >= MIN_WEEKS + 2:
                hist = w["close"].iloc[-(LOOKBACK_WEEKS + 1):-1]
                R, r_date = float(hist.max()), hist.idxmax()
                age_w = (w.index[-2] - r_date).days / 7
                if age_w >= MIN_WEEKS:
                    wc = float(w["close"].iloc[-1])
                    wv = float(w["volume"].iloc[-1]); wv20 = float(w["volume"].iloc[-21:-1].mean())
                    row = {**base, "level": round(R, 2), "level_set": f"{r_date:%b %Y}",
                           "level_age_y": round((w.index[-1] - r_date).days / 365.25, 1),
                           "history_y": round((w.index[-1] - w.index[0]).days / 365.25, 1),
                           "vol_x": round(wv / wv20, 1) if wv20 else None}
                    vd = float(v.iloc[-1] / vol50.iloc[-1]) if vol50.iloc[-1] else 0
                    if daily:
                        # a fresh DAILY close above the ceiling — provisional until the
                        # week closes above it too (Vishal: weekly-close confirmation)
                        if c.iloc[-1] > R and c.iloc[-2] <= R:
                            res["multiyear"].append({**row, "status": "PROVISIONAL", "vol_x": round(vd, 1),
                                                     "entry": round(R, 2), "stop": round(R * 0.93, 2)})
                    elif wc > R:
                        res["multiyear"].append({**row, "status": "BREAKOUT", "entry": round(R, 2),
                                                 "stop": round(R * 0.93, 2)})
                    elif wc >= R * (1 - NEAR_PCT / 100) and c.iloc[-1] >= hi52 * 0.95:
                        res["multiyear"].append({**row, "status": "NEAR",
                                                 "gap_pct": round((R / wc - 1) * 100, 1),
                                                 "entry": round(R, 2), "stop": None})

        # ---- forming: a tight base on a rising 21-EMA, not yet broken (watch list) --
        if "forming" in which and leader and len(g) > 70:
            win = g.iloc[-10:]
            top, bot = float(win["high"].max()), float(win["low"].min())
            if ((top - bot) / bot * 100 <= 8
                    and ((win["close"] / e21.iloc[-10:] - 1).abs() <= 0.04).all()
                    and e21.iloc[-1] > e21.iloc[-11]
                    and top * 0.97 <= c.iloc[-1] <= top):
                res["forming"].append({**base, "range_pct": round((top - bot) / bot * 100, 1),
                                       "gap_pct": round((top / c.iloc[-1] - 1) * 100, 1),
                                       "entry": round(top, 2), "stop": round(bot, 2)})

        if not len(wk_idx):
            continue
        # ---- daily setups: an event in the latest week, still valid at Friday --
        for t in wk_idx:
            if t < 61:
                continue
            vx = float(v.iloc[t] / vol50.iloc[t]) if vol50.iloc[t] else None
            if not leader:                                  # "one leader, five ways in"
                break
            if "breakout" in which:
                for n, depth_max in ((30, 25.0), (60, 35.0)):
                    win = g.iloc[t - n:t]
                    top, bot = float(win["high"].max()), float(win["low"].min())
                    depth = (top - bot) / top * 100
                    if (depth <= depth_max and c.iloc[t] > top and c.iloc[t - 1] <= top
                            and vx and vx >= 1.5 and c.iloc[-1] > top):
                        res["breakout"].append({**base, "day": f"{g['date'].iloc[t]:%a %d %b}",
                                                "base_weeks": n // 5, "base_depth_pct": round(depth, 1),
                                                "vol_x": round(vx, 1), "entry": round(top, 2),
                                                "stop": round(max(bot, top * 0.92), 2)})
                        break
            if "pivot" in which:
                win = g.iloc[t - 10:t]
                top, bot = float(win["high"].max()), float(win["low"].min())
                tight = (top - bot) / bot * 100
                on_ema = ((win["close"] / e21.iloc[t - 10:t] - 1).abs() <= 0.04).all()
                rising = e21.iloc[t] > e21.iloc[t - 10]
                if (tight <= 10 and on_ema and rising and c.iloc[t] > top and c.iloc[t - 1] <= top
                        and vx and vx >= 1.5 and c.iloc[-1] > top):
                    res["pivot"].append({**base, "day": f"{g['date'].iloc[t]:%a %d %b}",
                                         "range_pct": round(tight, 1), "vol_x": round(vx, 1),
                                         "entry": round(top, 2), "stop": round(bot, 2)})
            if "pullback" in which:
                for name, e in (("21-EMA", e21), ("50-EMA", e50)):
                    above_before = (c.iloc[t - 5:t] > e.iloc[t - 5:t]).all()
                    if (above_before and l.iloc[t] <= e.iloc[t] * 1.01 and c.iloc[t] > e.iloc[t]
                            and c.iloc[t] > c.iloc[t - 1] and e.iloc[t] > e.iloc[t - 10]
                            and e.iloc[-1] < c.iloc[-1] <= e.iloc[-1] * 1.05):
                        res["pullback"].append({**base, "day": f"{g['date'].iloc[t]:%a %d %b}",
                                                "ema": name, "ema_level": round(float(e.iloc[-1]), 2),
                                                "pct_above_ema": round(float((c.iloc[-1] / e.iloc[-1] - 1) * 100), 1),
                                                "entry": round(float(e.iloc[-1]), 2),
                                                "stop": round(float(l.iloc[t]) * 0.98, 2)})
                        break
            if "shakeout" in which:
                sup = float(l.iloc[t - 16:t - 1].min())
                if (c.iloc[t - 1] > sup and sma50.iloc[t - 1] and c.iloc[t - 1] > sma50.iloc[t - 1]
                        and l.iloc[t] < sup * 0.99 and c.iloc[t] >= sup and c.iloc[-1] > sup):
                    res["shakeout"].append({**base, "day": f"{g['date'].iloc[t]:%a %d %b}",
                                            "support": round(sup, 2),
                                            "undercut_pct": round(float((l.iloc[t] / sup - 1) * 100), 1),
                                            "entry": round(sup, 2), "stop": round(float(l.iloc[t]), 2)})
            if "gapup" in which:
                gap = (o.iloc[t] / h.iloc[t - 1] - 1) * 100
                if (gap >= 3 and c.iloc[t] >= o.iloc[t] * 0.99 and vx and vx >= 2
                        and c.iloc[-1] > h.iloc[t - 1] and sma50.iloc[t] and c.iloc[t] > sma50.iloc[t]):
                    res["gapup"].append({**base, "day": f"{g['date'].iloc[t]:%a %d %b}",
                                         "gap_pct": round(float(gap), 1), "vol_x": round(vx, 1),
                                         "entry": round(float(h.iloc[t - 1]), 2),
                                         "stop": round(float(h.iloc[t - 1]) * 0.97, 2)})
    for k in res:                                                    # one row per symbol per scan
        seen, uniq = set(), []
        for r in res[k]:
            if r["symbol"] not in seen:
                seen.add(r["symbol"]); uniq.append(r)
        res[k] = uniq
    res["_meta"] = {"as_of": f"{last:%d %b %Y}", "last": last, "universe": n_univ,
                    "skipped_gap": n_gap, "bench_6m": b6}
    return res


# ---------------------------------------------------------------------------
# automation — the "New Stock Setups" Telegram group (Vishal + Lakshmi, 01-Nov-2026)
# ---------------------------------------------------------------------------
# Saturday morning: the week's list (as of Friday's close), best few per setup,
# plus a watch list for next week — Lakshmi plans the week on Saturdays.
# Mon–Fri evenings: TRIGGERS ONLY (multi-year crosses, pivot breaks, gap-ups),
# silent when nothing fired. Every posted setup is logged in `scan_setups`, so:
#   * a setup that closes below its stop gets a ❌ failure note (once),
#   * a breakout that comes back to its entry on light volume gets a 🔁 retest note,
#   * every setup is scored 4/8/13 weeks later vs the Smallcap 250 — the same
#     self-scoring idea as exit_audit.py, so the scan has to earn its place.
# Runs on GitHub Actions (needs ~250 MB of cached daily files — not the 512 MB
# Render worker). No model calls: Screener pages only, ~1.5 s each.

LABEL = {"multiyear": "🏔 Multi-year breakout", "breakout": "📈 Base breakout",
         "pivot": "🎯 Pivot breakout", "gapup": "🚀 Gap-up",
         "shakeout": "🪤 Shakeout &amp; reclaim", "pullback": "↩️ Pullback to EMA"}
ORDER = ("multiyear", "breakout", "pivot", "gapup", "shakeout", "pullback")
DAILY_SETUPS = ("multiyear", "pivot", "gapup")
WEEKLY_PER_SETUP = 5          # per setup on Saturday's list; the overflow count is shown
WATCH_MAX = 8
DAILY_MAX = 8
FRESH_PCT, STRETCHED_PCT = 3.0, 8.0   # past-entry bands; beyond STRETCHED is chasing, not listed
REPEAT_DAYS = 14              # the same stock + setup isn't re-posted within this
RETEST_DAYS = (4, 30)         # calendar days after posting that a retest counts
SCORE_WEEKS = (4, 8, 13)
GROUP = "setups"
SETUPS_SQL = """create table if not exists scan_setups (
  id bigserial primary key, posted_on date not null, symbol text not null,
  setup text not null, mode text not null, name text, cap text,
  entry numeric, stop numeric, close numeric, status text default 'open',
  status_on date, retest_on date,
  ret_4w numeric, bench_4w numeric, ret_8w numeric, bench_8w numeric,
  ret_13w numeric, bench_13w numeric, created_at timestamptz default now(),
  unique (posted_on, symbol, setup));"""


def _past(r: dict):
    """% the close sits above the entry (negative = still below it)."""
    return (r["close"] / r["entry"] - 1) * 100 if r.get("entry") else None


def _band(p) -> str:
    if p is None:
        return ""
    return "fresh" if p <= FRESH_PCT else "stretched" if p <= STRETCHED_PCT else "chasing"


def _about(page: str) -> str:
    import html as H
    import re
    m = re.search(r'class="sub show-more-box about"[^>]*>(.*?)</div>', page, re.S)
    if not m:
        return ""
    t = re.sub(r"<sup>.*?</sup>", "", m.group(1), flags=re.S)
    t = re.sub(r"\s+", " ", H.unescape(re.sub(r"<[^>]+>", " ", t))).strip()
    t = re.sub(r"\s*\(Source[^)]*\)", "", t)
    first = t
    for m in re.finditer(r"\.\s+(?=[A-Z])", t):          # first sentence end, not "Dr." / "Ltd."
        word = (re.findall(r"[A-Za-z]+$", t[:m.start()]) or [""])[0]
        if word.lower() not in ("dr", "mr", "mrs", "ms", "ltd", "pvt", "co", "inc", "no", "st", "corp"):
            first = t[:m.start()]
            break
    first = first.rstrip(". ")
    return first if len(first) <= 140 else first[:137].rsplit(" ", 1)[0] + "…"


def enrich(symbols) -> dict:
    """What the company does, market cap, P/E and the latest quarter's EPS, from
    Screener (the same pages fundamentals.py reads; identity-checked there)."""
    import screener_data as S
    out = {}
    for sym in dict.fromkeys(symbols):
        try:
            page, _basis = S.fetch_page(S.slug_for(sym + ".NS"))
        except Exception as e:
            print(f"  ⚠️ screener {sym}: {type(e).__name__}: {e}")
            continue
        if page:
            tr = S.top_ratios(page)
            q = S.quarterly(page, keep=5)
            out[sym] = {"about": _about(page), "mcap_cr": tr.get("market_cap_cr"),
                        "pe": tr.get("pe"), "q_eps": (q[-1] if q else {}).get("eps")}
        time.sleep(1.5)
    return out


def _books(client) -> dict:
    """symbol -> 'Lakshmi holds · Vishal watches' (our three Indian books)."""
    import re
    who = {}
    for table, verb in (("holdings", "holds"), ("watchlist", "watches")):
        try:
            rows = client.table(table).select("portfolio_id,stock_name").execute().data
        except Exception as e:
            print(f"  ⚠️ {table} read failed: {type(e).__name__}: {e}")
            continue
        for r in rows:
            m = re.search(r"\(XNSE:([^)]+)\)", r.get("stock_name") or "")
            person = {1: "Vishal", 2: "Lakshmi", 3: "Abinaya"}.get(r.get("portfolio_id"))
            if m and person:
                who.setdefault(m.group(1).upper(), set()).add(f"{person} {verb}")
    return {k: " · ".join(sorted(v)) for k, v in who.items()}


def _rs(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"₹{x:,.0f}" if x >= 100 else f"₹{x:,.2f}"


def _fmt_stock(r: dict, setup: str, info: dict, books: dict, also=()) -> str:
    import html as H
    sym = r["symbol"]
    i = info.get(sym, {})
    size = " · ".join(x for x in ((r.get("cap") or "—").replace("—", ""),
                                  f"₹{i['mcap_cr']:,.0f} Cr" if i.get("mcap_cr") else "") if x)
    lines = [f"<b>{H.escape(r.get('name') or sym, quote=False)}</b> ({sym})" + (f" · {size}" if size else "")]
    if i.get("about"):
        lines.append(f"<i>{H.escape(i['about'], quote=False)}</i>")
    p = _past(r)
    risk = (f" (−{(1 - r['stop'] / r['close']) * 100:.0f}% from CMP)"
            if r.get("stop") and r["stop"] < r["close"] else "")
    entry = (f"Entry {_rs(r.get('entry'))} · Stop {_rs(r.get('stop'))}{risk}" if r.get("stop")
             else f"Trigger {_rs(r.get('entry'))}")
    where = f" · {p:+.1f}% vs entry ({_band(p)})" if p is not None and p >= 0 else ""
    lines.append(f"{entry} · CMP {_rs(r['close'])}{where}")
    bits = []
    if r.get("vol_x"):
        bits.append(f"Vol {r['vol_x']:.1f}×")
    bits.append(f"{r['pct_52wh']:+.0f}% vs 52wH" if r["pct_52wh"] < -0.5 else "at 52wH")
    if i.get("pe"):
        bits.append(f"P/E {i['pe']:.0f}")
    eps = i.get("q_eps")
    if eps is not None:
        bits.append(f"run-rate P/E {r['close'] / (eps * 4):.0f}" if eps > 0 else "latest qtr a loss")
    lines.append(" · ".join(bits))
    ctx = []
    if setup == "multiyear":
        ctx.append(f"ceiling {_rs(r['level'])} from {r['level_set']} ({r['level_age_y']:.1f}y)")
    elif setup == "breakout":
        ctx.append(f"{r['base_weeks']}-week base, {r['base_depth_pct']:.0f}% deep, broke {r['day']}")
    elif setup == "pivot":
        ctx.append(f"{r['range_pct']:.0f}% range on the rising 21-EMA, broke {r['day']}")
    elif setup == "gapup":
        ctx.append(f"gapped {r['gap_pct']:.1f}% over the prior high on {r['day']}, held")
    elif setup == "shakeout":
        ctx.append(f"undercut {_rs(r['support'])} by {abs(r['undercut_pct']):.1f}% on {r['day']}, reclaimed")
    elif setup == "pullback":
        ctx.append(f"bounced off the {r['ema']} on {r['day']}")
    if also:
        ctx.append("also " + ", ".join(LABEL[a].split(" ", 1)[1] for a in also))
    if books.get(sym):
        ctx.append(f"📌 {books[sym]}")
    lines.append(" · ".join(ctx))
    return "\n".join(lines)


def _primary(res: dict, setups) -> dict:
    """One entry per stock: its first setup in ORDER supplies entry/stop, the
    rest are listed as 'also'."""
    out = {}
    for k in setups:
        for r in res.get(k, []):
            if r["symbol"] in out:
                out[r["symbol"]]["also"].append(k)
            else:
                out[r["symbol"]] = {"setup": k, "row": r, "also": []}
    return out


def _rank(rows: list) -> list:
    """Fresh first, then by volume."""
    band = {"fresh": 0, "stretched": 1, "chasing": 2, "": 1}
    return sorted(rows, key=lambda x: (band[_band(_past(x["row"]))], -(x["row"].get("vol_x") or 0)))


def _weekly_show(picked: dict) -> dict:
    """setup -> (shown, chasing, n_more) — what Saturday's list prints."""
    out = {}
    for k in ORDER:
        ranked = _rank([x for x in picked.values() if x["setup"] == k])
        if not ranked:
            continue
        shown = [x for x in ranked if _band(_past(x["row"])) != "chasing"][:WEEKLY_PER_SETUP]
        chasing = [x["row"]["symbol"] for x in ranked if _band(_past(x["row"])) == "chasing"]
        out[k] = (shown, chasing, len(ranked) - len(shown) - len(chasing))
    return out


def _daily_show(picked: dict):
    """(shown, chasing): by volume, the >STRETCHED_PCT ones named but not listed."""
    ranked = sorted(picked.values(), key=lambda x: -(x["row"].get("vol_x") or 0))
    ok = [x for x in ranked if _band(_past(x["row"])) != "chasing"]
    return ok, [x["row"]["symbol"] for x in ranked if _band(_past(x["row"])) == "chasing"]


# ---- the setups log -------------------------------------------------------

def _table_ok(client) -> bool:
    try:
        client.table("scan_setups").select("id").limit(1).execute()
        return True
    except Exception as e:
        print(f"  ⚠️ scan_setups table unreadable ({type(e).__name__}: {str(e)[:120]}) — nothing is "
              f"logged, scored or de-duplicated until it exists. Run once in Supabase:\n{SETUPS_SQL}")
        return False


def _recent(client, since: date) -> list:
    rows, start = [], 0
    while True:                                   # paginate past the 1,000-row cap
        part = (client.table("scan_setups").select("*").gte("posted_on", since.isoformat())
                .order("posted_on", desc=True).range(start, start + 999).execute().data)
        rows += part
        if len(part) < 1000:
            return rows
        start += 1000


def track(client, panel: pd.DataFrame, last: pd.Timestamp, logged: list):
    """Stops hit, retests holding, and 4/8/13-week scores for logged setups.
    Returns (failures, retests, score_line). Status writes happen in run() only
    after the message is delivered; scores are facts and are stored here."""
    if not logged:
        return [], [], ""
    want = {r["symbol"] for r in logged} | {BENCH}
    px = {s: g.set_index("date") for s, g in panel[panel["symbol"].isin(want)].groupby("symbol")}
    bench = px.get(BENCH)
    failures, retests, scores = [], [], []
    for r in logged:
        g = px.get(r["symbol"])
        if g is None or g.empty or g.index[-1] != last:
            continue
        posted = pd.Timestamp(r["posted_on"])
        close, low = float(g["close"].iloc[-1]), float(g["low"].iloc[-1])
        age = (last - posted).days
        if r.get("status") == "open" and r.get("stop") and 0 < age <= 7 * SCORE_WEEKS[-1]:
            if close < float(r["stop"]):
                failures.append({**r, "now": close})
            elif (not r.get("retest_on") and r["setup"] in ("multiyear", "breakout", "pivot", "gapup")
                  and RETEST_DAYS[0] <= age <= RETEST_DAYS[1] and r.get("entry")):
                e = float(r["entry"])
                v50 = float(g["volume"].iloc[-51:-1].mean()) if len(g) > 51 else None
                vx = float(g["volume"].iloc[-1]) / v50 if v50 else None
                if low <= e * 1.02 and close >= e and vx is not None and vx < 1.0:
                    retests.append({**r, "now": close, "vol_x": round(vx, 1)})
        for w in SCORE_WEEKS:
            if r.get(f"ret_{w}w") is None and age >= 7 * w and bench is not None and r.get("close"):
                at = posted + pd.Timedelta(days=7 * w)
                c_then = g["close"][g.index <= at]
                b0, b1 = bench["close"][bench.index <= posted], bench["close"][bench.index <= at]
                if len(c_then) and len(b0) and len(b1):
                    ret = round((float(c_then.iloc[-1]) / float(r["close"]) - 1) * 100, 2)
                    b = round((float(b1.iloc[-1]) / float(b0.iloc[-1]) - 1) * 100, 2)
                    scores.append((r["id"], w, ret, b))
                    r[f"ret_{w}w"], r[f"bench_{w}w"] = ret, b
    for rid, w, ret, b in scores:
        try:
            client.table("scan_setups").update({f"ret_{w}w": ret, f"bench_{w}w": b}).eq("id", rid).execute()
        except Exception as e:
            print(f"  ⚠️ score write {rid}: {type(e).__name__}: {e}")
    parts = []
    for w in SCORE_WEEKS:
        xs = [(float(r[f"ret_{w}w"]), float(r[f"bench_{w}w"])) for r in logged if r.get(f"ret_{w}w") is not None]
        if len(xs) >= 5:
            avg, bavg = np.mean([a for a, _ in xs]), np.mean([b for _, b in xs])
            beat = sum(a > b for a, b in xs)
            parts.append(f"{w}w: {len(xs)} setups avg {avg:+.1f}% vs SC250 {bavg:+.1f}%, {beat} beat it")
    return failures, retests, ("📊 <b>Scorecard</b> (from the post-day close) — " + " | ".join(parts)) if parts else ""


def _fmt_tracking(failures, retests) -> list:
    import html as H
    out = []
    if retests:
        out.append("<b>🔁 Retest holding</b> (back at the breakout level on light volume)\n" + "\n".join(
            f"• <b>{H.escape(r.get('name') or r['symbol'], quote=False)}</b> ({r['symbol']}): low touched entry "
            f"{_rs(float(r['entry']))}, closed {_rs(r['now'])} on {r['vol_x']:.1f}× volume — posted "
            f"{pd.Timestamp(r['posted_on']):%d %b} as {LABEL[r['setup']].split(' ', 1)[1]}" for r in retests))
    if failures:
        out.append("<b>❌ Stopped out</b> (closed below the stop)\n" + "\n".join(
            f"• <b>{H.escape(r.get('name') or r['symbol'], quote=False)}</b> ({r['symbol']}): closed {_rs(r['now'])}, "
            f"stop {_rs(float(r['stop']))} — posted {pd.Timestamp(r['posted_on']):%d %b} at "
            f"{_rs(float(r['close']))} ({(r['now'] / float(r['close']) - 1) * 100:+.1f}%)" for r in failures))
    return out


# ---- messages -------------------------------------------------------------

def weekly_message(res: dict, info: dict, books: dict, picked: dict, extra: list) -> str:
    import html as H
    m = res["_meta"]
    blocks = [f"📋 <b>Weekly setups — week to {m['last']:%a %d %b}</b>\n"
              f"Leaders only: above the 50 &amp; 200-DMA, within 25% of the 52-week high, beating the "
              f"Smallcap 250 over 6 months. {m['universe']:,} NSE stocks scanned (ex-Nifty 100, "
              f"≥₹{MIN_TURNOVER_CR:g} Cr/day).\nFresh = ≤{FRESH_PCT:g}% past entry · stretched "
              f"{FRESH_PCT:g}–{STRETCHED_PCT:g}% · further than that isn't listed."]
    for k, (shown, chasing, more) in _weekly_show(picked).items():
        title = f"<b>{LABEL[k]}</b> ({len(shown) + len(chasing) + more})"
        if not shown:
            blocks.append(f"{title}\n<i>all already more than {STRETCHED_PCT:g}% past entry: {', '.join(chasing)}</i>")
            continue
        blocks.append(title)
        blocks += [_fmt_stock(x["row"], k, info, books, x["also"]) for x in shown]
        tail = ([f"+{more} more not shown"] if more > 0 else []) + \
               ([f"past {STRETCHED_PCT:g}% (not listed): {', '.join(chasing)}"] if chasing else [])
        if tail:
            blocks.append("<i>" + " · ".join(tail) + "</i>")
    watch = []
    for r in res.get("multiyear", []):
        if r["status"] == "NEAR":
            watch.append((r["gap_pct"], f"• <b>{H.escape(r['name'] or r['symbol'], quote=False)}</b> ({r['symbol']}) · "
                                        f"{r['cap']} — {r['gap_pct']:.1f}% below its {_rs(r['level'])} ceiling "
                                        f"from {r['level_set']}"))
    for r in res.get("forming", []):
        watch.append((r["gap_pct"], f"• <b>{H.escape(r['name'] or r['symbol'], quote=False)}</b> ({r['symbol']}) · "
                                    f"{r['cap']} — tight {r['range_pct']:.0f}% base on the 21-EMA, trigger "
                                    f"{_rs(r['entry'])} ({r['gap_pct']:.1f}% away)"))
    if watch:
        watch.sort(key=lambda x: x[0])
        blocks.append("<b>👀 Watch next week</b> (a break on volume posts here as a trigger)\n"
                      + "\n".join(w for _, w in watch[:WATCH_MAX])
                      + (f"\n<i>+{len(watch) - WATCH_MAX} more</i>" if len(watch) > WATCH_MAX else ""))
    blocks += extra
    blocks.append("<i>Chart levels, not buy calls: check the business and size the position before acting.</i>")
    return "\n\n".join(blocks)


def daily_message(res: dict, info: dict, books: dict, picked: dict, extra: list) -> str:
    m = res["_meta"]
    blocks = [f"⚡ <b>Setup triggers — {m['last']:%a %d %b}</b>"]
    ranked, chasing = _daily_show(picked)
    for k in DAILY_SETUPS:
        rows = [x for x in ranked[:DAILY_MAX] if x["setup"] == k]
        if not rows:
            continue
        note = " — provisional, confirms on Friday's weekly close" if k == "multiyear" else ""
        blocks.append(f"<b>{LABEL[k]}</b>{note}")
        blocks += [_fmt_stock(x["row"], k, info, books, x["also"]) for x in rows]
    tail = ([f"+{len(ranked) - DAILY_MAX} more on lighter volume: "
             f"{', '.join(x['row']['symbol'] for x in ranked[DAILY_MAX:])}"] if len(ranked) > DAILY_MAX else []) +            ([f"already more than {STRETCHED_PCT:g}% past entry (not listed): {', '.join(chasing)}"] if chasing else [])
    if tail:
        blocks.append("<i>" + " · ".join(tail) + "</i>")
    blocks += extra
    return "\n\n".join(blocks)


# ---- the run --------------------------------------------------------------

def run(mode: str | None = None, force: bool = False) -> int:
    """mode: 'weekly' | 'daily' | None (= weekly on Saturday, daily Mon–Fri, Sunday off).
    Returns a process exit code. With ALERTS_DRY_RUN the message is printed and
    nothing is written (alerts.sb() hands back a read-only client)."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import alerts
    from notify import chat_for_group, send_telegram
    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    if mode is None and today.weekday() == 6 and not force:
        print("[setups] Sunday — nothing to do")
        return 0
    mode = mode or ("weekly" if today.weekday() == 5 else "daily")
    corporate_actions(refresh=True)
    build_history(end=today)
    panel = pd.read_parquet(PANEL)
    last = panel["date"].max()
    if not force:
        if mode == "daily" and last.date() != today:
            print(f"[setups] ⚠️ no NSE file for {today:%d %b} (newest {last:%d %b}) — holiday or not yet "
                  f"published; nothing sent")
            return 0
        if mode == "weekly" and last.date() < today - timedelta(days=today.weekday()):
            print(f"[setups] ⚠️ no session this week (newest {last:%d %b}) — nothing sent")
            return 0
    client = alerts.sb()
    table = _table_ok(client)
    logged = _recent(client, (last - pd.Timedelta(days=7 * SCORE_WEEKS[-1] + 7)).date()) if table else []
    if not force and any(r["posted_on"] == f"{last:%Y-%m-%d}" and r["mode"] == mode for r in logged):
        print(f"[setups] {mode} post for {last:%d %b} already sent — skipping")
        return 0
    caps, names = cap_labels(), company_names()
    if not any(v == "Large" for v in caps.values()) or not names:
        # without the Nifty 100 list large caps slip in; without EQUITY_L, ETFs do —
        # a wrong list is worse than no list (House Rule #2)
        print(f"[setups] ⚠️ index lists ({len(caps)} names) or NSE's equity list ({len(names)}) "
              f"failed to download — nothing sent")
        return 1
    if mode == "weekly":
        res = scan(ALL_SETUPS, panel=panel, caps=caps, names=names)
        # a multi-year NEAR hasn't broken anything yet — it belongs on the watch list only
        picked = _primary({**res, "multiyear": [r for r in res["multiyear"] if r["status"] == "BREAKOUT"]}, ORDER)
    else:
        res = scan(DAILY_SETUPS, daily=True, panel=panel, caps=caps, names=names)
        picked = _primary(res, DAILY_SETUPS)
        cutoff = last - pd.Timedelta(days=REPEAT_DAYS)
        seen = {(r["symbol"], r["setup"]) for r in logged if pd.Timestamp(r["posted_on"]) >= cutoff}
        picked = {s: x for s, x in picked.items() if (s, x["setup"]) not in seen}
    m = res["_meta"]
    print(f"[setups] {mode} · as of {m['as_of']} · universe {m['universe']} · {m['skipped_gap']} skipped for "
          f"an unexplained gap · " + ", ".join(f"{k} {len(v)}" for k, v in res.items() if k != "_meta"))
    failures, retests, score_line = track(client, panel, last, logged)
    extra = _fmt_tracking(failures, retests)
    if mode == "weekly" and score_line:
        extra.append(score_line)
    if mode == "weekly":
        show = [x for shown, _, _ in _weekly_show(picked).values() for x in shown]
    else:
        show = _daily_show(picked)[0][:DAILY_MAX]
    if not show and not extra:
        print("[setups] nothing triggered — silent")
        return 0
    info = enrich([x["row"]["symbol"] for x in show])        # only what gets printed
    books = _books(client)
    msg = (weekly_message if mode == "weekly" else daily_message)(res, info, books, picked, extra)
    if not send_telegram(msg, chat_id=chat_for_group(GROUP)):
        print("[setups] ⚠️ Telegram send FAILED — nothing logged, the next run retries")
        return 1
    if table:
        rows = [{"posted_on": f"{last:%Y-%m-%d}", "symbol": x["row"]["symbol"], "setup": x["setup"],
                 "mode": mode, "name": x["row"].get("name"), "cap": x["row"].get("cap"),
                 "entry": x["row"].get("entry"), "stop": x["row"].get("stop"), "close": x["row"]["close"]}
                for x in show]
        rows = [{k: (v.item() if isinstance(v, np.generic) else v) for k, v in r.items()} for r in rows]  # Rule #6
        try:
            if rows:
                client.table("scan_setups").upsert(rows, on_conflict="posted_on,symbol,setup").execute()
            for r in failures:
                client.table("scan_setups").update(
                    {"status": "stopped", "status_on": f"{last:%Y-%m-%d}"}).eq("id", r["id"]).execute()
            for r in retests:
                client.table("scan_setups").update({"retest_on": f"{last:%Y-%m-%d}"}).eq("id", r["id"]).execute()
        except Exception as e:
            print(f"  ⚠️ scan_setups write failed: {type(e).__name__}: {e}")
    print(f"[setups] sent: {len(show)} setups, {len(retests)} retests, {len(failures)} stopped")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if cmd == "history":
        build_history()
    elif cmd == "panel":
        _write_panel()
    elif cmd == "corp-actions":
        corporate_actions(refresh=True)
    elif cmd == "run":
        # python scanner.py run [weekly|daily] [--force]
        args = sys.argv[2:]
        sys.exit(run(next((x for x in args if x in ("weekly", "daily")), None), force="--force" in args))
    else:
        which = tuple(sys.argv[2:]) or ("multiyear", "breakout", "pivot", "pullback", "shakeout", "gapup")
        r = scan(which)
        m = r.pop("_meta")
        print(f"as of {m['as_of']} · universe {m['universe']} stocks (ex-Nifty 100, ≥₹{MIN_TURNOVER_CR:g} Cr/day)")
        for k, rows in r.items():
            print(f"\n=== {k}: {len(rows)}")
            for row in rows[:40]:
                print("  ", row)
