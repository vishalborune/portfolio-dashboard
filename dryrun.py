#!/usr/bin/env python
"""DRY-RUN harness — run any alert mode against LIVE data and PRINT exactly what
WOULD be sent to Telegram, without sending anything and without writing any
dedup/state row (so it can never suppress a later real alert). Safe anytime.

    python dryrun.py deals          # NSE + BSE bulk/block deals in your stocks
    python dryrun.py filings-nse    # NSE exchange filings (15-min job)
    python dryrun.py filings        # filings incl BSE
    python dryrun.py eod-entries    # evening entry/add + risk/stop alerts (EOD)
    python dryrun.py states         # flowchart state-change + volume alerts
    python dryrun.py fast-poll      # one live intraday cycle (entries + risk)
    python dryrun.py digest         # weekly digest (email + telegram preview)
    python dryrun.py filings-audit  # read-only coverage report
    python dryrun.py reconcile      # read-only: does the digest's valuation
                                    # still match an independent re-pricing?
    python dryrun.py thesis "Krishival Foods (XNSE:KRISHIVAL)"
                                    # Stage-2 scorecard for one stock — prints the
                                    # note, sends nothing, stores nothing

Creds are read from .streamlit/secrets.toml. ALERTS_DRY_RUN makes notify.py print
instead of deliver, and makes alerts.sb() return a read-only client.
"""
import os
import sys
import tomllib
from pathlib import Path

mode = sys.argv[1] if len(sys.argv) > 1 else "states"
repo = Path(__file__).resolve().parent
try:
    s = tomllib.load(open(repo / ".streamlit" / "secrets.toml", "rb"))
    os.environ.setdefault("SUPABASE_URL", s["SUPABASE_URL"])
    os.environ.setdefault("SUPABASE_SERVICE_KEY", s["SUPABASE_SERVICE_KEY"])
    # Optional creds: pass through so a dry-run actually exercises the AI summary
    # path (needs ANTHROPIC_API_KEY). Without this you'd only ever preview
    # headline-only filings and never see the real summarized output.
    for _k in ("ANTHROPIC_API_KEY", "RESEND_API_KEY",
               "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if s.get(_k):
            os.environ.setdefault(_k, s[_k])
except Exception as e:
    print(f"Could not read .streamlit/secrets.toml: {e}")
    sys.exit(1)

os.environ["ALERTS_DRY_RUN"] = "1"
# a dummy chat id so the alert functions reach the (now-mocked) sender and we
# actually SEE the message text; the sender prints instead of delivering.
os.environ.setdefault("TELEGRAM_CHAT_ID", "dry-run")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "dry-run")

print(f"===== DRY-RUN: {mode} — nothing sent, nothing written =====\n")
import alerts  # noqa: E402  (env must be set before import)

if mode == "thesis":
    # Needs the stock name as the 2nd arg: python dryrun.py thesis "Name (XNSE:SYM)"
    import thesis as _thesis
    if len(sys.argv) < 3:
        print('usage: python dryrun.py thesis "Company Name (XNSE:SYMBOL)" [reason]')
        sys.exit(1)
    _res = _thesis.generate(sys.argv[2], reason=(sys.argv[3] if len(sys.argv) > 3 else ""))
    print(_res["markdown"] if _res.get("status") == "OK"
          else f"INSUFFICIENT DATA — {_res.get('why')}")
elif mode == "fast-poll":
    alerts.run_fast_poll(minutes=0.05, interval=2)   # ~one live cycle
else:
    {
        "states": alerts.run_states,
        "filings": alerts.run_filings,
        "filings-nse": lambda: alerts.run_filings(nse_only=True),
        "filings-audit": alerts.run_filings_audit,
        "deals": alerts.run_deals,
        "digest": alerts.run_digest,
        "reconcile": alerts.run_reconcile,
        "morning-brief": alerts.run_morning_brief,
        "morning-levels": alerts.run_morning_levels,
        "eod-entries": alerts.run_eod_entries,
    }.get(mode, alerts.run_states)()

print("\n===== end dry-run =====")
