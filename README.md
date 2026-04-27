# Portfolio Dashboard

Real-time NSE/BSE portfolio tracker. Pulls live prices, tracks unrealised + realised P&L, and lets you collaborate with someone on watchlists and buy/sell notes — all backed by a Google Sheet you both edit.

## What this gives you

- **Live P&L** — current value, day's change, allocation %, sector mix, all auto-computed from yfinance
- **Long-run tracking** — daily snapshots build an equity curve over time
- **Realised P&L analytics** — win rate, best/worst trades, average per-trade P&L
- **Collaborative watchlist + notes** — your friend edits the Google Sheet, dashboard reflects it within ~2 minutes
- **Shareable URL** — one link, optional password protection

Everything runs on free infrastructure. Total monthly cost: ₹0.

---

## Setup — 15 minutes, one time

### Step 1: Get your data into Google Sheets

1. Open `Portfolio_Template.xlsx` (already populated with your 17 holdings + 15 realised trades)
2. Go to [sheets.google.com](https://sheets.google.com) → **File → Import → Upload** the template
3. Choose **Replace spreadsheet** when prompted
4. You now have 4 tabs: `Holdings`, `Realised`, `Watchlist`, `Notes`

### Step 2: Share the sheet

1. Click **Share** (top-right)
2. Set **General access** → "Anyone with the link" → **Viewer**
   _(this is what the dashboard reads from — keep it Viewer for safety)_
3. Add your friend's email as **Editor** so they can update Watchlist and Notes
4. Copy the share URL — you'll paste it into the dashboard

That's it for collaboration. Your friend edits Watchlist + Notes directly in Sheets, you both see live P&L in the dashboard.

### Step 3: Deploy the dashboard (free, 5 mins)

**Option A — Streamlit Community Cloud (recommended)**

1. Push the `portfolio-dashboard` folder to a **private** GitHub repo
2. Go to [share.streamlit.io](https://share.streamlit.io) → sign in with GitHub
3. **New app** → pick the repo → main file: `app.py` → **Deploy**
4. Once live, go to **Settings → Secrets** and add:
   ```toml
   PORTFOLIO_PASSWORD = "your-password-here"
   DEFAULT_SHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
   ```
5. Share the dashboard URL with your friend — they enter the password and they're in

**Option B — Run locally first to test**

```bash
cd portfolio-dashboard
pip install -r requirements.txt
streamlit run app.py
```

Open [localhost:8501](http://localhost:8501), paste your Sheet URL in the sidebar.

---

## How it works day-to-day

**You (manual updates):**
- New buy → add a row to the `Holdings` tab in Google Sheets
- Stock sold → move the row from `Holdings` to `Realised` and fill in sale price + date

**Your friend:**
- Suggesting a stock → add to `Watchlist` tab with a target buy price + reasoning
- General market view → drop a note in the `Notes` tab

**Dashboard:**
- Refreshes prices every 5 minutes automatically
- Hit **🔄 Force refresh** in the sidebar for an instant update
- Hit **📸 Take snapshot** on the History tab once a day (or schedule it) to build your equity curve

---

## Stock name format — important

Every stock name must end with the exchange code in parentheses. This is how the dashboard finds the right ticker.

- NSE: `COMPANY NAME (XNSE:SYMBOL)` → e.g. `WEBSOL ENERGY SYSTEM LIMITED (XNSE:WEBELSOLAR)`
- BSE: `COMPANY NAME (XBOM:NUMBER)` → e.g. `KWALITY PHARMACEUTICALS LIMITED (XBOM:539997)`

Your existing format already follows this — no changes needed. Just keep the same convention for new additions.

---

## File map

| File | Purpose |
|---|---|
| `app.py` | The full Streamlit dashboard (one file, ~500 lines) |
| `requirements.txt` | Python deps — pip installs everything in 30s |
| `Portfolio_Template.xlsx` | Your data, cleaned, with 4 tabs ready for Sheets import |
| `snapshots.csv` | Auto-generated when you take your first snapshot |

---

## Roadmap (when you want more)

- **Auto-snapshot daily** via GitHub Actions cron (no manual button needed)
- **Telegram alerts** when watchlist stocks hit target buy price
- **Sector concentration warnings** (e.g. flag if any sector > 30%)
- **Tax computation** for realised P&L (STCG vs LTCG split)

Ping me when ready to add any of these.

---

**Next step:** Upload `Portfolio_Template.xlsx` to Google Sheets, copy the share URL, and either deploy on Streamlit Cloud or run locally with `streamlit run app.py`. You'll have live P&L in under 15 minutes.
