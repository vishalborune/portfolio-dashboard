"""
notify.py — delivery pipes for the alert engine.

Telegram: real-time alerts (state changes, filings, add signals).
Email (Resend): the Sunday digest only.

Required env vars:
  TELEGRAM_BOT_TOKEN   from @BotFather
  TELEGRAM_CHAT_ID     the group chat id (negative number for groups)
Optional (digest):
  RESEND_API_KEY       from resend.com
  DIGEST_EMAILS        comma-separated recipients
  DIGEST_FROM          verified sender, default onboarding@resend.dev
"""

import os
import requests

TG_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_telegram(text: str) -> bool:
    """Send a message to the configured Telegram group. Returns success."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("⚠️ Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing)")
        return False
    try:
        r = requests.post(
            TG_API.format(token=token),
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if r.status_code != 200:
            print(f"⚠️ Telegram send failed: {r.status_code} {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"⚠️ Telegram send error: {e}")
        return False


def send_email(subject: str, html: str) -> bool:
    """Send the digest email via Resend. Returns success."""
    api_key = os.environ.get("RESEND_API_KEY")
    to = os.environ.get("DIGEST_EMAILS", "")
    if not api_key or not to:
        print("⚠️ Email not configured (RESEND_API_KEY / DIGEST_EMAILS missing)")
        return False
    recipients = [e.strip() for e in to.split(",") if e.strip()]
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={
                "from": os.environ.get("DIGEST_FROM", "Portfolio <onboarding@resend.dev>"),
                "to": recipients,
                "subject": subject,
                "html": html,
            },
            timeout=20,
        )
        if r.status_code not in (200, 201):
            print(f"⚠️ Email send failed: {r.status_code} {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"⚠️ Email send error: {e}")
        return False
