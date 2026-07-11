# Entry point for the Lakshmi household deployment.
# Streamlit Cloud needs a distinct main-file per app even from the same repo.
# app.py calls main() at module level, so importing it boots the dashboard.
# Which household this serves is decided by APP_TENANT in THIS app's secrets.
import app  # noqa: F401
