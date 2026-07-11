# Entry point for the Lakshmi household deployment.
# Streamlit Cloud requires a distinct main-file per app, even from the same
# repo — this wrapper runs the exact same dashboard code as app.py.
# Which household it serves is decided by APP_TENANT in this app's secrets.
import app  # noqa: F401
