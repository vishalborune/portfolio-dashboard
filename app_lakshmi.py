# Entry point for the Lakshmi household deployment.
# Streamlit Cloud needs a distinct main-file per app even from the same repo.
# This executes app.py exactly as if it were run directly, so the full
# dashboard boots. Which household it serves is decided by APP_TENANT in
# THIS app's Streamlit secrets (set APP_TENANT = "lakshmi").
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).parent / "app.py"), run_name="__main__")
