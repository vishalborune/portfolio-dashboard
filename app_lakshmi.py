# Entry point for the Lakshmi household deployment.
# Streamlit re-executes THIS file on every interaction (rerun). `import app`
# only executes app.py's body on the FIRST run (Python caches modules), so we
# must call main() explicitly - that's what renders the page on every rerun.
# Which household this serves is decided by APP_TENANT in THIS app's secrets.
import app

app.main()
