
# Legacy Assistant — Dissertation Demo
See instructions inside the archive root after extraction.
Install:
  python -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  pip install -e .
Run CLI:
  python cli.py -q "how many policies are active"
Run UI:
  streamlit run apps/streamlit_app.py
