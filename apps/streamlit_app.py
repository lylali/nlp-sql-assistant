import re
import streamlit as st, pandas as pd
from datetime import datetime
from legacy_assistant.config import AppConfig
from legacy_assistant.db import create_demo_connection, run_sql, schema_introspect
from legacy_assistant.nl2sql import generate_candidates
from legacy_assistant.feedback import record_feedback
from legacy_assistant.feedback_learn import ingest_feedback_to_corpus
from legacy_assistant.active import active_priority
from legacy_assistant.templates import TEMPLATES
from legacy_assistant.feedback_learn import load_user_corpus, load_patterns
from legacy_assistant.dynamic_templates import generate_dynamic_corpus
from legacy_assistant.learner import learn_schema

st.set_page_config(page_title="SQL Assistant", layout="wide")

@st.cache_resource
def get_conn():
    cfg=AppConfig()
    return create_demo_connection(n_policies=120, n_claims=80)

conn=get_conn()
conn = create_demo_connection(n_policies=120, n_claims=80)
schema=schema_introspect(conn)
learned = learn_schema(conn)
_dyn = generate_dynamic_corpus(learned) if learned.get("tables") else []
_user = load_user_corpus()                       # [{"q","sql","count"}]
_pats = load_patterns() 

# Materialise a few pattern variants (kept tiny)
_pat_mat = []
for p in _pats[:50]:
    qpat = p.get("q_pat","")
    if qpat:
        _pat_mat.append({"q": qpat.replace("{K}","10").replace("{YEAR}","2024")})

_corpus_qs = [x["q"] for x in TEMPLATES] \
           + [x["q"] for x in _dyn] \
           + [x["q"] for x in _user] \
           + [x["q"] for x in _pat_mat]


def implied_limit_from_question(q: str) -> int | None:
    ql = q.lower()
    m = re.search(r"\btop\s+(\d+)\b", ql)
    if m: return int(m.group(1))
    m = re.search(r"\bfirst\s+(\d+)\b", ql)
    if m: return int(m.group(1))
    m = re.search(r"\blimit\s+(\d+)\b", ql)
    if m: return int(m.group(1))
    return None

with st.sidebar:
    st.header("Schema (demo)")

    # If schema came from schema_introspect 
    if isinstance(schema, dict) and "tables" in schema:
        for t in sorted(schema["tables"].keys()):
            cols = schema["tables"][t].get("columns", [])
            with st.expander(f"{t} ({len(cols)})", expanded=False):
                st.write(", ".join(cols) if cols else "—")

    # Backward-compat: if schema is the old {table: [cols]} dict
    elif isinstance(schema, dict):
        for t in sorted(schema.keys()):
            cols = schema.get(t, [])
            with st.expander(f"{t} ({len(cols)})", expanded=False):
                st.write(", ".join(cols) if cols else "—")

    else:
        st.write("No schema available.")


with st.sidebar:
    if st.button("🔁 Learn new feedback now"):
        new_items, total = ingest_feedback_to_corpus()
        if new_items:
            st.success(f"Learned {new_items} new feedback item(s). Corpus size: {total}.")
        else:
            st.info("No new feedback to learn.")

st.title("SQL Assistant")
q=st.text_input("Question", value="How many policies are active right now?")

if st.button("Generate SQL"):
    st.session_state["cands"] = generate_candidates(q, conn=conn)
    priority = active_priority(q, st.session_state["cands"], [it["q"] for it in TEMPLATES])
    if priority >= 0.6:
        st.info(f"This query looks ambiguous (active-learn priority {priority:.2f}). "
                "If you correct the SQL below, I'll learn from it and improve future answers.")
    # When feedback submitted:
    # - call ingest_feedback_to_corpus(); 
    # - optionally display how many templates/patterns/synonyms updated.
    changed, total = ingest_feedback_to_corpus()
    if changed:
        st.toast(f"Updated {changed} feedback entries. Corpus size: {total}.", icon="✅")


    # set default row-limit based on the question, if implied
    imp = implied_limit_from_question(q)
    if imp:
        st.session_state["row_limit_default"] = int(imp)

cands=st.session_state.get("cands")

left,right=st.columns([1.2,1.8])

with left:
    st.subheader("Candidates")
    if not cands:
        st.info("Enter a question and click Generate SQL.")
    else:
        opts=[f"Candidate {i+1} (score {c.score:.2f}) — {c.rationale}" for i,c in enumerate(cands)]
        choice=st.radio("Pick one to run", list(range(len(cands))), format_func=lambda i: opts[i])
        sql=cands[choice].sql; st.code(sql, language="sql"); st.session_state["sql_to_run"]=sql

with st.expander("NLP debug (spaCy)", expanded=False):
    from legacy_assistant.nlp import entities, keywords
    st.write("Keywords:", keywords(q))
    st.write("Entities:", entities(q))


with right:
    st.subheader("Result")
    default_limit = int(st.session_state.get("row_limit_default", AppConfig().row_limit_default))
    row_limit = st.number_input(
    "Row limit (applied only if SQL has no LIMIT)",
    min_value=10, max_value=50000, value=default_limit, step=10
)

    if st.button("Run query"):
        sql = st.session_state.get("sql_to_run")

        if not sql:
            st.warning("Generate and select a candidate first.")
        else:
            # Only add a LIMIT if none exists already
            has_limit = bool(re.search(r"(?i)\blimit\s+\d+\b", sql or ""))

            cols, rows = run_sql(conn, sql, row_limit=None if has_limit else int(row_limit))

            if cols and "error" in cols:
                st.error(rows[0][0])
                st.code(rows[0][1], language="sql")

            elif cols:
                df = pd.DataFrame(rows, columns=cols)
                st.dataframe(df, use_container_width=True)

                # --- CSV download button (no charts) ---
                if not df.empty:
                    # build a safe filename from the question or timestamp
                    q_text = (st.session_state.get("question") or "query").strip().lower()
                    slug = re.sub(r"[^a-z0-9]+", "-", q_text).strip("-")[:60] or "query"
                    fname = f"{slug}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"

                    csv_bytes = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Download CSV",
                        data=csv_bytes,
                        file_name=fname,
                        mime="text/csv",
                        help="Download the current table as a CSV file."
                    )
                else:
                    st.info("No rows returned.")



    st.markdown("---"); st.subheader("Feedback")
    correct=st.checkbox("Mark as correct"); corrected_sql=st.text_area("Corrected SQL (optional)", height=100); note=st.text_input("Note (optional)")
    if st.button("Submit feedback"):
        sql=st.session_state.get("sql_to_run","")
        record_feedback(q, sql, correct=bool(correct), corrected_sql=corrected_sql or None, note=note or None)
        st.success("Feedback saved.")


