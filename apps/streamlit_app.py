import re
import streamlit as st, pandas as pd
from legacy_assistant.config import AppConfig
from legacy_assistant.db import create_demo_connection, run_sql, schema_introspect
from legacy_assistant.nl2sql import generate_candidates
from legacy_assistant.feedback import record_feedback
from legacy_assistant.feedback_learn import ingest_feedback_to_corpus
from legacy_assistant.active import active_priority
from legacy_assistant.feedback_learn import ingest_feedback_to_corpus

st.set_page_config(page_title="SQL Assistant", layout="wide")

@st.cache_resource
def get_conn():
    cfg=AppConfig()
    return create_demo_connection(cfg.demo_rows_policies, cfg.demo_rows_claims)

conn=get_conn()
schema=schema_introspect(conn)

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
    for t, cols in schema.items():
        with st.expander(t): st.write(", ".join(cols))

st.title("SQL Assistant")
q=st.text_input("Question", value="How many policies are active right now?")

if st.button("Generate SQL"):
    # (Re)learn any new feedback into user corpus before generating
    added = ingest_feedback_to_corpus()
    if added:
        st.toast(f"Learned {added} new template(s) from feedback.", icon="✅")
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
            has_limit = " limit " in sql.lower()
            cols, rows = run_sql(conn, sql, row_limit=None if has_limit else int(row_limit))
            if cols and "error" in cols:
                st.error(rows[0][0]); st.code(rows[0][1], language="sql")
            elif cols:
                df = pd.DataFrame(rows, columns=cols)
                st.dataframe(df, use_container_width=True)
                if len(df.columns) >= 2:
                    st.bar_chart(df.set_index(df.columns[0])[df.columns[1]])


    st.markdown("---"); st.subheader("Feedback")
    correct=st.checkbox("Mark as correct"); corrected_sql=st.text_area("Corrected SQL (optional)", height=100); note=st.text_input("Note (optional)")
    if st.button("Submit feedback"):
        sql=st.session_state.get("sql_to_run","")
        record_feedback(q, sql, correct=bool(correct), corrected_sql=corrected_sql or None, note=note or None)
        st.success("Feedback saved.")
