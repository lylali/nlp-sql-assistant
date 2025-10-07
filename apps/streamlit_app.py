import streamlit as st, pandas as pd
from legacy_assistant.config import AppConfig
from legacy_assistant.db import create_demo_connection, run_sql, schema_introspect
from legacy_assistant.nl2sql import generate_candidates
from legacy_assistant.feedback import record_feedback
from legacy_assistant.feedback_learn import ingest_feedback_to_corpus

st.set_page_config(page_title="Legacy Assistant (Demo)", layout="wide")

@st.cache_resource
def get_conn():
    cfg=AppConfig()
    # IMPORTANT: make SQLite cross-thread safe for Streamlit
    import sqlite3
    conn = create_demo_connection(":memory:", check_same_thread=False)
    return conn

conn=get_conn()
schema=schema_introspect(conn)

with st.sidebar:
    st.header("Schema (demo)")
    for t, cols in schema.items():
        with st.expander(t): st.write(", ".join(cols))

st.title("Legacy Database Assistant — Demo Only (SQLite)")
q=st.text_input("Question", value="How many policies are active right now?")

if st.button("Generate SQL"):
    # (Re)learn any new feedback into user corpus before generating
    added = ingest_feedback_to_corpus()
    if added:
        st.toast(f"Learned {added} new template(s) from feedback.", icon="✅")
    st.session_state["cands"] = generate_candidates(q, conn=conn)

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

with right:
    st.subheader("Result")
    row_limit=st.number_input("Row limit (safety)", min_value=10, max_value=5000, value=AppConfig().row_limit_default, step=10)
    if st.button("Run query"):
        sql=st.session_state.get("sql_to_run")
        if not sql:
            st.warning("Generate and select a candidate first.")
        else:
            cols,rows=run_sql(conn, sql, row_limit=int(row_limit))
            if cols and "error" in cols:
                st.error(rows[0][0]); st.code(rows[0][1], language="sql")
            elif cols:
                df=pd.DataFrame(rows, columns=cols); st.dataframe(df, use_container_width=True)
                if len(df.columns)>=2: st.bar_chart(df.set_index(df.columns[0])[df.columns[1]])

    st.markdown("---"); st.subheader("Feedback")
    correct=st.checkbox("Mark as correct"); corrected_sql=st.text_area("Corrected SQL (optional)", height=100); note=st.text_input("Note (optional)")
    if st.button("Submit feedback"):
        sql=st.session_state.get("sql_to_run","")
        record_feedback(q, sql, correct=bool(correct), corrected_sql=corrected_sql or None, note=note or None)
        st.success("Feedback saved.")
