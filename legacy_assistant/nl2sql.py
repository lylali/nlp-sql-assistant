# legacy_assistant/nl2sql.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import re

from .templates import TEMPLATES
from .retriever import rank
from .learner import learn_schema
from .dynamic_templates import generate_dynamic_corpus
from .feedback_learn import load_user_corpus
from .nlp import keywords
from .predictor import score_table_column, predict_filters, predict_numbers

@dataclass
class Candidate:
    sql: str
    score: float
    rationale: str

# General patterns
RE_COUNT_DISTINCT = re.compile(r"\bhow many ([a-z0-9_ ]+?) in ([a-z0-9_ ]+)\b", re.I)
RE_COUNT_ROWS     = re.compile(r"\bhow many (rows|records|entries) in ([a-z0-9_ ]+)\b", re.I)
RE_UNIQUE         = re.compile(r"\b(unique|distinct) ([a-z0-9_ ]+) in ([a-z0-9_ ]+)\b", re.I)
RE_TOPK_IN_BY     = re.compile(r"\btop\s+(\d+)\s+([a-z0-9_ ]+)\s+in\s+([a-z0-9_ ]+)\s+by\s+([a-z0-9_ ]+)\b", re.I)
RE_SHOW_LIST      = re.compile(r"\b(show|list)\b", re.I)

def _ensure_limit(sql: str, n: int = 200) -> str:
    """
    Ensure SELECT statements have at most one LIMIT.
    - Do NOT append a LIMIT if the SQL already ends with LIMIT or if it's an aggregate (COUNT, SUM, AVG).
    - Strips duplicate LIMITs.
    """
    s = sql.strip().rstrip(";")
    lower = s.lower()
    # if already has a LIMIT (anywhere), don't add another
    if " limit " in lower or lower.endswith(" limit"):
        return s
    # if it's an aggregate (count/sum/avg), LIMIT is not needed
    if any(fn in lower for fn in ["count(", "sum(", "avg("]):
        return s
    # append LIMIT safely
    return f"{s}\nLIMIT {n}"

def generate_candidates(question: str, conn=None) -> List[Candidate]:
    q = question.strip()
    cands: List[Candidate] = []

    learned = learn_schema(conn) if conn is not None else {"tables":{}, "columns":{}}
    dynamic_corpus: List[Dict[str, str]] = generate_dynamic_corpus(learned) if learned["tables"] else []
    user_corpus: List[Dict[str,str]] = load_user_corpus()

    toks = keywords(q)

    # 1) Schema-aware rules
    m = RE_COUNT_DISTINCT.search(q)
    if m and learned["tables"]:
        col_like, tab_like = m.group(1), m.group(2)
        t, c, sc = score_table_column(learned, keywords(col_like) + keywords(tab_like))
        if t and c:
            cands.append(Candidate(sql=f"SELECT COUNT(DISTINCT {c}) AS distinct_{c}_count FROM {t}",
                                   score=0.95, rationale="Rule: count distinct column in table"))

    m = RE_COUNT_ROWS.search(q)
    if m and learned["tables"]:
        tab_like = m.group(2)
        t, _, sc = score_table_column(learned, keywords(tab_like))
        if t:
            cands.append(Candidate(sql=f"SELECT COUNT(*) AS row_count FROM {t}",
                                   score=0.94, rationale="Rule: count rows in table"))

    m = RE_UNIQUE.search(q)
    if m and learned["tables"]:
        col_like, tab_like = m.group(2), m.group(3)
        t, c, sc = score_table_column(learned, keywords(col_like) + keywords(tab_like))
        if t and c:
            cands.append(Candidate(sql=_ensure_limit(f"SELECT DISTINCT {c} FROM {t} ORDER BY {c}"),
                                   score=0.92, rationale="Rule: unique/distinct values"))

    m = RE_TOPK_IN_BY.search(q)
    if m and learned["tables"]:
        k, colA_like, tab_like, colB_like = m.groups()
        tA, colA, _ = score_table_column(learned, keywords(colA_like)+keywords(tab_like))
        tB, colB, _ = score_table_column(learned, keywords(colB_like)+keywords(tab_like))
        if tA and tB and tA == tB and colA and colB:
            sql = f"SELECT {colA}, SUM({colB}) AS s FROM {tA} GROUP BY {colA} ORDER BY s DESC LIMIT {int(k)}"
            cands.append(Candidate(sql=sql, score=0.90, rationale="Rule: top-K by aggregate"))

    # Generic show/list with inferred filters and year/limit
    if RE_SHOW_LIST.search(q) and learned["tables"]:
        # predict a plausible (table, column), then add filters
        t, c, sc = score_table_column(learned, toks)
        if t:
            where = []
            # equality filters from values mentioned
            for (ft, fc, fv) in predict_filters(learned, q):
                if ft == t:
                    where.append(f"LOWER({fc}) = '{fv}'")
            # year if present -> pick a date-looking column
            k, year = predict_numbers(q)
            if year:
                date_col = None
                for col in learned["tables"][t]["columns"]:
                    if learned["columns"][f"{t}.{col}"]["is_date"]:
                        date_col = col; break
                if date_col:
                    where.append(f"substr({date_col},1,4)='{year}'")
            where_sql = (" WHERE " + " AND ".join(where)) if where else ""
            sql = f"SELECT * FROM {t}{where_sql}"
            cands.append(Candidate(sql=_ensure_limit(sql), score=0.88, rationale="Rule: show/list with inferred filters"))

    # 2) Retriever over static + dynamic + user corpora
    corpus = [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in TEMPLATES] + dynamic_corpus + user_corpus
    if corpus:
        idxs = rank(q.lower(), [x["q"] for x in corpus], topk=5)
        for rank_i, i in enumerate(idxs):
            item = corpus[i]
            cands.append(Candidate(sql=_ensure_limit(item["sql"]), score=0.78 - 0.04*rank_i, rationale=f"Retriever: {item['q']}"))

    if not cands:
        cands.append(Candidate(sql="SELECT * FROM policies LIMIT 25", score=0.40, rationale="Fallback sample"))
    return cands
