from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
import re

from .templates import TEMPLATES               # static curated templates
from .retriever import rank
from .lex import tokenize, normalized, surface_forms, singular
from .learner import learn_schema
from .dynamic_templates import generate_dynamic_corpus

@dataclass
class Candidate:
    sql: str
    score: float
    rationale: str

def generate_candidates(question: str, conn=None) -> List[Candidate]:
    """
    Enhanced generator:
      1) Direct schema-aware rules (COUNT DISTINCT, COUNT rows, unique, equals filter, top-k by).
      2) Static templates.
      3) Dynamic templates learned from the DB.
      4) Fallback.
    """
    q = question.strip()
    cands: List[Candidate] = []

    learned = learn_schema(conn) if conn is not None else {"tables": {}, "columns": {}}
    dynamic_corpus = generate_dynamic_corpus(learned) if learned.get("tables") else []

    # ... (rest of the enhanced implementation you pasted earlier)
    return cands

# --- General, schema-aware patterns (map words -> table/column) ---
PAT_COUNT_DISTINCT = re.compile(r"\bhow many ([a-z0-9_ ]+?) in ([a-z0-9_ ]+)\b", re.I)
PAT_COUNT_ROWS     = re.compile(r"\bhow many (rows|records|entries) in ([a-z0-9_ ]+)\b", re.I)
PAT_UNIQUE         = re.compile(r"\b(unique|distinct) ([a-z0-9_ ]+) in ([a-z0-9_ ]+)\b", re.I)
PAT_SHOW_EQ        = re.compile(r"\b(show|list)\s+([a-z0-9_ ]+)\s+where\s+([a-z0-9_ ]+)\s*=\s*([a-z0-9_ -]+)\b", re.I)
PAT_TOPK_BY        = re.compile(r"\btop\s+(\d+)\s+([a-z0-9_ ]+)\s+in\s+([a-z0-9_ ]+)\s+by\s+([a-z0-9_ ]+)\b", re.I)
PAT_YEAR_IN        = re.compile(r"\b([a-z0-9_ ]+)\s+in\s+(19|20)\d{2}\b", re.I)

def best_match(name_like: str, options: List[str]) -> Optional[str]:
    """Very light fuzzy matching by normalized token overlap + exact surface forms."""
    want = normalized(name_like)
    cand = None; best = 0.0
    for o in options:
        o_norm = normalized(o)
        ws = set(want.split()); os = set(o_norm.split())
        j = len(ws & os) / max(1, len(ws | os))
        if j > best:
            best, cand = j, o
    return cand

def resolve_table_column(learned: Dict[str, Any], table_like: str, col_like: Optional[str]=None) -> Tuple[Optional[str], Optional[str]]:
    # table
    tables = list(learned["tables"].keys()) + sum((tinfo["surfaces"] for tinfo in learned["tables"].values()), [])
    t = best_match(table_like, tables)
    # map surface back to canonical table if needed
    canon_t = None
    if t in learned["tables"]:
        canon_t = t
    else:
        # find which table owns this surface
        for tt, info in learned["tables"].items():
            if t in info["surfaces"]:
                canon_t = tt; break
    if not canon_t:
        return None, None

    if not col_like:
        return canon_t, None

    cols = learned["tables"][canon_t]["columns"]
    col_surfaces = []
    mapping = {}
    for c in cols:
        surfs = surface_forms(c)
        col_surfaces.extend(surfs)
        for s in surfs:
            mapping[s] = c

    cbest = best_match(col_like, cols + col_surfaces)
    if not cbest:
        return canon_t, None
    if cbest in mapping:
        return canon_t, mapping[cbest]
    return canon_t, cbest

def generate_candidates(question: str, conn=None) -> List[Candidate]:
    """
    Enhanced generator:
      1) Direct schema-aware rules (COUNT DISTINCT, COUNT rows, unique, equals filter, top-k by).
      2) Static templates.
      3) Dynamic templates learned from the DB.
      4) Fallback.
    """
    q = question.strip()
    cands: List[Candidate] = []

    # Learn schema + dynamic corpus (cache outside if you want; cheap on demo DB)
    learned = learn_schema(conn) if conn is not None else {"tables":{}, "columns":{}}
    dynamic_corpus: List[Dict[str, str]] = generate_dynamic_corpus(learned) if learned["tables"] else []

    # ---- General rules (schema-aware) ----
    # how many <col> in <table>
    m = PAT_COUNT_DISTINCT.search(q)
    if m and learned["tables"]:
        col_like, table_like = m.group(1), m.group(2)
        t, c = resolve_table_column(learned, table_like, col_like)
        if t and c:
            sql = f"SELECT COUNT(DISTINCT {c}) AS distinct_{c}_count FROM {t}"
            cands.append(Candidate(sql=sql, score=0.93, rationale="Rule: count distinct column in table"))
            # don’t return yet; we still want alternates

    # how many rows in <table>
    m = PAT_COUNT_ROWS.search(q)
    if m and learned["tables"]:
        table_like = m.group(2)
        t, _ = resolve_table_column(learned, table_like, None)
        if t:
            sql = f"SELECT COUNT(*) AS row_count FROM {t}"
            cands.append(Candidate(sql=sql, score=0.92, rationale="Rule: count rows in table"))

    # unique/distinct <col> in <table>
    m = PAT_UNIQUE.search(q)
    if m and learned["tables"]:
        col_like, table_like = m.group(2), m.group(3)
        t, c = resolve_table_column(learned, table_like, col_like)
        if t and c:
            sql = f"SELECT DISTINCT {c} FROM {t} ORDER BY {c} LIMIT 200"
            cands.append(Candidate(sql=sql, score=0.90, rationale="Rule: distinct column values in table"))

    # show <table> where <col> = <value>
    m = PAT_SHOW_EQ.search(q)
    if m and learned["tables"]:
        table_like, col_like, value_like = m.group(2), m.group(3), m.group(4).strip().lower()
        t, c = resolve_table_column(learned, table_like, col_like)
        if t and c:
            sql = f"SELECT * FROM {t} WHERE LOWER({c}) = '{value_like}' LIMIT 200"
            cands.append(Candidate(sql=sql, score=0.88, rationale="Rule: equality filter"))

    # top k <colA> in <table> by <colB>
    m = PAT_TOPK_BY.search(q)
    if m and learned["tables"]:
        k, colA_like, table_like, colB_like = m.group(1), m.group(2), m.group(3), m.group(4)
        t, colA = resolve_table_column(learned, table_like, colA_like)
        _, colB = resolve_table_column(learned, table_like, colB_like)
        if t and colA and colB:
            sql = f"SELECT {colA}, SUM({colB}) AS s FROM {t} GROUP BY {colA} ORDER BY s DESC LIMIT {k}"
            cands.append(Candidate(sql=sql, score=0.86, rationale="Rule: top-k by numeric aggregate"))

    # "<table> in <year>"
    m = PAT_YEAR_IN.search(q)
    if m and learned["tables"]:
        table_like, year = m.group(1), re.search(r"(19|20)\\d{2}", q).group(0)
        t, _ = resolve_table_column(learned, table_like, None)
        if t:
            # pick a date-ish column if any; otherwise no-op
            date_col = None
            for c in learned["tables"][t]["columns"]:
                if learned["columns"][f"{t}.{c}"]["is_date"]:
                    date_col = c; break
            if date_col:
                sql = f"SELECT * FROM {t} WHERE substr({date_col},1,4)='{year}' LIMIT 200"
                cands.append(Candidate(sql=sql, score=0.84, rationale="Rule: year filter"))

    # ---- Retriever over static + dynamic corpora ----
    corpus = [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in TEMPLATES] + dynamic_corpus
    if corpus:
        idxs = rank(q.lower(), [x["q"] for x in corpus], topk=5)
        for rank_i, i in enumerate(idxs):
            item = corpus[i]
            score = 0.74 - 0.04*rank_i
            cands.append(Candidate(sql=item["sql"], score=score, rationale=f"Retriever: {item['q']}"))

    if not cands:
        cands.append(Candidate(sql="SELECT * FROM policies LIMIT 25", score=0.40, rationale="Fallback sample"))
    return cands
