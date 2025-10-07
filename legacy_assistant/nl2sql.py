# legacy_assistant/nl2sql.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import re, math

from .templates import TEMPLATES
from .retriever import rank
from .learner import learn_schema
from .dynamic_templates import generate_dynamic_corpus
from .feedback_learn import load_user_corpus
from .nlp import keywords
# predictor helpers used inside generate_candidates (imported here for clarity)
from .predictor import score_table_column, predict_filters, predict_numbers


@dataclass
class Candidate:
    sql: str
    score: float
    rationale: str


# ---------------------------
# Generic query pattern regex
# ---------------------------
RE_COUNT_DISTINCT = re.compile(r"\bhow many ([a-z0-9_ ]+?) in ([a-z0-9_ ]+)\b", re.I)
RE_COUNT_ROWS     = re.compile(r"\bhow many (rows|records|entries) in ([a-z0-9_ ]+)\b", re.I)
RE_UNIQUE         = re.compile(r"\b(unique|distinct) ([a-z0-9_ ]+) in ([a-z0-9_ ]+)\b", re.I)
RE_TOPK_IN_BY     = re.compile(r"\btop\s+(\d+)\s+([a-z0-9_ ]+)\s+in\s+([a-z0-9_ ]+)\s+by\s+([a-z0-9_ ]+)\b", re.I)
RE_SHOW_LIST      = re.compile(r"\b(show|list)\b", re.I)
RE_YEAR_IN        = re.compile(r"\b([a-z0-9_ ]+)\s+in\s+(19|20)\d{2}\b", re.I)

# ---------------------------
# SQL parsing helpers (light)
# ---------------------------
SQL_TABLE_RE = re.compile(r'\bFROM\s+([a-z_][\w]*)', re.I)
SQL_JOIN_RE  = re.compile(r'\bJOIN\s+([a-z_][\w]*)', re.I)
SQL_DISTINCT_COL_RE = re.compile(r'\bSELECT\s+DISTINCT\s+([a-z_][\w]*)', re.I)

def _sql_tables(sql: str) -> set[str]:
    """Extract referenced tables from FROM/JOIN."""
    return set(SQL_TABLE_RE.findall(sql)) | set(SQL_JOIN_RE.findall(sql))

def _sql_distinct_cols(sql: str) -> set[str]:
    """Extract columns used in SELECT DISTINCT."""
    return set(SQL_DISTINCT_COL_RE.findall(sql))

# ---------------------------
# Column picking for DISTINCT
# ---------------------------
ID_LIKE = re.compile(r'(^|_)(id|number)$', re.I)

def _pick_distinct_column(learned: dict, table: str, col_like_phrase: str) -> str:
    """
    Choose a good column for DISTINCT queries:
      1) exact normalized match to the phrase if present
      2) avoid id/number-like columns
      3) prefer categorical columns (fewer distinct samples as proxy)
      4) fallback to first column
    """
    cols = learned["tables"][table]["columns"]
    samples = learned["tables"][table]["samples"]

    # exact normalized match
    want = " ".join(col_like_phrase.strip().lower().replace("_", " ").split())
    for c in cols:
        cn = " ".join(c.lower().replace("_", " ").split())
        if cn == want:
            return c

    # avoid id-like
    non_id = [c for c in cols if not ID_LIKE.search(c)] or cols[:]

    # prefer categorical (by small distinct sample heuristic)
    def cat_key(c: str):
        return (len(samples.get(c, [])) or 10_000, c)

    non_id.sort(key=cat_key)
    return non_id[0] if non_id else cols[0]


# ---------------------------
# Utility
# ---------------------------
def _ensure_limit(sql: str, n: int = 200) -> str:
    s = sql.strip().rstrip(";")
    if s.lower().startswith("select") and " limit " not in s.lower():
        s += f"\nLIMIT {n}"
    return s

def _dedupe_keep_best(cands: List[Candidate]) -> List[Candidate]:
    """Deduplicate by exact SQL; keep highest score."""
    best: Dict[str, Candidate] = {}
    for c in cands:
        key = c.sql.strip()
        if key not in best or c.score > best[key].score:
            best[key] = c
    return sorted(best.values(), key=lambda x: (-x.score, x.rationale))


# ---------------------------
# Main entry
# ---------------------------
def generate_candidates(question: str, conn=None) -> List[Candidate]:
    q = question.strip()
    cands: List[Candidate] = []

    # Learn schema + build corpora
    learned = learn_schema(conn) if conn is not None else {"tables": {}, "columns": {}}
    dynamic_corpus: List[Dict[str, str]] = generate_dynamic_corpus(learned) if learned.get("tables") else []
    user_corpus: List[Dict[str, str]] = load_user_corpus()

    toks = keywords(q)

    # ---------- Schema-aware rules (high-priority, robust) ----------

    # UNIQUE/DISTINCT <col> IN <table>
    m = RE_UNIQUE.search(q)
    if m and learned.get("tables"):
        col_like, tab_like = m.group(2), m.group(3)
        t, _, _ = score_table_column(learned, keywords(col_like) + keywords(tab_like))
        if t:
            c = _pick_distinct_column(learned, t, col_like)
            cands.append(
                Candidate(
                    sql=_ensure_limit(f"SELECT DISTINCT {c} FROM {t} ORDER BY {c}"),
                    score=0.99,  # ensure top priority over retriever
                    rationale="Rule: unique/distinct (resolved table/column with non-ID/categorical preference)",
                )
            )

    # HOW MANY <col> IN <table>  -> COUNT(DISTINCT col)
    m = RE_COUNT_DISTINCT.search(q)
    if m and learned.get("tables"):
        col_like, tab_like = m.group(1), m.group(2)
        t, c, _ = score_table_column(learned, keywords(col_like) + keywords(tab_like))
        if t and c:
            cands.append(
                Candidate(
                    sql=f"SELECT COUNT(DISTINCT {c}) AS distinct_{c}_count FROM {t}",
                    score=0.95,
                    rationale="Rule: count distinct column in table",
                )
            )

    # HOW MANY ROWS IN <table>    -> COUNT(*)
    m = RE_COUNT_ROWS.search(q)
    if m and learned.get("tables"):
        tab_like = m.group(2)
        t, _, _ = score_table_column(learned, keywords(tab_like))
        if t:
            cands.append(
                Candidate(
                    sql=f"SELECT COUNT(*) AS row_count FROM {t}",
                    score=0.94,
                    rationale="Rule: count rows in table",
                )
            )

    # TOP K <colA> IN <table> BY <colB>  -> GROUP BY colA ORDER BY SUM(colB)
    m = RE_TOPK_IN_BY.search(q)
    if m and learned.get("tables"):
        k, colA_like, tab_like, colB_like = m.groups()
        tA, colA, _ = score_table_column(learned, keywords(colA_like) + keywords(tab_like))
        tB, colB, _ = score_table_column(learned, keywords(colB_like) + keywords(tab_like))
        if tA and tB and tA == tB and colA and colB:
            sql = f"SELECT {colA}, SUM({colB}) AS s FROM {tA} GROUP BY {colA} ORDER BY s DESC LIMIT {int(k)}"
            cands.append(Candidate(sql=sql, score=0.90, rationale="Rule: top-K by aggregate"))

    # "<table> in <year>"  (choose a date-like column if available)
    m = RE_YEAR_IN.search(q)
    if m and learned.get("tables"):
        tab_like, year = m.group(1), re.search(r"(19|20)\d{2}", q).group(0)
        t, _, _ = score_table_column(learned, keywords(tab_like))
        if t:
            date_col = None
            for col in learned["tables"][t]["columns"]:
                if learned["columns"][f"{t}.{col}"]["is_date"]:
                    date_col = col
                    break
            if date_col:
                cands.append(
                    Candidate(
                        sql=_ensure_limit(f"SELECT * FROM {t} WHERE substr({date_col},1,4)='{year}'"),
                        score=0.88,
                        rationale="Rule: year filter",
                    )
                )

    # SHOW/LIST ...  -> infer table, equality filters, year, and add LIMIT
    if RE_SHOW_LIST.search(q) and learned.get("tables"):
        t, c, _ = score_table_column(learned, toks)
        if t:
            where = []
            # equality filters from value mentions (value index)
            for (ft, fc, fv) in predict_filters(learned, q):
                if ft == t:
                    where.append(f"LOWER({fc}) = '{fv}'")
            # year
            k_val, year = predict_numbers(q)
            if year:
                date_col = None
                for col in learned["tables"][t]["columns"]:
                    if learned["columns"][f"{t}.{col}"]["is_date"]:
                        date_col = col
                        break
                if date_col:
                    where.append(f"substr({date_col},1,4)='{year}'")
            where_sql = (" WHERE " + " AND ".join(where)) if where else ""
            cands.append(
                Candidate(
                    sql=_ensure_limit(f"SELECT * FROM {t}{where_sql}"),
                    score=0.88,
                    rationale="Rule: show/list with inferred filters",
                )
            )

    # ---------- Retriever over static + dynamic + user corpora ----------
    # Build combined corpus with frequency counts for user templates
    corpus = [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": 1} for x in TEMPLATES]
    corpus += [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": 1} for x in dynamic_corpus]
    corpus += [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": int(x.get("count", 1))} for x in user_corpus]

    # Predict (table, column) once for compatibility scoring
    t_pred, c_pred, _ = score_table_column(learned, toks) if learned.get("tables") else (None, None, 0.0)

    if corpus:
        idxs = rank(q.lower(), [x["q"] for x in corpus], topk=8)
        for rank_i, i in enumerate(idxs):
            item = corpus[i]
            base = 0.78 - 0.04 * rank_i
            # small frequency boost for user-learned items
            boost = min(0.12, 0.04 * math.log1p(item.get("count", 1)))

            # compatibility with predicted table/column
            tables = _sql_tables(item["sql"])
            dcols  = _sql_distinct_cols(item["sql"])
            compat = 0.0
            if t_pred and t_pred in tables:
                compat += 0.08
            if c_pred and (c_pred in dcols or any(c_pred in seg for seg in dcols)):
                compat += 0.05

            # penalty if predicted table disagrees
            penalty = 0.0
            if t_pred and tables and t_pred not in tables:
                penalty -= 0.15

            score = base + boost + compat + penalty
            cands.append(
                Candidate(
                    sql=_ensure_limit(item["sql"]),
                    score=score,
                    rationale=f"Retriever: {item['q']} (freq×{item.get('count',1)})",
                )
            )

    # ---------- Fallback + dedupe ----------
    if not cands:
        cands.append(Candidate(sql="SELECT * FROM policies LIMIT 25", score=0.40, rationale="Fallback sample"))

    cands = _dedupe_keep_best(cands)
    return cands
