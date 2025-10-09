# legacy_assistant/nl2sql.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import re, math

# Core components
from .templates import TEMPLATES
from .retriever import rank
from .learner import learn_schema
from .dynamic_templates import generate_dynamic_corpus
from .feedback_learn import load_user_corpus
from .nlp import keywords
from .predictor import score_table_column, predict_filters, predict_numbers
from .paraphrase import paraphrase_questions
from .pmi import build_pmi
from .joins import two_table_candidates, synthesize_join_templates


# ---------------------------
# Data structure
# ---------------------------
@dataclass
class Candidate:
    sql: str
    score: float
    rationale: str


# ---------------------------
# Regex patterns (queries)
# ---------------------------
RE_COUNT_DISTINCT = re.compile(r"\bhow many ([a-z0-9_ ]+?) in ([a-z0-9_ ]+)\b", re.I)
RE_COUNT_ROWS     = re.compile(r"\bhow many (rows|records|entries) in ([a-z0-9_ ]+)\b", re.I)
RE_UNIQUE         = re.compile(r"\b(unique|distinct) ([a-z0-9_ ]+) in ([a-z0-9_ ]+)\b", re.I)
RE_TOPK_IN_BY     = re.compile(r"\btop\s+(\d+)\s+([a-z0-9_ ]+)\s+in\s+([a-z0-9_ ]+)\s+by\s+([a-z0-9_ ]+)\b", re.I)
RE_SHOW_LIST      = re.compile(r"\b(show|list)\b", re.I)
RE_YEAR_IN        = re.compile(r"\b([a-z0-9_ ]+)\s+in\s+(19|20)\d{2}\b", re.I)

# ---------------------------
# Light SQL parsing helpers
# ---------------------------
SQL_TABLE_RE        = re.compile(r'\bFROM\s+([a-z_][\w]*)', re.I)
SQL_JOIN_RE         = re.compile(r'\bJOIN\s+([a-z_][\w]*)', re.I)
SQL_DISTINCT_COL_RE = re.compile(r'\bSELECT\s+DISTINCT\s+([a-z_][\w]*)', re.I)

def _sql_tables(sql: str) -> set[str]:
    return set(SQL_TABLE_RE.findall(sql)) | set(SQL_JOIN_RE.findall(sql))

def _sql_distinct_cols(sql: str) -> set[str]:
    return set(SQL_DISTINCT_COL_RE.findall(sql))

# ---------------------------
# Column picking for DISTINCT
# ---------------------------
ID_LIKE = re.compile(r'(^|_)(id|number)$', re.I)

def _pick_distinct_column(learned: dict, table: str, col_like_phrase: str) -> str:
    """
    Choose a good column for DISTINCT queries:
      1) exact normalized match to phrase
      2) avoid id/number-like columns
      3) prefer categorical (few distinct sample values) as proxy
      4) fallback to first column
    """
    cols = learned["tables"][table]["columns"]
    samples = learned["tables"][table]["samples"]

    want = " ".join(col_like_phrase.strip().lower().replace("_", " ").split())
    for c in cols:
        cn = " ".join(c.lower().replace("_", " ").split())
        if cn == want:
            return c

    non_id = [c for c in cols if not ID_LIKE.search(c)] or cols[:]

    def cat_key(c: str):
        return (len(samples.get(c, [])) or 10_000, c)

    non_id.sort(key=cat_key)
    return non_id[0] if non_id else cols[0]

# ---------------------------
# Utility
# ---------------------------
def _strip_sql(sql: str) -> str:
    """Do NOT add LIMIT here; executor/UI decides."""
    return sql.strip().rstrip(";")

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

    # Learn schema + corpora
    learned = learn_schema(conn) if conn is not None else {"tables": {}, "columns": {}}
    dynamic_corpus: List[Dict[str, str]] = generate_dynamic_corpus(learned) if learned.get("tables") else []
    user_corpus: List[Dict[str, str]] = load_user_corpus()

    # Build expanded corpus (paraphrases) for retriever + PMI
    base_corpus = [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in TEMPLATES] \
                + [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in dynamic_corpus] \
                + [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in user_corpus]
    expanded: List[Dict[str, str]] = []
    for it in base_corpus:
        qq = it["q"]
        expanded.append(it)
        for pp in paraphrase_questions(qq):
            if pp != qq:
                expanded.append({"q": pp, "sql": it["sql"]})

    # PMI model from expanded corpus
    pmi = build_pmi(expanded, min_df=1)

    toks = keywords(q)

    # ---------- High-priority schema-aware rules ----------

    # UNIQUE/DISTINCT <col> IN <table>
    m = RE_UNIQUE.search(q)
    if m and learned.get("tables"):
        col_like, tab_like = m.group(2), m.group(3)
        t, _, _ = score_table_column(learned, keywords(col_like) + keywords(tab_like), pmi=pmi)
        if t:
            c = _pick_distinct_column(learned, t, col_like)
            cands.append(
                Candidate(
                    sql=_strip_sql(f"SELECT DISTINCT {c} FROM {t} ORDER BY {c}"),
                    score=0.99,  # outrank retriever when resolved
                    rationale="Rule: unique/distinct (resolved table/column; non-ID/categorical preference)",
                )
            )

    # HOW MANY <col> IN <table>  -> COUNT(DISTINCT col)
    m = RE_COUNT_DISTINCT.search(q)
    if m and learned.get("tables"):
        col_like, tab_like = m.group(1), m.group(2)
        t, c, _ = score_table_column(learned, keywords(col_like) + keywords(tab_like), pmi=pmi)
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
        t, _, _ = score_table_column(learned, keywords(tab_like), pmi=pmi)
        if t:
            cands.append(
                Candidate(
                    sql=f"SELECT COUNT(*) AS row_count FROM {t}",
                    score=0.94,
                    rationale="Rule: count rows in table",
                )
            )

    # TOP K <colA> IN <table> BY <colB>
    m = RE_TOPK_IN_BY.search(q)
    if m and learned.get("tables"):
        k, colA_like, tab_like, colB_like = m.groups()
        tA, colA, _ = score_table_column(learned, keywords(colA_like) + keywords(tab_like), pmi=pmi)
        tB, colB, _ = score_table_column(learned, keywords(colB_like) + keywords(tab_like), pmi=pmi)
        if tA and tB and tA == tB and colA and colB:
            sql = f"SELECT {colA}, SUM({colB}) AS s FROM {tA} GROUP BY {colA} ORDER BY s DESC LIMIT {int(k)}"
            cands.append(Candidate(sql=_strip_sql(sql), score=0.90, rationale="Rule: top-K by aggregate"))

    # "<table> in <year>"  (choose a date-like column if available)
    m = RE_YEAR_IN.search(q)
    if m and learned.get("tables"):
        tab_like, year = m.group(1), re.search(r"(19|20)\d{2}", q).group(0)
        t, _, _ = score_table_column(learned, keywords(tab_like), pmi=pmi)
        if t:
            date_col = None
            for col in learned["tables"][t]["columns"]:
                if learned["columns"][f"{t}.{col}"]["is_date"]:
                    date_col = col
                    break
            if date_col:
                cands.append(
                    Candidate(
                        sql=_strip_sql(f"SELECT * FROM {t} WHERE substr({date_col},1,4)='{year}'"),
                        score=0.88,
                        rationale="Rule: year filter",
                    )
                )

    # SHOW/LIST ...  -> infer table, equality filters (value index), year
    if RE_SHOW_LIST.search(q) and learned.get("tables"):
        t, c, _ = score_table_column(learned, toks, pmi=pmi)
        if t:
            where = []
            for (ft, fc, fv) in predict_filters(learned, q):
                if ft == t:
                    where.append(f"LOWER({fc}) = '{fv}'")
            _k, year = predict_numbers(q)
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
                    sql=_strip_sql(f"SELECT * FROM {t}{where_sql}"),
                    score=0.88,
                    rationale="Rule: show/list with inferred filters",
                )
            )

    # --- Join discovery: if two tables are mentioned and a FK is inferable, propose join candidates
    if learned.get("tables"):
        table_pairs = two_table_candidates(toks, learned)
        for (t1, t2) in table_pairs[:2]:
            for item in synthesize_join_templates(learned, t1, t2)[:3]:
                cands.append(Candidate(sql=_strip_sql(item["sql"]), score=0.83, rationale=f"Join discovery: {item['q']}"))

    # ---------- Retriever over static + dynamic + user (with frequency + compatibility) ----------
    # Include user frequency counts when available
    user_items_with_counts = [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": int(x.get("count", 1))}
                              for x in user_corpus]
    corpus = [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": 1} for x in TEMPLATES]
    corpus += [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": 1} for x in dynamic_corpus]
    corpus += user_items_with_counts

    # Predict (table, column) once for compatibility scoring
    t_pred, c_pred, _ = score_table_column(learned, toks, pmi=pmi) if learned.get("tables") else (None, None, 0.0)

    if corpus:
        idxs = rank(q.lower(), [x["q"] for x in corpus], topk=8)
        for rank_i, i in enumerate(idxs):
            item = corpus[i]
            base = 0.78 - 0.04 * rank_i
            boost = min(0.12, 0.04 * math.log1p(item.get("count", 1)))  # frequency boost for user-learned items

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
                    sql=_strip_sql(item["sql"]),
                    score=score,
                    rationale=f"Retriever: {item['q']} (freq×{item.get('count',1)})",
                )
            )

    # ---------- Fallback + dedupe ----------
    if not cands:
        cands.append(Candidate(sql="SELECT * FROM policies", score=0.40, rationale="Fallback sample"))

    cands = _dedupe_keep_best(cands)
    return cands
