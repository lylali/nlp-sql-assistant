# legacy_assistant/nl2sql.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import re, math

# Core components from your project
from .templates import TEMPLATES
from .retriever import rank
from .learner import learn_schema
from .dynamic_templates import generate_dynamic_corpus
from .feedback_learn import load_user_corpus, load_patterns
from .nlp import keywords
from .predictor import score_table_column, predict_filters, predict_numbers
from .paraphrase import paraphrase_questions
from .pmi import build_pmi
from .joins import two_table_candidates, synthesize_join_templates
from .joins import synthesize_aggregate_join, _pk_for   # join-aware Top-K

# ---------------------------
# Data structure
# ---------------------------
@dataclass
class Candidate:
    sql: str
    score: float
    rationale: str

# ---------------------------
# Regex patterns (query intents)
# ---------------------------
RE_COUNT_DISTINCT   = re.compile(r"\bhow many ([a-z0-9_ ]+?) in ([a-z0-9_ ]+)\b", re.I)
RE_COUNT_ROWS       = re.compile(r"\bhow many (rows|records|entries) in ([a-z0-9_ ]+)\b", re.I)
RE_UNIQUE           = re.compile(r"\b(unique|distinct) ([a-z0-9_ ]+) in ([a-z0-9_ ]+)\b", re.I)
RE_TOPK_IN_BY       = re.compile(r"\btop\s+(\d+)\s+([a-z0-9_ ]+)\s+in\s+([a-z0-9_ ]+)\s+by\s+([a-z0-9_ ]+)\b", re.I)
RE_TOPK_WITH_HIGHEST= re.compile(r"\btop\s+(\d+)\s+([a-z0-9_ ]+?)\s+with\s+highest\s+([a-z0-9_ ]+)\b", re.I)
RE_SHOW_LIST        = re.compile(r"\b(show|list)\b", re.I)
RE_YEAR_IN          = re.compile(r"\b([a-z0-9_ ]+)\s+in\s+(19|20)\d{2}\b", re.I)

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
    Choose a good column for DISTINCT:
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
    return (sql or "").strip().rstrip(";")

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
    q = (question or "").strip()
    cands: List[Candidate] = []

    # ---- Learn schema + corpora
    learned = learn_schema(conn) if conn is not None else {"tables": {}, "columns": {}}
    dynamic_corpus: List[Dict[str, str]] = generate_dynamic_corpus(learned) if learned.get("tables") else []
    user_corpus: List[Dict[str, str]] = load_user_corpus()
    induced_patterns = load_patterns()  # [{"q_pat","sql_pat"}]

    # materialize a few pattern variants (keep tiny)
    pat_corpus: List[Dict[str, str]] = []
    for p in induced_patterns[:50]:
        qpat = p.get("q_pat", "")
        spat = p.get("sql_pat", "")
        if "{K}" in qpat or "{K}" in spat:
            pat_corpus.append({"q": qpat.replace("{K}", "10"), "sql": spat.replace("{K}", "10")})
        else:
            pat_corpus.append({"q": qpat, "sql": spat})

    # ---- Build expanded corpus (paraphrases) for retriever + PMI
    base_corpus = [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in TEMPLATES] \
                + [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in dynamic_corpus] \
                + [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in user_corpus] \
                + [{"q": x["q"], "sql": " ".join(x["sql"].split())} for x in pat_corpus]

    expanded: List[Dict[str, str]] = []
    for it in base_corpus:
        qq = it["q"]
        expanded.append(it)
        for pp in paraphrase_questions(qq):
            if pp != qq:
                expanded.append({"q": pp, "sql": it["sql"]})

    # ---- PMI model from expanded corpus
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
                    score=0.99,
                    rationale="Rule: unique/distinct (resolved table/column; non-ID/categorical preference)",
                )
            )

    # HOW MANY <col> IN <table>  -> COUNT(DISTINCT col)
    m = RE_COUNT_DISTINCT.search(q)
    if m and learned.get("tables"):
        col_like, tab_like = m.group(1), m.group(2)
        t, c, _ = score_table_column(learned, keywords(col_like) + keywords(tab_like), pmi=pmi)
        if t:
            # If scorer picked an ID-like column (e.g., user_id), choose a better categorical column
            if (not c) or c.lower().endswith("_id") or re.search(r"(?:^|_)(id|number)$", c, re.I):
                c = _pick_distinct_column(learned, t, col_like)
            cands.append(
                Candidate(
                    sql=f"SELECT COUNT(DISTINCT {c}) AS distinct_{c}_count FROM {t}",
                    score=0.95,
                    rationale="Rule: count distinct column in table (ID-avoidance fallback)",
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

    # TOP K <colA> IN <table> BY <colB> (single-table aggregate)
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

    # --- Join discovery: if two tables are mentioned and a FK is inferable, propose simple join candidates
    if learned.get("tables"):
        table_pairs = two_table_candidates(toks, learned)
        for (t1, t2) in table_pairs[:2]:
            for item in synthesize_join_templates(learned, t1, t2)[:3]:
                cands.append(Candidate(sql=_strip_sql(item["sql"]), score=0.83, rationale=f"Join discovery: {item['q']}"))


    # --- Top K <entity> with highest <metric> (join-aware) ---
    m = RE_TOPK_WITH_HIGHEST.search(q)
    if m and learned.get("tables"):
        k_str, ent_like, met_like = m.groups()
        try:
            k = int(k_str)
        except Exception:
            k = 10

        # ----- Resolve entity (group-by) table -----
        # Hard hint: anything like org/organization/company/client -> organizations
        ent_lo = ent_like.lower()
        t_ent = None
        if "organization" in ent_lo or "org" in ent_lo or "company" in ent_lo or "client" in ent_lo or "customer" in ent_lo:
            if "organizations" in learned["tables"]:
                t_ent = "organizations"
        # If no hard hint worked, use the scorer
        if not t_ent:
            t_ent, _, _ = score_table_column(learned, keywords(ent_like), pmi=pmi)

        # ----- Resolve metric intent/table/column -----
        met_lo = met_like.lower()
        count_intent = any(w in met_lo for w in ["count","counts","users","user","entries","rows","records"])

        # Prefer 'users' table for user-count intents if present
        t_metric = None
        c_metric = None
        if count_intent and "users" in learned.get("tables", {}):
            t_metric = "users"

        # If not set by hint, fall back to scorer
        if not t_metric:
            t_metric, c_metric, _ = score_table_column(learned, keywords(met_like), pmi=pmi)

        if count_intent:
            if t_ent and t_metric:
                sql = synthesize_aggregate_join(
                    learned,
                    target_table=t_ent,
                    metric_table=t_metric,
                    metric_col=_pk_for(learned, t_metric),  # ignored for COUNT
                    k=k,
                    agg="COUNT"
                )
                if sql:
                    cands.append(
                        Candidate(sql=_strip_sql(sql), score=0.96,
                                rationale="Rule: top-K by highest counts via join")
                    )
        else:
            if t_ent and t_metric and c_metric:
                sql = synthesize_aggregate_join(
                    learned,
                    target_table=t_ent,
                    metric_table=t_metric,
                    metric_col=c_metric,
                    k=k,
                    agg="SUM"
                )
                if sql:
                    cands.append(
                        Candidate(sql=_strip_sql(sql), score=0.94,
                                rationale="Rule: top-K by highest metric via join")
                    )


    # ---------- Retriever over static + dynamic + user (+ induced patterns) ----------
    # Include user frequency counts when available (but cap the influence)
    user_items_with_counts = [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": int(x.get("count", 1))}
                              for x in user_corpus]
    corpus = [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": 1} for x in TEMPLATES]
    corpus += [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": 1} for x in dynamic_corpus]
    corpus += user_items_with_counts
    corpus += [{"q": x["q"], "sql": " ".join(x["sql"].split()), "count": 1} for x in pat_corpus]

    # Predict (table, column) once for compatibility scoring
    t_pred, c_pred, _ = score_table_column(learned, toks, pmi=pmi) if learned.get("tables") else (None, None, 0.0)

    if corpus:
        idxs = rank(q.lower(), [x["q"] for x in corpus], topk=8)
        for rank_i, i in enumerate(idxs):
            item = corpus[i]
            base = 0.78 - 0.04 * rank_i
            boost = min(0.05, 0.02 * math.log1p(item.get("count", 1)))  # smaller cap to avoid hijack

            # compatibility with predicted table/column
            tables = _sql_tables(item["sql"])
            dcols  = _sql_distinct_cols(item["sql"])
            compat = 0.0
            if t_pred and t_pred in tables:
                compat += 0.10
            if c_pred and (c_pred in dcols or any(c_pred in seg for seg in dcols)):
                compat += 0.05

            # strong penalty if predicted table disagrees
            penalty = 0.0
            if t_pred and tables and t_pred not in tables:
                penalty -= 0.45

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
