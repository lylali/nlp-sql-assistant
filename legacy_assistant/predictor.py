# legacy_assistant/predictor.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
from rapidfuzz import fuzz, process  # fast & accurate fuzzy match
from .nlp import keywords, numbers_and_years, synonyms_for, entities
from .pmi import pmi_score as _pmi_score

def _score_token_surface(tok: str, surface: str) -> float:
    """Similarity between a query token (lemma) and a surface form."""
    # Use WRatio which blends multiple metrics, normalized to 0..1
    return fuzz.WRatio(tok, surface) / 100.0

def score_table_column(
    learned: Dict[str,Any],
    q_tokens: List[str],
    pmi: Dict[str,float] | None = None
) -> Tuple[Optional[str], Optional[str], float]:
    """
    Predict (table, column) using:
      - fuzzy similarity (rapidfuzz) on surfaces
      - synonyms expansion
      - PMI(token↔table.col) small boost when available
    """
    # Heuristic: if question tokens mention a table name directly, prefer that table
    hard_hint = None
    for t in learned.get("tables", {}):
        if any(tok in (t, t.rstrip("s")) for tok in q_tokens):
            hard_hint = t
            break

    best = (None, None, 0.0)
    for t, tinfo in learned.get("tables", {}).items():
        tsurfs = set(tinfo.get("surfaces", [])) | {t}
        t_score = 0.0
        # table-level score
        for tok in q_tokens:
            for s in tsurfs:
                t_score += 0.35 * _score_token_surface(tok, s)
        if hard_hint and t == hard_hint:
            t_score += 0.4    # strong bias toward explicitly mentioned table

        # column-level score
        for c in tinfo.get("columns", []):
            csurfs = {c, c.replace("_"," ")}
            c_score = t_score
            for tok in q_tokens:
                for s in csurfs | set(synonyms_for(tok)):
                    c_score += 0.8 * _score_token_surface(tok, s)
                if pmi:
                    c_score += 0.2 * _pmi_score(pmi, tok, t, c)
            if c_score > best[2]:
                best = (t, c, c_score)

        # table-only fallback
        if t_score > best[2]:
            best = (t, None, t_score)
    return best

def predict_filters(learned: Dict[str,Any], q: str) -> List[Tuple[str,str,str]]:
    """
    Guess equality filters from values mentioned in question.
    - Use entity types to bias which columns might match.
    - Still back by sampled value index from learn_schema() (cheap).
    """
    # Build value index once
    idx: Dict[str, List[Tuple[str,str]]] = {}
    for t, tinfo in learned.get("tables", {}).items():
        for c, vals in tinfo.get("samples", {}).items():
            for v in vals:
                if isinstance(v, str):
                    key = v.strip().lower()
                    if key:
                        idx.setdefault(key, []).append((t, c))

    ent = entities(q)
    out: List[Tuple[str,str,str]] = []

    # Prefer ORG/GPE strings first
    for bucket in ("ORG","GPE","LOC"):
        for val in ent.get(bucket, []):
            v = val.strip().lower()
            if v in idx:
                t, c = idx[v][0]
                out.append((t, c, v))

    # Then fall back to any raw keyword that matches a sampled value
    for tok in keywords(q):
        if tok in idx:
            t, c = idx[tok][0]
            out.append((t, c, tok))

    # De-duplicate while preserving order
    seen=set(); dedup=[]
    for it in out:
        if (it[0],it[1],it[2]) not in seen:
            seen.add((it[0],it[1],it[2])); dedup.append(it)
    return dedup

def predict_numbers(q: str) -> Tuple[Optional[int], Optional[int]]:
    nums, years = numbers_and_years(q)
    k = nums[0] if nums else None
    y = years[0] if years else None
    return k, y
