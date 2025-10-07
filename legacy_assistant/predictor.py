# legacy_assistant/predictor.py
from __future__ import annotations
import sqlite3
from typing import Dict, Any, List, Tuple, Optional
from .nlp import keywords, numbers_and_years, synonyms_for, edit_distance

def build_value_index(learned: Dict[str,Any]) -> Dict[str, List[Tuple[str,str]]]:
    """
    Reverse index: value (lower) -> [(table, column), ...]
    """
    idx: Dict[str, List[Tuple[str,str]]] = {}
    for t, tinfo in learned.get("tables", {}).items():
        for c, vals in tinfo.get("samples", {}).items():
            for v in vals:
                if isinstance(v, str):
                    key = v.strip().lower()
                    if not key: continue
                    idx.setdefault(key, []).append((t, c))
    return idx

def score_table_column(learned: Dict[str,Any], q_tokens: List[str]) -> Tuple[Optional[str], Optional[str], float]:
    """
    Predict the most likely (table, column) given tokens.
    Scoring signals:
      - exact token match vs column/table surfaces (+)
      - synonym match (+)
      - small edit distance (+)
    """
    best = (None, None, 0.0)
    for t, tinfo in learned.get("tables", {}).items():
        # table surfaces
        tsurfs = set(tinfo.get("surfaces", [])) | {t}
        t_score = 0.0
        for tok in q_tokens:
            tok_syn = set(synonyms_for(tok))
            for s in tsurfs | tok_syn:
                d = edit_distance(tok, s, 2)
                if d == 0: t_score += 0.5
                elif d == 1: t_score += 0.25

        # check columns
        for c in tinfo.get("columns", []):
            csurfs = set([c.replace("_"," "), c])
            c_score = t_score
            for tok in q_tokens:
                tok_syn = set(synonyms_for(tok))
                for s in csurfs | tok_syn:
                    d = edit_distance(tok, s, 2)
                    if d == 0: c_score += 0.6
                    elif d == 1: c_score += 0.3
            if c_score > best[2]:
                best = (t, c, c_score)

        # also consider table-only predictions
        if t_score > best[2]:
            best = (t, None, t_score)

    return best

def predict_filters(learned: Dict[str,Any], q: str) -> List[Tuple[str,str,str]]:
    """
    Guess equality filters from values mentioned in question using the value index.
    Returns a list of (table, column, value_lc).
    """
    idx = build_value_index(learned)
    out: List[Tuple[str,str,str]] = []
    for tok in keywords(q):
        val = tok.strip().lower()
        if val in idx:
            # prefer the first mapping (could be several)
            t, c = idx[val][0]
            out.append((t, c, val))
    return out

def predict_numbers(q: str) -> Tuple[Optional[int], Optional[int]]:
    nums, years = numbers_and_years(q)
    k = None
    if nums:
        # choose the first non-year as K (top-K, limit)
        k = nums[0]
    y = years[0] if years else None
    return k, y
