# legacy_assistant/predictor.py
from __future__ import annotations
import sqlite3
from typing import Dict, Any, List, Tuple, Optional
from .nlp import keywords, numbers_and_years, synonyms_for, edit_distance
from .pmi import pmi_score as _pmi_score

def score_table_column(learned: Dict[str,Any], q_tokens: List[str], pmi: Dict[str,float] | None = None) -> Tuple[Optional[str], Optional[str], float]:
    """
    Predict (table, column) with fuzzy matching + optional PMI boost.
    """
    best = (None, None, 0.0)
    for t, tinfo in learned.get("tables", {}).items():
        tsurfs = set(tinfo.get("surfaces", [])) | {t}
        base_t = 0.0
        for tok in q_tokens:
            tok_syn = set(synonyms_for(tok))
            for s in tsurfs | tok_syn:
                d = edit_distance(tok, s, 2)
                if d == 0: base_t += 0.5
                elif d == 1: base_t += 0.25
        for c in tinfo.get("columns", []):
            c_score = base_t
            csurfs = set([c.replace("_"," "), c])
            for tok in q_tokens:
                tok_syn = set(synonyms_for(tok))
                for s in csurfs | tok_syn:
                    d = edit_distance(tok, s, 2)
                    if d == 0: c_score += 1.0
                    elif d == 1: c_score += 0.5
                if pmi:
                    c_score += 0.2 * _pmi_score(pmi, tok, t, c)  # PMI bump (small but discriminative)
            if c_score > best[2]:
                best = (t, c, c_score)
        if base_t > best[2]:
            best = (t, None, base_t)
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
