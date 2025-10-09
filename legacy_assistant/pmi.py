# legacy_assistant/pmi.py
from __future__ import annotations
import math, re
from typing import Dict, List, Tuple

TOKEN = re.compile(r"[a-z0-9_]+")

def _tok(s: str) -> List[str]:
    return TOKEN.findall((s or "").lower())

def _columns_from_sql(sql: str) -> List[str]:
    # very light extraction: grab <table>.<column> and bare columns after SELECT/WHERE/GROUP BY
    pairs = re.findall(r"\b([a-z_]\w*)\.([a-z_]\w*)\b", sql or "", flags=re.I)
    cols = [f"{t}.{c}" for t,c in pairs]
    # include DISTINCT col pattern
    dcols = re.findall(r"\bselect\s+distinct\s+([a-z_]\w*)", sql or "", flags=re.I)
    cols += dcols
    return list(dict.fromkeys(cols))

def build_pmi(corpus: List[Dict[str,str]], min_df:int=1) -> Dict[str, float]:
    """
    Build PMI scores for pairs (token, column_key).
    corpus items: {"q": "...", "sql": "..."}  (column_key is "table.col" or "col")
    Returns dict with keys: f"{token}||{colkey}" -> PMI value.
    """
    # Count token and column occurrences and co-occurrences
    tf_tok: Dict[str,int] = {}
    tf_col: Dict[str,int] = {}
    tf_pair: Dict[Tuple[str,str], int] = {}
    N = 0

    for item in corpus:
        q = (item.get("q") or "").lower()
        sql = item.get("sql") or ""
        toks = set(_tok(q))
        cols = set(_columns_from_sql(sql))
        if not toks or not cols:
            continue
        N += 1
        for t in toks:
            tf_tok[t] = tf_tok.get(t,0)+1
        for c in cols:
            tf_col[c] = tf_col.get(c,0)+1
        for t in toks:
            for c in cols:
                tf_pair[(t,c)] = tf_pair.get((t,c),0)+1

    # Compute PMI with add-1 smoothing
    pmi: Dict[str,float] = {}
    if N == 0:
        return pmi
    for (t,c), n_tc in tf_pair.items():
        if tf_tok.get(t,0) < min_df or tf_col.get(c,0) < min_df:
            continue
        p_t  = (tf_tok[t]+1) / (N+1)
        p_c  = (tf_col[c]+1) / (N+1)
        p_tc = (n_tc+1) / (N+1)
        val = math.log(p_tc/(p_t*p_c))
        pmi[f"{t}||{c}"] = val
    return pmi

def pmi_score(pmi: Dict[str,float], token: str, table: str, column: str) -> float:
    """
    Lookup helper: returns PMI(token, 'table.column') if present; backs off to PMI(token, 'column') if present.
    """
    key1 = f"{token.lower()}||{table}.{column}"
    key2 = f"{token.lower()}||{column}"
    return pmi.get(key1, pmi.get(key2, 0.0))
