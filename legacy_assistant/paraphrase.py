# legacy_assistant/paraphrase.py
from __future__ import annotations
from typing import List, Dict

SYN = {
    "unique": ["distinct"],
    "distinct": ["unique"],
    "how many": ["count of", "number of"],
    "rows": ["records", "entries"],
    "show": ["list", "display"],
    "in": ["within"],
    "top": ["first"],
    "by": ["ordered by"],
}

def _variants(token: str) -> List[str]:
    return list({token, *SYN.get(token, [])})

def _swap_phrases(q: str) -> List[str]:
    out = {q}
    for k, alts in SYN.items():
        if k in q:
            for a in alts:
                out.add(q.replace(k, a))
    return list(out)

def paraphrase_questions(q: str) -> List[str]:
    """
    Produce a handful of safe paraphrases; keep short to avoid corpus explosion.
    """
    ql = (q or "").lower().strip()
    if not ql:
        return []
    cand = set([ql])
    # 1) phrase swaps
    for s in list(cand):
        for v in _swap_phrases(s):
            cand.add(v)
    # 2) minor reorder: "<unique> <col> in <table>" -> "in <table> <unique> <col>"
    if " in " in ql:
        parts = ql.split(" in ", 1)
        left, right = parts[0], parts[1]
        cand.add(f"in {right} {left}")
    # 3) add "the" occasionally
    if " rows in " in ql:
        cand.add(ql.replace(" rows in ", " rows in the "))
    return list(cand)[:6]  # cap
