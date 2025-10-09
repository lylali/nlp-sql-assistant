# legacy_assistant/active.py
from __future__ import annotations
from typing import List, Dict, Tuple
import math
from dataclasses import dataclass
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz
from .nlp import keywords

@dataclass
class Cand:
    sql: str
    score: float
    rationale: str

def uncertainty_from_candidates(cands: List[Cand]) -> float:
    """
    Higher means more uncertain. Combines:
      - margin between top-2 scores
      - entropy over top-k normalized scores
    """
    if not cands:
        return 1.0
    scores = sorted([max(1e-6, c.score) for c in cands], reverse=True)[:5]
    # margin (small margin -> uncertain)
    margin = scores[0] - (scores[1] if len(scores) > 1 else 0.0)
    # entropy
    ssum = sum(scores)
    probs = [s/ssum for s in scores]
    ent = -sum(p*math.log(p + 1e-12) for p in probs) / math.log(len(probs) + 1e-9)
    # combine: low margin & high entropy => uncertain
    u = (1.0 - min(1.0, margin)) * 0.6 + ent * 0.4
    return max(0.0, min(1.0, u))

def is_novel_question(q: string, corpus_qs: List[str], sim_threshold: float = 0.75) -> bool:
    """
    Novelty via TF-IDF cosine + rapidfuzz quick check.
    """
    qs = [x for x in corpus_qs if x]
    if not qs:
        return True
    # quick fuzzy scan
    best = max(fuzz.WRatio(q, ref)/100.0 for ref in qs)
    if best >= sim_threshold:
        return False
    # TF-IDF cosine
    vec = TfidfVectorizer(stop_words="english", ngram_range=(1,2), lowercase=True)
    X = vec.fit_transform(qs + [q])
    sims = cosine_similarity(X[-1], X[:-1]).ravel()
    return sims.max() < 0.55

def active_priority(question: str, cands: List[Cand], corpus_qs: List[str]) -> float:
    """
    Return priority 0..1 for asking user to label/correct this question.
    Combines uncertainty + novelty.
    """
    u = uncertainty_from_candidates(cands)
    nov = 1.0 if is_novel_question(question.lower(), corpus_qs) else 0.0
    return max(0.0, min(1.0, 0.6*u + 0.4*nov))
