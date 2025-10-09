# legacy_assistant/retriever.py
from __future__ import annotations
from typing import List
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

# English stop words + bi-grams help paraphrase robustness
_VEC = TfidfVectorizer(stop_words="english", ngram_range=(1,2), min_df=1, lowercase=True)

def rank(query: str, corpus_qs: List[str], topk: int = 5) -> List[int]:
    """
    Rank corpus questions by cosine similarity to query using TF–IDF.
    Returns indices of topk matches.
    """
    if not corpus_qs:
        return []
    docs = corpus_qs + [query or ""]
    X = _VEC.fit_transform(docs)
    sims = linear_kernel(X[-1], X[:-1]).ravel()   # cosine similarities to all corpus items
    idxs = sims.argsort()[::-1][:topk]
    return idxs.tolist()
