# legacy_assistant/nlp.py
from __future__ import annotations
from typing import List, Tuple, Dict, Any
import re
from .feedback_learn import load_synonyms as _load_syn

# Lazy-load spaCy so CLI start is fast and Streamlit cache can keep the nlp object
_SPACY = None
def _get_nlp():
    global _SPACY
    if _SPACY is None:
        import spacy
        try:
            _SPACY = spacy.load("en_core_web_sm")
        except OSError:
            # fallback: try to import the package name directly if installed via wheel URL
            import en_core_web_sm
            _SPACY = en_core_web_sm.load()
    return _SPACY

# Basic stop set; spaCy stop words are a bit too broad for our purpose
STOP = {
    "the","a","an","in","at","by","for","of","to","on","with",
    "show","list","display","give","me","how","many","what","which","where","when",
    "is","are","do","does","all","any","and","or","from","table","column",
    "rows","records","entries","unique","distinct","top","first","within","by"
}

# Curated synonyms used by paraphrase & some matching
SYN: Dict[str, List[str]] = {
    "policy": ["policies","contract"],
    "claim": ["claims","loss","case"],
    "role": ["roles","permission","group"],
    "roles": ["role","permissions","groups"],
    "status": ["state","stage"],
    "amount": ["value","sum","total"],
    "city": ["town","location"],
    "number": ["code","id","identifier"],
    "active": ["current","open","enabled"],
    "organization": ["organizations","org","company","customer","client","party","policyholder"],
    "organizations": ["organization","orgs","companies","clients","parties","policyholders"],
    "user": ["users","account","accounts","member","members","user_account"],
    "users": ["user","accounts","members","user_account"],
    "credit limit": ["credit_limit","limit","coverage","exposure"],
    "policy": ["policies","contract"],    
}

TOKEN = re.compile(r"[a-z0-9_]+")

def spacy_doc(text: str):
    return _get_nlp()(text or "")

def tokens(text: str) -> List[str]:
    """Lowercased lemmas, filtered by stopwords and punctuation."""
    doc = spacy_doc(text)
    out = []
    for t in doc:
        if t.is_space or t.is_punct: 
            continue
        lem = (t.lemma_ or t.text).lower()
        lem = lem.strip()
        if not lem or lem in STOP:
            continue
        if len(lem) <= 1:
            continue
        out.append(lem)
    return out

def raw_tokens(text: str) -> List[str]:
    """Fallback regex tokens (used by PMI builder and quick scans)."""
    return TOKEN.findall((text or "").lower())

def keywords(text: str) -> List[str]:
    """Alias kept for backward-compat with your code."""
    return tokens(text)

def numbers_and_years(text: str) -> Tuple[List[int], List[int]]:
    """Extract numeric literals; split into generic numbers and year-like."""
    doc = spacy_doc(text)
    nums, years = [], []
    for ent in doc.ents:
        if ent.label_ == "DATE":
            # try to extract a 4-digit year
            m = re.search(r"(19|20)\d{2}", ent.text)
            if m:
                years.append(int(m.group(0)))
        elif ent.label_ in ("CARDINAL","QUANTITY","ORDINAL"):
            # pick integers in the span
            for tok in TOKEN.findall(ent.text):
                if tok.isdigit():
                    v = int(tok)
                    if 1900 <= v <= 2100:
                        years.append(v)
                    else:
                        nums.append(v)
    # fallback: raw digits
    for tok in raw_tokens(text):
        if tok.isdigit():
            v = int(tok)
            if 1900 <= v <= 2100 and v not in years:
                years.append(v)
            elif v not in nums:
                nums.append(v)
    return nums, years

def entities(text: str) -> Dict[str, List[str]]:
    """Expose useful NER for filters (ORG, GPE/LOC, CARDINAL/DATE)."""
    doc = spacy_doc(text)
    out: Dict[str, List[str]] = {"ORG":[], "GPE":[], "LOC":[], "DATE":[], "CARDINAL":[]}
    for e in doc.ents:
        if e.label_ in out:
            out[e.label_].append(e.text)
    return out

_dyn_syn_cache = None
def synonyms_for(tok: str) -> List[str]:
    global _dyn_syn_cache
    if _dyn_syn_cache is None:
        learned = _load_syn()  # {"token": {"maps_to": {...}, "count": N}}
        # Promote tokens that repeatedly map to the same column as aliases of that column name
        _dyn_syn_cache = {}
        for k, v in (learned or {}).items():
            if int(v.get("count",0)) >= 2:
                _dyn_syn_cache[k] = list(v.get("maps_to", {}).keys())
    s = SYN.get(tok, []) + _dyn_syn_cache.get(tok, [])
    if tok.endswith("s"): s.append(tok[:-1])
    else: s.append(tok + "s")
    return list({tok, *s})
