from __future__ import annotations
import re
from typing import Iterable, List

TOKEN = re.compile(r"[a-z0-9_]+")

def tokenize(text: str) -> List[str]:
    return TOKEN.findall(text.lower())

def underscore_to_words(name: str) -> str:
    return name.replace("_", " ").strip().lower()

def singular(s: str) -> str:
    # extremely small, safe singularizer (don’t overdo English morphology)
    if s.endswith("ies"): return s[:-3] + "y"
    if s.endswith("ses"): return s[:-2]  # statuses -> status
    if s.endswith("s") and len(s) > 3: return s[:-1]
    return s

def surface_forms(name: str) -> List[str]:
    w = underscore_to_words(name)
    # forms: raw, singular, plural (basic)
    forms = {w, singular(w)}
    return sorted(forms)

def normalized(s: str) -> str:
    return " ".join(tokenize(s))
