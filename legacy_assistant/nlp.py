# legacy_assistant/nlp.py
from __future__ import annotations
import re
from typing import List, Tuple, Dict

TOKEN = re.compile(r"[a-z0-9_]+")
STOP = {
    "the","a","an","in","at","by","for","of","to","on","with","show","list","give","me",
    "how","many","what","which","where","when","is","are","do","does","all","any","and",
    "rows","records","entries",
    "unique","distinct","top","by","from","table","column"
}

# Small curated synonym map (extend freely)
SYN: Dict[str, List[str]] = {
    "policy": ["pol","policies","contract"],
    "claim": ["claims","loss","case"],
    "organization": ["org","company","customer","party","client"],
    "user": ["users","account","member"],
    "role": ["roles","permission","group"],
    "status": ["state","stage"],
    "amount": ["value","sum"],
    "credit": ["limit","creditlimit","credit_limit"],
    "city": ["town","location"],
    "number": ["code","id","identifier"],
    "active": ["current","open","enabled"],
}

def tokenize(s: str) -> List[str]:
    return TOKEN.findall(s.lower())

def keywords(s: str) -> List[str]:
    toks = tokenize(s)
    return [t for t in toks if t not in STOP]

def numbers_and_years(s: str) -> Tuple[List[int], List[int]]:
    years, nums = [], []
    for t in tokenize(s):
        if t.isdigit():
            v = int(t)
            if 1900 <= v <= 2100:
                years.append(v)
            else:
                nums.append(v)
    return nums, years

def synonyms_for(tok: str) -> List[str]:
    out = SYN.get(tok, [])
    # add trivial plural/singular variants
    if tok.endswith("s"): out.append(tok[:-1])
    else: out.append(tok + "s")
    return list({tok, *out})

def edit_distance(a: str, b: str, max_d: int = 2) -> int:
    # tiny bounded Levenshtein
    if a == b: return 0
    if abs(len(a)-len(b)) > max_d: return max_d + 1
    dp = list(range(len(b)+1))
    for i, ca in enumerate(a, 1):
        prev, dp[0] = dp[0], i
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            dp[j], prev = min(dp[j]+1, dp[j-1]+1, prev+cost), dp[j]
    return dp[-1]

def importance(tokens: List[str]) -> Dict[str, float]:
    # crude TF weighting to bias rare keywords
    tf: Dict[str, int] = {}
    for t in tokens: tf[t] = tf.get(t, 0) + 1
    total = sum(tf.values()) or 1
    return {t: tf[t]/total for t in tf}
