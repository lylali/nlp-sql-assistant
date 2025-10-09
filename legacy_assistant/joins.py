# legacy_assistant/joins.py
from __future__ import annotations
import re
from typing import Dict, Any, List, Tuple

PAIR_RE = re.compile(r"\b([a-z_][\w]*)\b.*\b([a-z_][\w]*)\b")

def infer_fk_map(learned: Dict[str,Any]) -> Dict[Tuple[str,str], Tuple[str,str,str]]:
    """
    Infer foreign keys from column names like <other>_id.
    Returns mapping:
      (src_table, dst_table) -> (src_table, src_col, dst_table)  # join on src_col = dst_table.id (or dst_table.<dst_table>_id fallback)
    """
    fk = {}
    tables = list(learned.get("tables", {}).keys())
    for t in tables:
        for c in learned["tables"][t]["columns"]:
            if c.endswith("_id"):
                other = c[:-3]
                # pick the best dst table name by exact or singular match
                cand = None
                for tt in tables:
                    if tt == t: continue
                    if tt == other or tt.rstrip("s") == other.rstrip("s"):
                        cand = tt; break
                if cand:
                    fk[(t, cand)] = (t, c, cand)
    return fk

def two_table_candidates(tokens: List[str], learned: Dict[str,Any]) -> List[Tuple[str,str]]:
    """
    From tokens, try to find two table mentions (rough, using surfaces).
    Returns list of table pairs (t1, t2) in order of appearance.
    """
    surfaces = {}
    for t, info in learned.get("tables", {}).items():
        for s in info["surfaces"]+[t]:
            surfaces[s] = t
    hits = []
    for w in tokens:
        if w in surfaces:
            hits.append(surfaces[w])
    pairs = []
    for i in range(len(hits)-1):
        if hits[i] != hits[i+1]:
            pairs.append((hits[i], hits[i+1]))
    # de-dup while keeping order
    seen=set(); out=[]
    for p in pairs:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def synthesize_join_templates(learned: Dict[str,Any], t1: str, t2: str) -> List[Dict[str,str]]:
    """
    Create safe 2-table templates when a foreign key exists between t1 and t2 (either direction).
    """
    fk = infer_fk_map(learned)
    items: List[Dict[str,str]] = []

    def join_sql(src, col, dst):
        # choose some representative columns
        show1 = learned["tables"][src]["columns"][:2]
        show2 = learned["tables"][dst]["columns"][:2]
        sel = ", ".join([f"{src}.{show1[0]}", f"{dst}.{show2[0]}"])
        return f"""SELECT {sel}
FROM {src} 
JOIN {dst} ON {src}.{col} = {dst}.id
LIMIT 200"""

    # src->dst
    key = (t1, t2)
    if key in fk:
        src, col, dst = fk[key]
        items.append({"q": f"show {t1} with {t2}", "sql": join_sql(src,col,dst)})
        items.append({"q": f"count {t1} by {t2}", "sql":
                      f"SELECT {dst}.id, COUNT(*) AS n FROM {src} JOIN {dst} ON {src}.{col}={dst}.id GROUP BY {dst}.id ORDER BY n DESC LIMIT 200"})
        items.append({"q": f"sum by {t2} in {t1}", "sql":
                      f"SELECT {dst}.id, SUM(1) AS s FROM {src} JOIN {dst} ON {src}.{col}={dst}.id GROUP BY {dst}.id ORDER BY s DESC LIMIT 200"})
    # dst->src
    key = (t2, t1)
    if key in fk:
        src, col, dst = fk[key]
        items.append({"q": f"show {t2} with {t1}", "sql": join_sql(src,col,dst)})
        items.append({"q": f"count {t2} by {t1}", "sql":
                      f"SELECT {dst}.id, COUNT(*) AS n FROM {src} JOIN {dst} ON {src}.{col}={dst}.id GROUP BY {dst}.id ORDER BY n DESC LIMIT 200"})
        items.append({"q": f"sum by {t1} in {t2}", "sql":
                      f"SELECT {dst}.id, SUM(1) AS s FROM {src} JOIN {dst} ON {src}.{col}={dst}.id GROUP BY {dst}.id ORDER BY s DESC LIMIT 200"})
    return items
