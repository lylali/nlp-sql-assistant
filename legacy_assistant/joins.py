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


# ---------- FK inference & PK heuristics ----------

def _pk_for(learned: Dict[str,Any], table: str) -> str:
    cols = learned["tables"][table]["columns"]
    if "id" in cols: return "id"
    cand = f"{table.rstrip('s')}_id"
    if cand in cols: return cand
    # fallback: first *_id
    for c in cols:
        if c.endswith("_id"): return c
    # absolute fallback: first column
    return cols[0]

def infer_fk_edges(learned: Dict[str,Any]) -> List[Tuple[str,str,str,str]]:
    """
    Infer FKs by:
      1) Column name equals another table's PK (exact): users.org_id -> organizations.org_id
      2) Column stem matches table name or its singular: policy_id -> policy/policies
      3) Fallback: stem is contained in table name (org -> organizations)
    Returns edges as (src_table, src_col, dst_table, dst_pk).
    """
    edges: List[Tuple[str,str,str,str]] = []
    tables = list(learned.get("tables", {}).keys())

    # precompute PKs
    pks = {t: _pk_for(learned, t) for t in tables}

    for t in tables:
        for c in learned["tables"][t]["columns"]:
            if not c.endswith("_id"):
                continue
            stem = c[:-3].lower()
            # 1) exact PK name match
            for u in tables:
                if u == t: continue
                if c == pks[u]:
                    edges.append((t, c, u, pks[u])); break
            else:
                # 2) exact/singular table name match
                for u in tables:
                    if u == t: continue
                    if u.lower() == stem or u.rstrip("s").lower() == stem.rstrip("s"):
                        edges.append((t, c, u, pks[u])); break
                else:
                    # 3) containment (org in organizations)
                    for u in tables:
                        if u == t: continue
                        if stem and (stem in u.lower() or u.lower() in stem):
                            edges.append((t, c, u, pks[u])); break
    return edges


def _adjacency(learned: Dict[str,Any]) -> Dict[str, List[Tuple[str,str,str]]]:
    """Adj list: t -> [(u, t_col, u_pk), ...]."""
    adj: Dict[str, List[Tuple[str,str,str]]] = {}
    for (t, c, u, pk) in infer_fk_edges(learned):
        adj.setdefault(t, []).append((u, c, pk))
    return adj

def find_join_path(learned: Dict[str,Any], src: str, dst: str, max_hops: int = 2) -> Optional[List[Tuple[str,str,str,str]]]:
    """
    BFS up to 2 hops to connect src -> dst.
    Returns a list of join steps as (left_table, left_col, right_table, right_pk).
    """
    if src == dst:
        return []
    adj = _adjacency(learned)
    # 0-hop handled above; try 1-hop
    for (u, c, pk) in adj.get(src, []):
        if u == dst:
            return [(src, c, dst, pk)]
    # 2-hop: src -> mid -> dst
    for (u, c, pk) in adj.get(src, []):
        for (v, c2, pk2) in adj.get(u, []):
            if v == dst:
                return [(src, c, u, pk), (u, c2, dst, pk2)]
    return None

# ---------- Two-table discovery (kept) ----------

def two_table_candidates(tokens: List[str], learned: Dict[str,Any]) -> List[Tuple[str,str]]:
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
    seen=set(); out=[]
    for p in pairs:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

# ---------- Aggregate join synthesis ----------

def _guess_label_column(learned: Dict[str,Any], table: str) -> Optional[str]:
    """
    Choose a human-friendly label column to display alongside the PK.
    Priority list is tailored for your schema.
    """
    prio = [
        "org_name", "organization_name", "org_code", "name", "display_name",
        "username", "policy_number", "claim_number", "invoice_number", "title"
    ]
    cols = set(learned["tables"][table]["columns"])
    for p in prio:
        if p in cols:
            return p
    # if none match, try first textual-looking column
    for c in learned["tables"][table]["columns"]:
        if not c.endswith("_id") and not c.endswith("_number"):
            return c
    return None

def synthesize_aggregate_join(
    learned: Dict[str,Any],
    target_table: str,
    metric_table: str,
    metric_col: str,
    k: int,
    agg: str = "SUM"
) -> Optional[str]:
    """
    Compose SELECT over a join path from target_table (group-by) to metric_table.
    For agg='COUNT', metric_col is ignored and COUNT(*) is used.
    SELECTS readable columns:
      - <target_table>.<pk>  AS <target_table>_id
      - <target_table>.<label_col>  AS <label_col>   (if available)
      - metric as 'user_count' (if counting users), 'row_count' (other COUNT),
        or 'sum_<metric_col>' (for SUM).
    """
    path = find_join_path(learned, target_table, metric_table, max_hops=2)
    if path is None:
        # Try reverse direction
        path = find_join_path(learned, metric_table, target_table, max_hops=2)
        if path is None:
            return None
        start = metric_table
    else:
        start = target_table

    # PK and label for the group entity
    t_pk = _pk_for(learned, target_table)
    label_col = _guess_label_column(learned, target_table)

    # Build FROM/JOIN chain
    steps = path[:]
    from_tbl = start
    joins = []
    left = from_tbl
    for (L, Lcol, R, Rpk) in steps:
        # ensure the join is written from the current 'left'
        if L != left:
            L, R = R, L
            Lcol, Rpk = Rpk, Lcol
        joins.append(f"JOIN {R} ON {L}.{Lcol} = {R}.{Rpk}")
        left = R

    # Select list + metric alias
    id_alias = f"{target_table}_id"
    select_cols = [f"{target_table}.{t_pk} AS {id_alias}"]
    if label_col:
        select_cols.append(f"{target_table}.{label_col} AS {label_col}")

    if agg.upper() == "COUNT":
        metric_alias = "user_count" if metric_table.lower() == "users" else "row_count"
        metric_expr = f"COUNT(*) AS {metric_alias}"
        order_expr = metric_alias
    else:
        metric_alias = f"sum_{metric_col}"
        metric_expr = f"SUM({metric_table}.{metric_col}) AS {metric_alias}"
        order_expr = metric_alias

    sel = ", ".join(select_cols + [metric_expr])
    grp = "GROUP BY " + ", ".join([a.split(" AS ")[0] for a in select_cols])

    sql = (
        f"SELECT {sel}\n"
        f"FROM {from_tbl}\n"
        + ("\n".join(joins) + "\n" if joins else "")
        + f"{grp}\nORDER BY {order_expr} DESC\nLIMIT {int(k)}"
    )
    return sql


