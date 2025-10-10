from __future__ import annotations
import os, json, tempfile, re
from typing import List, Dict, Tuple

FEEDBACK_LOG    = "feedback.jsonl"
FEEDBACK_OFFSET = "feedback.offset"        # pointer to last processed byte
USER_CORPUS     = "user_templates.jsonl"   # {"q","sql","count"}
SYN_STORE       = "synonyms.json"          # {"token": {"maps_to": {...}, "count": N}}
PATTERNS        = "patterns.jsonl"         # {"q_pat","sql_pat"}

_SQL_COL = re.compile(r"\b([a-z_]\w*)\.([a-z_]\w*)\b", re.I)

def _load_jsonl(path: str) -> List[dict]:
    items=[]
    if os.path.exists(path):
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if line:
                    try: items.append(json.loads(line))
                    except: pass
    return items

def _dump_jsonl_atomic(path: str, items: List[dict]) -> None:
    fd, tmp = tempfile.mkstemp(prefix="user_templates_", suffix=".jsonl", dir=os.path.dirname(path) or ".")
    os.close(fd)
    with open(tmp,"w",encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False)+"\n")
    os.replace(tmp, path)

def _load_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            return json.loads(open(path,"r",encoding="utf-8").read())
        except:
            return {}
    return {}

def _dump_json(path: str, obj: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# NEW: offset helpers
def _read_offset() -> int:
    try:
        return int(open(FEEDBACK_OFFSET, "r", encoding="utf-8").read().strip() or 0)
    except Exception:
        return 0

def _write_offset(n: int) -> None:
    with open(FEEDBACK_OFFSET, "w", encoding="utf-8") as f:
        f.write(str(int(n)))

def ingest_feedback_to_corpus() -> Tuple[int, int]:
    """
    Process ONLY new lines appended to feedback.jsonl since last run.
    Returns (new_unique_items_added, total_unique_templates).
    """
    # Aggregate current corpus
    corpus_items = _load_jsonl(USER_CORPUS)
    index: Dict[Tuple[str,str], int] = {}
    for it in corpus_items:
        q = (it.get("q") or "").strip().lower()
        sql = " ".join((it.get("sql") or "").strip().split())
        cnt = int(it.get("count") or 1)
        if q and sql:
            index[(q, sql)] = index.get((q, sql), 0) + max(1, cnt)

    new_count = 0
    offset = _read_offset()
    size_after = offset

    if not os.path.exists(FEEDBACK_LOG):
        return (0, len(index))

    syn = _load_json(SYN_STORE)
    patterns = _load_jsonl(PATTERNS)

    with open(FEEDBACK_LOG, "r", encoding="utf-8") as f:
        f.seek(offset)
        for line in f:
            size_after += len(line.encode("utf-8"))
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            q = (rec.get("question") or "").strip().lower()
            corr = (rec.get("corrected_sql") or "").strip()
            gen  = (rec.get("generated_sql") or "").strip()
            correct = bool(rec.get("correct", False))
            sql = corr if corr else (gen if correct else "")
            if not (q and sql):
                continue

            norm_sql = " ".join(sql.split())
            key = (q, norm_sql)
            was = index.get(key, 0)
            index[key] = was + 1
            if was == 0:
                new_count += 1

            # --- mine synonyms
            cols = set(f"{t}.{c}" for (t,c) in _SQL_COL.findall(norm_sql))
            toks = [w for w in re.findall(r"[a-z0-9_]+", q) if len(w)>1]
            for tok in toks:
                if tok in ("unique","distinct","how","many","rows","in","by","top","first","show","list"):
                    continue
                entry = syn.setdefault(tok, {"maps_to": {}, "count": 0})
                entry["count"] = int(entry["count"]) + 1
                for col in cols:
                    entry["maps_to"][col] = entry["maps_to"].get(col, 0) + 1

            # --- induce patterns
            pat_q = re.sub(r"\b(19|20)\d{2}\b", "{YEAR}", q)
            pat_q = re.sub(r"\b(top|first)\s+\d+\b", r"\1 {K}", pat_q)
            pat_sql = re.sub(r"\b(19|20)\d{2}\b", "{YEAR}", norm_sql)
            pat_sql = re.sub(r"\bLIMIT\s+\d+\b", "LIMIT {K}", pat_sql)
            patterns.append({"q_pat": pat_q, "sql_pat": pat_sql})

    # dedupe patterns
    seen=set(); uniq=[]
    for p in patterns:
        key = (p.get("q_pat"), p.get("sql_pat"))
        if key not in seen:
            seen.add(key); uniq.append(p)

    # write results
    new_items = [{"q": q, "sql": sql, "count": cnt} for (q, sql), cnt in index.items()]
    new_items.sort(key=lambda x: (-x["count"], x["q"]))
    _dump_jsonl_atomic(USER_CORPUS, new_items)
    _dump_json(SYN_STORE, syn)
    _dump_jsonl_atomic(PATTERNS, uniq)

    # move offset after successful writes
    _write_offset(size_after)

    return new_count, len(new_items)


def load_user_corpus() -> List[dict]:
    items = _load_jsonl(USER_CORPUS)
    # re-aggregate defensively
    agg: Dict[Tuple[str,str], int] = {}
    for it in items:
        q = (it.get("q") or "").strip().lower()
        sql = " ".join((it.get("sql") or "").strip().split())
        cnt = int(it.get("count") or 1)
        if q and sql:
            agg[(q, sql)] = agg.get((q, sql), 0) + max(1, cnt)
    merged = [{"q": q, "sql": sql, "count": cnt} for (q, sql), cnt in agg.items()]
    merged.sort(key=lambda x: (-x["count"], x["q"]))
    return merged

def load_synonyms() -> Dict[str, Dict]:
    return _load_json(SYN_STORE)

def load_patterns() -> List[dict]:
    return _load_jsonl(PATTERNS)
