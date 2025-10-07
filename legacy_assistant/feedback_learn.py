# legacy_assistant/feedback_learn.py
from __future__ import annotations
import json, os, tempfile, math
from typing import List, Dict, Tuple

FEEDBACK_LOG = "feedback.jsonl"
USER_CORPUS  = "user_templates.jsonl"   # canonical, de-duplicated; each line: {"q": "...", "sql": "...", "count": N}

def _load_jsonl(path: str) -> List[dict]:
    items: List[dict] = []
    if not os.path.exists(path):
        return items
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                continue
    return items

def _dump_jsonl_atomic(path: str, items: List[dict]) -> None:
    fd, tmp = tempfile.mkstemp(prefix="user_templates_", suffix=".jsonl", dir=os.path.dirname(path) or ".")
    os.close(fd)
    with open(tmp, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
    os.replace(tmp, path)

def ingest_feedback_to_corpus() -> Tuple[int, int]:
    """
    Fold feedback into user corpus (deduplicated with counts).
    Returns (added_or_updated, total_unique).
    Rules:
      - If feedback.correct == True -> use generated_sql
      - If corrected_sql present -> use corrected_sql (preferred)
      - Ignore entries with empty/invalid sql/question.
    """
    # Load existing corpus and index
    corpus_items = _load_jsonl(USER_CORPUS)
    index: Dict[Tuple[str, str], int] = {}
    for it in corpus_items:
        q = (it.get("q") or "").strip().lower()
        sql = (it.get("sql") or "").strip()
        cnt = int(it.get("count") or 1)
        if q and sql:
            index[(q, sql)] = index.get((q, sql), 0) + max(1, cnt)

    # Walk through feedback and tally counts
    fb_items = _load_jsonl(FEEDBACK_LOG)
    changed = 0
    for rec in fb_items:
        q = (rec.get("question") or "").strip().lower()
        if not q:
            continue
        # prefer corrected_sql when available; else generated_sql if marked correct
        corr = (rec.get("corrected_sql") or "").strip()
        gen  = (rec.get("generated_sql") or "").strip()
        correct = bool(rec.get("correct", False))
        sql = corr if corr else (gen if correct else "")
        if not sql:
            continue
        key = (q, " ".join(sql.split()))
        old = index.get(key, 0)
        index[key] = old + 1
        if old == 0:
            changed += 1
        else:
            changed += 1  # treat increments as "updated"

    # Rebuild corpus list sorted by count desc
    new_items = [{"q": q, "sql": sql, "count": cnt} for (q, sql), cnt in index.items()]
    new_items.sort(key=lambda x: (-x["count"], x["q"]))
    _dump_jsonl_atomic(USER_CORPUS, new_items)
    return changed, len(new_items)

def load_user_corpus() -> List[dict]:
    """
    Load deduplicated user corpus; if duplicates somehow exist, re-aggregate.
    Each item has: {"q": str, "sql": str, "count": int}
    """
    items = _load_jsonl(USER_CORPUS)
    if not items:
        return []
    agg: Dict[Tuple[str,str], int] = {}
    for it in items:
        q = (it.get("q") or "").strip().lower()
        sql = (it.get("sql") or "").strip()
        cnt = int(it.get("count") or 1)
        if q and sql:
            agg[(q, sql)] = agg.get((q, sql), 0) + max(1, cnt)
    merged = [{"q": q, "sql": sql, "count": cnt} for (q, sql), cnt in agg.items()]
    merged.sort(key=lambda x: (-x["count"], x["q"]))
    return merged
