# legacy_assistant/feedback_learn.py
from __future__ import annotations
import json, os
from typing import List, Dict, Any

FEEDBACK_LOG = "feedback.jsonl"
USER_CORPUS = "user_templates.jsonl"  # append-only Q→SQL pairs learned from feedback

def ingest_feedback_to_corpus() -> int:
    """
    Scan feedback.jsonl and append new (q, sql) pairs to user_templates.jsonl
    - If 'corrected_sql' exists, use that
    - Else if 'correct'==True, use generated_sql
    Returns number of new templates added.
    """
    if not os.path.exists(FEEDBACK_LOG):
        return 0
    existing = set()
    if os.path.exists(USER_CORPUS):
        with open(USER_CORPUS, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    d = json.loads(line)
                    existing.add((d.get("q",""), d.get("sql","")))
                except Exception:
                    pass

    adds = 0
    with open(FEEDBACK_LOG, "r", encoding="utf-8") as f, open(USER_CORPUS, "a", encoding="utf-8") as out:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            q = (rec.get("question") or "").strip().lower()
            sql = (rec.get("corrected_sql") or rec.get("generated_sql") or "").strip()
            correct = bool(rec.get("correct", False)) or bool(rec.get("corrected_sql"))
            if not (q and sql and correct):  # only trust positive/corrected feedback
                continue
            key = (q, sql)
            if key in existing:
                continue
            out.write(json.dumps({"q": q, "sql": " ".join(sql.split())}, ensure_ascii=False) + "\n")
            existing.add(key)
            adds += 1
    return adds

def load_user_corpus() -> List[Dict[str,str]]:
    items: List[Dict[str,str]] = []
    if not os.path.exists(USER_CORPUS):
        return items
    with open(USER_CORPUS, "r", encoding="utf-8") as f:
        for line in f:
            try:
                items.append(json.loads(line))
            except Exception:
                pass
    return items
