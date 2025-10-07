import json, time

FEEDBACK_PATH = "feedback.jsonl"

def record_feedback(question, generated_sql, correct=False, corrected_sql=None, note=None):
    """
    Append a feedback record to JSONL store (append-only).
    """
    rec = {
        "ts": int(time.time()),
        "question": question,
        "generated_sql": generated_sql,
        "correct": bool(correct),
        "corrected_sql": corrected_sql,
        "note": note,
    }
    line = json.dumps(rec, ensure_ascii=False) + "\n"
    with open(FEEDBACK_PATH, "a", encoding="utf-8") as f:
        f.write(line)
