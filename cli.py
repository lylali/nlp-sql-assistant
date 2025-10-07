#!/usr/bin/env python3
import argparse
from legacy_assistant.db import create_demo_connection, run_sql, schema_introspect
from legacy_assistant.nl2sql import generate_candidates
from legacy_assistant.feedback import record_feedback
from legacy_assistant.config import AppConfig

def main(argv=None):
    ap=argparse.ArgumentParser(description="Legacy Assistant Demo — NL→SQL")
    ap.add_argument("-q","--question", required=True)
    ap.add_argument("--show-schema", action="store_true")
    ap.add_argument("--topk", type=int, default=5)
    ap.add_argument("--apply", type=int, default=1)
    ap.add_argument("--row-limit", type=int, default=None)
    ap.add_argument("--feedback-correct", action="store_true")
    ap.add_argument("--feedback-corrected-sql")
    args=ap.parse_args(argv)

    cfg=AppConfig()
    conn=create_demo_connection(cfg.demo_rows_policies, cfg.demo_rows_claims)

    if args.show_schema:
        schema=schema_introspect(conn)
        for t, cols in schema.items(): print(f"{t}: {', '.join(cols)}")
        return 0

    cands=generate_candidates(args.question, conn=conn)
    print("\nCandidates:")
    for i,c in enumerate(cands[:args.topk],1):
        print(f"[{i}] score={c.score:.2f}  rationale={c.rationale}\nSQL:\n{c.sql}\n")

    idx=max(1,min(args.apply,len(cands)))-1
    sql=cands[idx].sql
    has_limit = " limit " in sql.lower()
    cols, rows = run_sql(conn, sql, row_limit=None if has_limit else (args.row_limit or cfg.row_limit_default))

    if not cols: print("No result."); return 0
    if "error" in cols:
        print("ERROR:", rows[0][0]); print("SQL:", rows[0][1])
    else:
        widths=[max(len(str(x)) for x in [col]+[row[i] for row in rows[:50]]) for i,col in enumerate(cols)]
        print(" | ".join(col.ljust(widths[i]) for i,col in enumerate(cols)))
        print("-+-".join("-"*w for w in widths))
        for row in rows[:50]: print(" | ".join(str(v).ljust(widths[i]) for i,v in enumerate(row)))
        if len(rows)>50: print(f"... ({len(rows)} rows total)")

    if args.feedback_correct or args.feedback_corrected_sql:
        record_feedback(args.question, sql, correct=args.feedback_correct, corrected_sql=args.feedback_corrected_sql)
    return 0

if __name__=="__main__":
    raise SystemExit(main())
