
from .db import create_demo_connection, run_sql
from .nl2sql import generate_candidates
QUESTIONS=[
 "how many policies are active",
 "list the top 5 organizations by total credit limit",
 "show claims for policy POL-000123",
 "which policies expired in 2024",
 "find organizations in Cardiff",
]
def evaluate():
    conn=create_demo_connection()
    ok=0
    for q in QUESTIONS:
        c=generate_candidates(q)[0]
        cols,rows=run_sql(conn, c.sql)
        if cols and "error" not in cols: ok+=1
    return ok, len(QUESTIONS)
if __name__=="__main__":
    ok,total=evaluate(); print(f"Evaluation: {ok}/{total} executed without error.")
