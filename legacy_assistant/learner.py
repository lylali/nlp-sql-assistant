from __future__ import annotations
import sqlite3
from typing import Dict, List, Any

from .lex import surface_forms

SAMPLE_DISTINCT_LIMIT = 40

def learn_schema(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Learn tables, columns, and sample values from the live demo DB.
    Returns a dict:
    {
      "tables": {
        table: {
          "columns": [col, ...],
          "samples": { col: [values...] },
          "surfaces": ["users","user",...]
        }, ...
      },
      "columns": {
        "table.col": {
          "surfaces": ["status","statu",...],
          "is_numeric": bool,
          "is_date": bool
        },
        ...
      }
    }
    """
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )]

    learned: Dict[str, Any] = {"tables": {}, "columns": {}}

    for t in tables:
        cols = [r[1] for r in cur.execute(f"PRAGMA table_info({t})")]
        learned["tables"][t] = {"columns": cols, "samples": {}, "surfaces": surface_forms(t)}

        # Sample distinct values for each column (for vocab + dynamic templates)
        for c in cols:
            # small sample to keep it fast
            try:
                q = f"SELECT {c} FROM {t} WHERE {c} IS NOT NULL GROUP BY {c} ORDER BY COUNT(*) DESC LIMIT {SAMPLE_DISTINCT_LIMIT}"
                vals = [row[0] for row in cur.execute(q)]
            except Exception:
                vals = []
            learned["tables"][t]["samples"][c] = vals

            # detect numeric/date-ish
            # sqlite pragma has types but in demo we can infer loosely
            is_num = False
            is_date = False
            for v in vals[:10]:
                if isinstance(v, (int, float)): is_num = True
                if isinstance(v, str) and len(v) >= 8 and v[4] == "-" and v[7] == "-":
                    is_date = True
            col_key = f"{t}.{c}"
            learned["columns"][col_key] = {
                "surfaces": surface_forms(c),
                "is_numeric": is_num,
                "is_date": is_date,
            }

    return learned
