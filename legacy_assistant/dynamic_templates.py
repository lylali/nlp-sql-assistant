from __future__ import annotations
from typing import Dict, Any, List

def generate_dynamic_corpus(learned: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Build a dynamic Q→SQL corpus from the learned schema.
    Each item: {"q": "...", "sql": "..."}
    """
    items: List[Dict[str, str]] = []

    for t, tinfo in learned["tables"].items():
        cols = tinfo["columns"]
        samples = tinfo["samples"]

        # Table-level patterns
        items.append({"q": f"how many rows in {t}", "sql": f"SELECT COUNT(*) AS row_count FROM {t}"})
        items.append({"q": f"list columns of {t}", "sql": f"-- columns: {', '.join(cols)}"})

        # Column-level patterns
        for c in cols:
            col_key = f"{t}.{c}"
            cinfo = learned["columns"][col_key]
            # Distinct / unique
            items.append({"q": f"unique {c} in {t}", "sql": f"SELECT DISTINCT {c} FROM {t} ORDER BY {c} LIMIT 200"})
            items.append({"q": f"how many {c} in {t}", "sql": f"SELECT COUNT(DISTINCT {c}) AS distinct_{c}_count FROM {t}"})
            # Count by column
            items.append({"q": f"count by {c} in {t}", "sql": f"SELECT {c}, COUNT(*) AS n FROM {t} GROUP BY {c} ORDER BY n DESC LIMIT 200"})

            # Numeric aggregations
            if cinfo["is_numeric"]:
                items.append({"q": f"sum {c} in {t}", "sql": f"SELECT SUM({c}) AS sum_{c} FROM {t}"})
                items.append({"q": f"average {c} in {t}", "sql": f"SELECT AVG({c}) AS avg_{c} FROM {t}"})
                # numeric top-k by this numeric column (group by another column if exists)
                if len(cols) >= 2:
                    other = cols[0] if cols[0] != c else (cols[1] if len(cols) > 1 else c)
                    items.append({"q": f"top 10 {other} in {t} by {c}",
                                  "sql": f"SELECT {other}, SUM({c}) AS s FROM {t} GROUP BY {other} ORDER BY s DESC LIMIT 10"})

            # Date filters (year)
            if cinfo["is_date"]:
                items.append({"q": f"{t} in 2024", "sql": f"SELECT * FROM {t} WHERE substr({c},1,4)='2024' LIMIT 200"})
                items.append({"q": f"{t} in 2025", "sql": f"SELECT * FROM {t} WHERE substr({c},1,4)='2025' LIMIT 200"})

            # Value filters from samples
            for v in samples.get(c, [])[:5]:
                if isinstance(v, str):
                    vq = (v or "").lower()
                    if 2 <= len(vq) <= 40:
                        items.append({"q": f"show {t} where {c} = {vq}",
                                      "sql": f"SELECT * FROM {t} WHERE LOWER({c}) = '{vq}' LIMIT 200"})

    return items
