# tests/test_queries.py
from __future__ import annotations
import re
import unittest

from legacy_assistant.db import create_demo_connection, run_sql
from legacy_assistant.nl2sql import generate_candidates


class NL2SQLTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Deterministic, in-memory demo DB
        cls.conn = create_demo_connection(
            n_orgs=12, n_policies=80, n_claims=60, n_users=30, seed=7
        )

    @classmethod
    def tearDownClass(cls):
        try:
            cls.conn.close()
        except Exception:
            pass

    # --- helpers -------------------------------------------------------------

    def best_sql(self, question: str) -> str:
        """Return the top-scored candidate SQL for a question."""
        cands = generate_candidates(question, conn=self.conn)
        self.assertTrue(cands, f"No candidates for question: {question}")
        return cands[0].sql

    def exec_ok(self, sql: str, row_limit: int | None = None):
        """Execute SQL and assert it runs without error; return (cols, rows)."""
        cols, rows = run_sql(self.conn, sql, row_limit=row_limit)
        if cols and "error" in cols:
            self.fail(f"SQL error: {rows[0][0]}\nSQL: {rows[0][1]}")
        return cols, rows

    # --- tests ---------------------------------------------------------------

    def test_count_distinct_roles_in_users(self):
        q = "How many roles in users?"
        sql = self.best_sql(q)
        # should target users + role
        self.assertIn(" from users".lower(), sql.lower())
        self.assertIn("count(distinct", sql.lower())
        self.assertIn("role", sql.lower())
        cols, rows = self.exec_ok(sql)
        # one-row numeric result
        self.assertGreaterEqual(len(rows), 1)
        self.assertGreaterEqual(rows[0][0], 0)

    def test_unique_status_in_claims(self):
        q = "unique status in claims"
        sql = self.best_sql(q)
        # should be SELECT DISTINCT status FROM claims ...
        self.assertIn("select distinct", sql.lower())
        self.assertIn(" status ", sql.lower())
        self.assertIn(" from claims", sql.lower())
        cols, rows = self.exec_ok(sql)
        # compare count with COUNT DISTINCT baseline
        bcols, brows = self.exec_ok("SELECT COUNT(DISTINCT status) FROM claims")
        expect_cnt = int(brows[0][0])
        self.assertEqual(len(rows), expect_cnt)

    def test_top10_orgs_user_counts(self):
        q = "top 10 organizations with highest user counts"
        sql = self.best_sql(q)
        s = sql.lower()
        # must mention both tables somewhere
        self.assertIn("users", s)
        self.assertIn("organizations", s)

        # must have a join keyword
        self.assertRegex(s, r"\bjoin\b")

        # must join the two tables via the org_id FK in either direction
        self.assertTrue(
            re.search(r"users\.org_id\s*=\s*organizations\.org_id", s) or
            re.search(r"organizations\.org_id\s*=\s*users\.org_id", s),
            f"Expected join on org_id between users and organizations:\n{sql}"
        )


        self.assertTrue("count(" in s or "count(*)" in s)
        # should limit to 10 and not double-append limit when executed
        self.assertRegex(s, r"\blimit\s+10\b")
        # simulate UI passing a row_limit even when LIMIT exists: run_sql must NOT double-append
        cols, rows = self.exec_ok(sql, row_limit=500)
        self.assertLessEqual(len(rows), 10)
        # if got org_name aliased, ensure descending order by metric
        metric_col = None
        for c in cols:
            if c.lower().endswith("user_count") or c.lower().startswith("sum_") or c.lower().endswith("_count"):
                metric_col = c
                break
        if metric_col:
            vals = [r[cols.index(metric_col)] for r in rows]
            self.assertEqual(vals, sorted(vals, reverse=True))

    def test_year_filter_policies(self):
        q = "policies in 2025"
        sql = self.best_sql(q)
        # expect a year filter on a date-like column (substr(...,1,4)='2025')
        self.assertRegex(sql.lower(), r"substr\(.+?,\s*1,\s*4\)\s*=\s*'2025'")
        cols, rows = self.exec_ok(sql)
        # sanity: zero or more rows is fine; just ensure it executed
        self.assertIsInstance(rows, list)

    def test_no_double_limit_when_rowlimit_given(self):
        # pick a known limited query
        q = "top 5 organizations with highest user counts"
        sql = self.best_sql(q)
        # Ensure only a single LIMIT appears in final executed text (indirectly via no error)
        # Also assert the SQL text itself contains a single LIMIT
        lims = re.findall(r"(?i)\blimit\s+\d+\b", sql)
        self.assertEqual(len(lims), 1, f"Query has multiple LIMITs:\n{sql}")
        # run with an extra row_limit to ensure run_sql does not append another
        self.exec_ok(sql, row_limit=500)


if __name__ == "__main__":
    unittest.main(verbosity=2)
