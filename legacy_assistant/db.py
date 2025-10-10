# legacy_assistant/db.py
from __future__ import annotations
import sqlite3, random, datetime as dt
from typing import Tuple, List, Dict, Any, Optional
import re  

# --------------------------------------------
# Public API
# --------------------------------------------

def create_demo_connection(
    n_orgs: int = 12,
    n_policies: int = 80,
    n_claims: int = 60,
    n_users: int = 30,
    seed: int = 7,
) -> sqlite3.Connection:
    """
    Build an in-memory SQLite database with a small 'legacy-style' schema.

    Option A (one-to-many): each user belongs to exactly one organization
      organizations (org_id PK)
        └─< policies (org_id FK)
        └─< users   (org_id FK)
      claims (-> policies)

    Notes:
    - Thread-safe for Streamlit (check_same_thread=False).
    - Dates are stored as ISO-8601 TEXT (YYYY-MM-DD) for simplicity.
    """
    r = random.Random(seed)
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    cur = conn.cursor()

    # --- Schema ---------------------------------------------------------------
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE organizations(
      org_id        INTEGER PRIMARY KEY,
      org_code      TEXT UNIQUE,
      org_name      TEXT,
      city          TEXT,
      country_code  TEXT
    );

    CREATE TABLE policies(
      policy_id     INTEGER PRIMARY KEY,
      policy_number TEXT UNIQUE,
      org_id        INTEGER NOT NULL REFERENCES organizations(org_id) ON DELETE RESTRICT,
      inception_date TEXT,
      expiry_date    TEXT,
      currency       TEXT,
      status         TEXT,
      credit_limit   REAL,
      org_name_dn    TEXT
    );

    CREATE TABLE claims(
      claim_id     INTEGER PRIMARY KEY,
      policy_id    INTEGER NOT NULL REFERENCES policies(policy_id) ON DELETE CASCADE,
      claim_number TEXT UNIQUE,
      created_at   TEXT,
      amount       REAL,
      status       TEXT
    );

    -- Option A: each user belongs to exactly one organization
    CREATE TABLE users(
      user_id    INTEGER PRIMARY KEY,
      username   TEXT UNIQUE,
      role       TEXT,
      email      TEXT,
      created_at TEXT,
      org_id     INTEGER REFERENCES organizations(org_id) ON DELETE SET NULL
    );

    CREATE INDEX IF NOT EXISTS idx_policies_org   ON policies(org_id);
    CREATE INDEX IF NOT EXISTS idx_policies_dates ON policies(inception_date, expiry_date);
    CREATE INDEX IF NOT EXISTS idx_policies_stat  ON policies(status);
    CREATE INDEX IF NOT EXISTS idx_claims_policy  ON claims(policy_id);
    CREATE INDEX IF NOT EXISTS idx_claims_status  ON claims(status);
    CREATE INDEX IF NOT EXISTS idx_users_org      ON users(org_id);
    """)

    # --- Seed data ------------------------------------------------------------
    cities = [
        ("London","GB"), ("Cardiff","GB"), ("Amsterdam","NL"),
        ("Rotterdam","NL"), ("Madrid","ES"), ("Barcelona","ES"),
        ("Dublin","IE"), ("Paris","FR"), ("Berlin","DE"), ("Rome","IT")
    ]
    currencies = ["GBP","EUR","USD"]
    statuses_policy = ["ACTIVE","EXPIRED","LAPSED","PENDING","CANCELLED"]
    statuses_claim  = ["OPEN","RESERVED","SETTLED","DENIED","WITHDRAWN"]
    roles = ["ADMIN","UNDERWRITER","CLAIMS","FINANCE","BROKER"]

    # organizations
    org_rows = []
    for i in range(1, n_orgs + 1):
        code = f"ORG-{i:04d}"
        name = f"Organization {i:04d}"
        city, cc = r.choice(cities)
        org_rows.append((code, name, city, cc))
    cur.executemany(
        "INSERT INTO organizations(org_code, org_name, city, country_code) VALUES (?,?,?,?)",
        org_rows
    )

    # policies
    pol_rows = []
    for i in range(1, n_policies + 1):
        org_id = r.randint(1, n_orgs)
        inc = _rand_date(r, year=2024, spread_days=365)
        exp = _add_days(inc, r.randint(180, 540))
        curr = r.choice(currencies)
        status = r.choices(statuses_policy, weights=[6,2,1,1,1], k=1)[0]
        climit = float(r.randint(20_000, 200_000))
        pol_rows.append((
            f"POL-{i:05d}", org_id, inc, exp, curr, status, climit,  # policy cols
        ))
    cur.executemany(
        """INSERT INTO policies(
            policy_number, org_id, inception_date, expiry_date, currency, status, credit_limit, org_name_dn
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, org_name FROM organizations WHERE org_id = ?""",
        # We need org_name_dn, so map org_id twice (for subselect)
        [(pn, oid, inc, exp, ccy, st, lim, oid) for (pn, oid, inc, exp, ccy, st, lim) in pol_rows]
    )

    # claims
    claim_rows = []
    max_pol_id = _max_id(cur, "policies", "policy_id")
    for i in range(1, min(n_claims, max_pol_id*2) + 1):
        pol_id = r.randint(1, max_pol_id)
        created = _rand_date(r, year=2025, spread_days=240)
        amount = float(r.randint(5_000, 80_000))
        status = r.choices(statuses_claim, weights=[3,3,2,1,1], k=1)[0]
        claim_rows.append((f"CLM-{i:05d}", pol_id, created, amount, status))
    cur.executemany(
        "INSERT INTO claims(claim_number, policy_id, created_at, amount, status) VALUES (?,?,?,?,?)",
        claim_rows
    )

    # users (Option A: assign exactly one organization per user)
    user_rows = []
    for i in range(1, n_users + 1):
        username = f"user{i:03d}"
        role = r.choice(roles)
        email = f"{username}@example.com"
        created = _rand_date(r, year=2025, spread_days=180)
        org_id = r.randint(1, n_orgs)
        user_rows.append((username, role, email, created, org_id))
    cur.executemany(
        "INSERT INTO users(username, role, email, created_at, org_id) VALUES (?,?,?,?,?)",
        user_rows
    )

    conn.commit()
    return conn


def run_sql(conn: sqlite3.Connection, sql: str, row_limit: Optional[int] = None) -> Tuple[List[str], List[tuple]]:
    """
    Execute SQL safely.
    - If row_limit is provided AND the SQL lacks an explicit LIMIT (case-insensitive), append LIMIT row_limit.
    - Otherwise, run as-is.
    Returns (columns, rows). On error, returns (["error","sql"], [(err, sql)]).
    """
    try:
        s = (sql or "").strip().rstrip(";")
        low = s.lower()

        # Detect an existing LIMIT that isn't inside a string (simple heuristic)
        has_limit = " limit " in low or low.endswith(" limit") or re.search(r"\blimit\s+\d+\b", low) is not None

        if (row_limit is not None) and (not has_limit) and low.startswith("select"):
            s = f"{s}\nLIMIT {int(row_limit)}"

        cur = conn.cursor()
        cur.execute(s)
        if cur.description is None:
            conn.commit()
            return [], []
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    except Exception as e:
        return ["error", "sql"], [(str(e), (sql or "").strip())]



def schema_introspect(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Introspect tables/columns and basic type flags + a few sample values per column.
    Output structure (used by learner/predictor):
    {
      "tables": {
        "<table>": {
          "columns": ["col1","col2",...],
          "surfaces": ["org","organization","organizations",...],  # light synonyms
          "samples": {"col": ["value1","value2",...]}
        },
      },
      "columns": {
        "<table>.<col>": {"is_date": bool, "is_numeric": bool}
      }
    }
    """
    cur = conn.cursor()
    out: Dict[str, Any] = {"tables": {}, "columns": {}}

    # list tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [t[0] for t in cur.fetchall()]

    for t in tables:
        # columns
        cur.execute(f"PRAGMA table_info({t})")
        cols = [row[1] for row in cur.fetchall()]
        out["tables"][t] = {
            "columns": cols,
            "surfaces": _table_surfaces(t),
            "samples": {},
        }
        # collect 8 sample values per column (strings only)
        for c in cols:
            try:
                cur.execute(f"SELECT {c} FROM {t} WHERE {c} IS NOT NULL LIMIT 8")
                vals = [v[0] for v in cur.fetchall()]
                str_vals = [v for v in vals if isinstance(v, str)]
                out["tables"][t]["samples"][c] = str_vals
            except Exception:
                out["tables"][t]["samples"][c] = []

            # type flags
            key = f"{t}.{c}"
            out["columns"][key] = {
                "is_date": _looks_date_sample(out["tables"][t]["samples"][c]) or c.endswith("_date") or c.endswith("_at"),
                "is_numeric": _name_looks_numeric(c),
            }

    return out


# --------------------------------------------
# Helpers
# --------------------------------------------

def _max_id(cur: sqlite3.Cursor, table: str, pk: str) -> int:
    cur.execute(f"SELECT MAX({pk}) FROM {table}")
    v = cur.fetchone()[0]
    return int(v or 0)

def _rand_date(r: random.Random, year: int = 2025, spread_days: int = 365) -> str:
    base = dt.date(year, 1, 1)
    d = base + dt.timedelta(days=r.randint(0, max(1, spread_days)))
    return d.isoformat()

def _add_days(date_iso: str, k: int) -> str:
    d = dt.date.fromisoformat(date_iso) + dt.timedelta(days=k)
    return d.isoformat()

def _name_looks_numeric(col: str) -> bool:
    col = col.lower()
    return any(tok in col for tok in ["amount", "limit", "value", "sum", "count", "price", "qty", "quantity"]) or \
           col.endswith("_id") or col.endswith("_count")

def _looks_date_sample(samples: List[str]) -> bool:
    for v in samples:
        if isinstance(v, str) and len(v) >= 10 and v[4] == "-" and v[7] == "-":
            return True
    return False

def _table_surfaces(t: str) -> List[str]:
    """
    Light synonyms per table to help the predictor (no heavy NLP here).
    """
    base = {t}
    if t == "organizations":
        base |= {"organization", "org", "company", "customer", "client", "party", "policyholder", "organizations"}
    elif t == "policies":
        base |= {"policy", "contract", "policies"}
    elif t == "claims":
        base |= {"claim", "loss", "case", "claims"}
    elif t == "users":
        base |= {"user", "account", "member", "users", "accounts", "members"}
    return sorted(base)
