
import sqlite3, random, datetime as dt
def create_demo_connection(policies:int=2000, claims:int=5000, seed:int=7, *args, **kwargs):
    r = random.Random(seed)
    check = bool(kwargs.get("check_same_thread", False))
    conn = sqlite3.connect(":memory:", check_same_thread=check)
    cur = conn.cursor()
    cur.executescript("""    PRAGMA foreign_keys=ON;
    CREATE TABLE organizations(org_id INTEGER PRIMARY KEY, org_code TEXT UNIQUE, org_name TEXT, city TEXT, country_code TEXT);
    CREATE TABLE policies(policy_id INTEGER PRIMARY KEY, policy_number TEXT UNIQUE, org_id INTEGER REFERENCES organizations(org_id),
                          inception_date TEXT, expiry_date TEXT, currency TEXT, status TEXT, credit_limit REAL, org_name_dn TEXT);
    CREATE TABLE claims(claim_id INTEGER PRIMARY KEY, policy_id INTEGER REFERENCES policies(policy_id),
                        claim_number TEXT UNIQUE, created_at TEXT, amount REAL, status TEXT);
    CREATE TABLE users(user_id INTEGER PRIMARY KEY, username TEXT UNIQUE, role TEXT);
    """ )
    # orgs
    cities = ["London","Cardiff","Amsterdam","Madrid","Paris","Berlin","Rome","Dublin","Prague","Lisbon"]
    def comp(): return f"{random.choice(['Alpha','Beta','Gamma','Orion','Vega','Nova'])} {random.choice(['Trading','Exports','Holdings'])} {random.choice(['Ltd','BV','GmbH'])}"
    org_rows = [(i, f"ORG-{i:04d}", comp(), random.choice(cities), random.choice(["GB","NL","ES","FR","DE","IE"])) for i in range(1, max(50, policies//10)+1)]
    cur.executemany("INSERT INTO organizations VALUES (?,?,?,?,?)", org_rows)
    # policies
    base = dt.date(2024,1,1)
    pol_rows = []
    for pid in range(1, policies+1):
        org_id = random.randint(1, len(org_rows))
        inc = base + dt.timedelta(days=random.randint(0,365))
        exp = inc + dt.timedelta(days=random.randint(180,540))
        status = random.choices(["ACTIVE","EXPIRED","CANCELLED","PENDING"], weights=[0.55,0.25,0.1,0.1])[0]
        credit = round(random.uniform(10000, 500000),2)
        pol_rows.append((pid, f"POL-{pid:06d}", org_id, inc.isoformat(), exp.isoformat(), random.choice(["GBP","EUR","USD"]), status, credit, org_rows[org_id-1][2]))
    cur.executemany("INSERT INTO policies VALUES (?,?,?,?,?,?,?,?,?)", pol_rows)
    # claims
    cl_rows = []
    for cid in range(1, claims+1):
        pid = random.randint(1, policies)
        created = base + dt.timedelta(days=random.randint(60,600))
        status = random.choices(["OPEN","CLOSED","PENDING","REJECTED"], weights=[0.35,0.45,0.15,0.05])[0]
        amount = round(random.uniform(500, 100000),2)
        cl_rows.append((cid, pid, f"CLM-{cid:06d}", created.isoformat(), amount, status))
    cur.executemany("INSERT INTO claims VALUES (?,?,?,?,?,?)", cl_rows)
    cur.executemany("INSERT INTO users VALUES (?,?,?)", [(1,"admin","ADMIN"), (2,"dev1","DEV"), (3,"analyst","ANALYST")])
    conn.commit(); return conn

def run_sql(conn, sql: str, row_limit: int | None = None):
    """
    Execute SQL. If 'row_limit' is provided and the SQL has no LIMIT, append it.
    Otherwise run SQL as-is (no automatic LIMIT).
    """
    s = sql.strip().rstrip(";")
    if row_limit is not None:
        low = s.lower()
        if low.startswith("select") and " limit " not in low:
            s = f"{s}\nLIMIT {int(row_limit)}"
    cur = conn.cursor()
    try:
        cur.execute(s)
        if cur.description is None:
            conn.commit()
            return [], []
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    except Exception as e:
        return ["error", "sql"], [(str(e), s)]


def schema_introspect(conn):
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    return {t: [r[1] for r in cur.execute(f"PRAGMA table_info({t})")] for t in tables}
