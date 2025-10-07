d
TEMPLATES = [
    {"q":"how many policies are active","sql":"SELECT COUNT(*) AS active_policies FROM policies WHERE status='ACTIVE'"},
    {"q":"list top {k} organizations by total credit limit","sql":"SELECT o.org_name, SUM(p.credit_limit) AS total_limit FROM organizations o JOIN policies p ON p.org_id=o.org_id GROUP BY o.org_name ORDER BY total_limit DESC LIMIT {k}"},
    {"q":"show claims for policy {policy_number}","sql":"SELECT c.claim_number, c.created_at, c.amount, c.status FROM claims c JOIN policies p ON p.policy_id=c.policy_id WHERE p.policy_number = '{policy_number}' ORDER BY c.created_at DESC"},
    {"q":"which policies expired in {year}","sql":"SELECT policy_number, expiry_date, status FROM policies WHERE substr(expiry_date,1,4) = '{year}' ORDER BY expiry_date DESC"},
    {"q":"find organizations in {city}","sql":"SELECT org_code, org_name, city, country_code FROM organizations WHERE lower(city) LIKE '%{city_lc}%' ORDER BY org_name LIMIT 100"}
]
