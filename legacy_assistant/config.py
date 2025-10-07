
from dataclasses import dataclass
@dataclass
class AppConfig:
    demo_rows_policies: int = 2000
    demo_rows_claims: int = 5000
    row_limit_default: int = 500
