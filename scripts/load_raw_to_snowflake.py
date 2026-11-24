"""Load raw parquet data into Snowflake RAW schema.

Run from the repo root:
    uv run python scripts/load_raw_to_snowflake.py

Uses batched INSERT statements (no PUT/stage required).
Idempotent: re-running replaces all data (CREATE OR REPLACE TABLE).
"""

import glob
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "raw"
PROFILES_YML = REPO_ROOT / "analytics" / "profiles.yml"
TARGET_SCHEMA = "RAW"

EXPECTED_POSTINGS_ROWS = 843_097
EXPECTED_SKILLS_ROWS = 3_304_574

BATCH_SIZE = 10_000


# ── connection helpers ──────────────────────────────────────────────


def load_profile(
    profiles_path: Path, profile: str = "analytics", target: str = "dev"
) -> dict:
    """Read Snowflake connection params from profiles.yml."""
    with open(profiles_path) as f:
        profiles = yaml.safe_load(f)
    return profiles[profile]["outputs"][target]


def load_private_key(key_path: str, passphrase: str) -> bytes:
    """Load and decrypt RSA private key, return DER bytes."""
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode(),
            backend=default_backend(),
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection(profile: dict) -> snowflake.connector.SnowflakeConnection:
    """Create Snowflake connection from profile dict."""
    pk_bytes = load_private_key(
        profile["private_key_path"],
        profile["private_key_passphrase"],
    )
    return snowflake.connector.connect(
        account=profile["account"],
        user=profile["user"],
        private_key=pk_bytes,
        role=profile["role"],
        warehouse=profile["warehouse"],
        database=profile["database"],
        schema=TARGET_SCHEMA,
    )


# ── DDL ─────────────────────────────────────────────────────────────

DDL_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"

DDL_JOB_POSTINGS = f"""
CREATE OR REPLACE TABLE {TARGET_SCHEMA}.RAW_JOB_POSTINGS (
    JOB_ID                      VARCHAR(32),
    JOB_TITLE                   VARCHAR,
    COMPANY_NAME                VARCHAR,
    JOB_LOCATION                VARCHAR,
    JOB_VIA                     VARCHAR,
    JOB_POSTED_AT               VARCHAR,
    JOB_SALARY                  VARCHAR,
    JOB_SCHEDULE_TYPE           VARCHAR,
    JOB_WORK_FROM_HOME          BOOLEAN,
    JOB_EXTENSIONS_RAW          VARCHAR,
    JOB_DETECTED_EXTENSIONS_RAW VARCHAR,
    SEARCH_TIME                 TIMESTAMP_TZ,
    SEARCH_DATE                 DATE,
    SEARCH_TERM                 VARCHAR,
    SEARCH_LOCATION             VARCHAR,
    ERROR                       BOOLEAN,
    ERROR_MESSAGE               VARCHAR
)
"""

DDL_JOB_SKILLS = f"""
CREATE OR REPLACE TABLE {TARGET_SCHEMA}.RAW_JOB_SKILLS (
    JOB_ID        VARCHAR(32),
    SKILL_KEYWORD VARCHAR,
    SKILL_ID      VARCHAR(32)
)
"""


# ── loading helpers ─────────────────────────────────────────────────


def clean_value(val):
    """Convert pandas/numpy special values to Python-native types."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (np.bool_, bool)):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    if hasattr(val, "item"):  # numpy scalar
        return val.item()
    return val


def batch_insert(cur, table: str, ncols: int, df: pd.DataFrame) -> int:
    """Insert a DataFrame into a Snowflake table in batches."""
    placeholders = ", ".join(["%s"] * ncols)
    sql = f"INSERT INTO {TARGET_SCHEMA}.{table} VALUES ({placeholders})"

    total = len(df)
    inserted = 0
    for start in range(0, total, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total)
        chunk = df.iloc[start:end]
        rows = [
            tuple(clean_value(v) for v in row) for row in chunk.itertuples(index=False)
        ]
        cur.executemany(sql, rows)
        inserted += len(rows)
        if inserted % 50_000 == 0 or inserted == total:
            log.info("  %s: %d / %d rows inserted", table, inserted, total)
    return inserted


def load_job_postings(cur) -> int:
    """Read all monthly parquet files and insert into Snowflake."""
    pattern = str(DATA_DIR / "raw_job_postings_*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No parquet files matching {pattern}")

    log.info("Reading %d job_postings parquet files ...", len(files))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    log.info("  DataFrame shape: %s", df.shape)

    return batch_insert(cur, "RAW_JOB_POSTINGS", 17, df)


def load_job_skills(cur) -> int:
    """Read skills parquet file and insert into Snowflake."""
    path = DATA_DIR / "raw_job_skills.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")

    log.info("Reading job_skills parquet ...")
    df = pd.read_parquet(path)
    log.info("  DataFrame shape: %s", df.shape)

    return batch_insert(cur, "RAW_JOB_SKILLS", 3, df)


# ── verification ────────────────────────────────────────────────────


def verify_counts(cur) -> bool:
    """Compare Snowflake row counts against expected values."""
    ok = True
    for table, expected in [
        ("RAW_JOB_POSTINGS", EXPECTED_POSTINGS_ROWS),
        ("RAW_JOB_SKILLS", EXPECTED_SKILLS_ROWS),
    ]:
        cur.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{table}")
        actual = cur.fetchone()[0]
        status = "OK" if actual == expected else "MISMATCH"
        log.info(
            "  %s.%s: %s (expected=%d, actual=%d)",
            TARGET_SCHEMA,
            table,
            status,
            expected,
            actual,
        )
        if actual != expected:
            ok = False
    return ok


# ── main ────────────────────────────────────────────────────────────


def main() -> int:
    profile = load_profile(PROFILES_YML)
    log.info(
        "Connecting to Snowflake account=%s, database=%s",
        profile["account"],
        profile["database"],
    )
    conn = get_connection(profile)

    try:
        cur = conn.cursor()

        log.info("Creating schema %s ...", TARGET_SCHEMA)
        cur.execute(DDL_SCHEMA)

        log.info("Creating table RAW_JOB_POSTINGS ...")
        cur.execute(DDL_JOB_POSTINGS)

        log.info("Creating table RAW_JOB_SKILLS ...")
        cur.execute(DDL_JOB_SKILLS)

        cur.execute(f"USE DATABASE {profile['database']}")
        cur.execute(f"USE SCHEMA {TARGET_SCHEMA}")
        cur.execute(f"USE WAREHOUSE \"{profile['warehouse']}\"")

        load_job_postings(cur)
        load_job_skills(cur)

        log.info("Verifying row counts ...")
        if verify_counts(cur):
            log.info("All counts match. Load complete.")
            return 0
        else:
            log.error("Row count mismatch detected!")
            return 1
    except Exception:
        log.exception("Load failed")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
