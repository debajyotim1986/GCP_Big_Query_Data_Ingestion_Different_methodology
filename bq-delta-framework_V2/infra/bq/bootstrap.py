"""
infra/bq/bootstrap.py
======================
Creates the BigQuery dataset and all 6 framework tables.

Usage
-----
    uv run python infra/bq/bootstrap.py --project my-proj --location US --env dev
    uv run python infra/bq/bootstrap.py --project my-proj --dry-run

Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
"""

from __future__ import annotations

import argparse
import logging
import sys

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# ── DDL statements ─────────────────────────────────────────────────────────────

DDL_TABLE_REGISTRY = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.table_registry`
(
    registry_id         STRING          NOT NULL,   -- e.g. 'sales_orders_v1'

    -- Source coordinates
    source_project      STRING          NOT NULL,
    source_dataset      STRING          NOT NULL,
    source_table        STRING          NOT NULL,

    -- Target coordinates
    target_project      STRING          NOT NULL,
    target_dataset      STRING          NOT NULL,
    target_table        STRING          NOT NULL,

    -- Delta detection config
    primary_key_cols    ARRAY<STRING>   NOT NULL,   -- ['order_id','tenant_id']
    delta_hash_cols     ARRAY<STRING>   NOT NULL,   -- [] = hash all columns
    watermark_col       STRING          NOT NULL,   -- 'updated_at'

    -- Runtime options
    batch_size          INT64,                       -- NULL = no row limit
    is_active           BOOL            NOT NULL     DEFAULT TRUE,
    priority            INT64                        DEFAULT 100,
    owner_team          STRING,
    tags                ARRAY<STRING>,

    -- Audit
    created_at          TIMESTAMP       NOT NULL     DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP       NOT NULL     DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = 'BQ Delta Framework — registered source/target table pairs'
)
"""

DDL_DQ_RULES = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dq_rules`
(
    rule_id             STRING          NOT NULL,   -- 'dq_so_001'
    registry_id         STRING          NOT NULL,   -- FK → table_registry
    rule_name           STRING          NOT NULL,
    rule_type           STRING          NOT NULL,
    -- Valid: NOT_NULL | RANGE | REGEX | UNIQUENESS | REFERENTIAL | CUSTOM_SQL

    target_column       STRING,                     -- NULL for CUSTOM_SQL / multi-col
    rule_params         JSON,
    -- NOT_NULL:     {}
    -- RANGE:        {"min": 0, "max": 1000000}
    -- REGEX:        {"pattern": "^[A-Z]{2}$"}
    -- UNIQUENESS:   {} (uses target_column or PK cols)
    -- REFERENTIAL:  {"ref_table": "proj.ds.tbl", "ref_col": "id"}
    -- CUSTOM_SQL:   {"sql": "SELECT * FROM {{staging}} WHERE ..."}

    action_on_fail      STRING          NOT NULL,
    -- Valid: REJECT | QUARANTINE | WARN

    rule_order          INT64                        DEFAULT 100,
    is_active           BOOL            NOT NULL     DEFAULT TRUE,
    description         STRING,

    created_at          TIMESTAMP       NOT NULL     DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP       NOT NULL     DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = 'BQ Delta Framework — DQ rules per registered table'
)
"""

DDL_PROCESS_CONTROL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.process_control`
(
    registry_id             STRING      NOT NULL,   -- PK

    -- Watermark state
    last_successful_run     TIMESTAMP,              -- NULL on first run
    last_run_id             STRING,
    last_row_count          INT64,

    -- Lock state
    status                  STRING,
    -- Valid: RUNNING | SUCCESS | FAILED | DQ_BLOCKED
    locked_at               TIMESTAMP,              -- NULL when unlocked
    locked_by               STRING,                 -- run_id holding lock
    dag_run_id              STRING,
    lock_expiry_hours       INT64                   DEFAULT 4,

    -- Audit
    created_at              TIMESTAMP   NOT NULL    DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP   NOT NULL    DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = 'BQ Delta Framework — watermark and concurrency lock state per table'
)
"""

DDL_RUN_AUDIT = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.run_audit`
(
    run_id              STRING          NOT NULL,   -- UUID per pipeline run
    registry_id         STRING          NOT NULL,
    dag_run_id          STRING,

    -- Classification
    status              STRING          NOT NULL,
    -- Valid: RUNNING | SUCCESS | NO_DELTA | FAILED | DQ_BLOCKED | LOCK_CONFLICT
    load_mode           STRING,
    -- Valid: FULL_LOAD | INCREMENTAL

    -- Extraction window
    watermark_from      TIMESTAMP,
    watermark_to        TIMESTAMP,

    -- Metrics
    rows_extracted      INT64,
    rows_affected       INT64,
    rows_quarantined    INT64,
    dq_rules_passed     INT64,
    dq_rules_failed     INT64,
    duration_secs       FLOAT64,

    -- Error info
    error_message       STRING,
    error_type          STRING,

    -- Timestamps
    started_at          TIMESTAMP,
    ended_at            TIMESTAMP
)
PARTITION BY DATE(started_at)
OPTIONS (
    partition_expiration_days = 365,
    description = 'BQ Delta Framework — per-run audit trail'
)
"""

DDL_DQ_ERROR_LOG = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dq_error_log`
(
    error_id            STRING          NOT NULL,   -- UUID per DQ failure event
    run_id              STRING          NOT NULL,
    registry_id         STRING          NOT NULL,
    rule_id             STRING          NOT NULL,
    rule_name           STRING,
    rule_type           STRING,
    action_on_fail      STRING,
    failed_row_count    INT64,
    sample_failing_sql  STRING,
    error_message       STRING,
    logged_at           TIMESTAMP
)
PARTITION BY DATE(logged_at)
OPTIONS (
    partition_expiration_days = 365,
    description = 'BQ Delta Framework — DQ rule failure details per run'
)
"""

DDL_QUARANTINE_RECORDS = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.quarantine_records`
(
    quarantine_id       STRING          NOT NULL,   -- UUID per quarantine row
    registry_id         STRING          NOT NULL,
    run_id              STRING          NOT NULL,
    rule_id             STRING,
    rule_name           STRING,

    -- State
    status              STRING          NOT NULL,
    -- Valid: PENDING | RESOLVED | REJECTED
    payload             JSON,                       -- original failing row as JSON

    -- Timestamps + resolution
    quarantined_at      TIMESTAMP       NOT NULL,
    resolved_at         TIMESTAMP,
    resolved_by         STRING,
    resolution_note     STRING
)
PARTITION BY DATE(quarantined_at)
OPTIONS (
    partition_expiration_days = 730,
    description = 'BQ Delta Framework — rows quarantined by QUARANTINE-action DQ rules'
)
"""


# ── Table list ─────────────────────────────────────────────────────────────────

TABLES = [
    ("table_registry",    DDL_TABLE_REGISTRY),
    ("dq_rules",          DDL_DQ_RULES),
    ("process_control",   DDL_PROCESS_CONTROL),
    ("run_audit",         DDL_RUN_AUDIT),
    ("dq_error_log",      DDL_DQ_ERROR_LOG),
    ("quarantine_records",DDL_QUARANTINE_RECORDS),
]


# ── Bootstrap logic ────────────────────────────────────────────────────────────

def bootstrap(project: str, dataset: str, location: str, dry_run: bool = False) -> None:
    client = bigquery.Client(project=project, location=location)

    # 1. Ensure dataset exists
    dataset_ref = f"{project}.{dataset}"
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset already exists: %s", dataset_ref)
    except Exception:
        if dry_run:
            logger.info("[DRY RUN] Would create dataset: %s", dataset_ref)
        else:
            bq_dataset = bigquery.Dataset(dataset_ref)
            bq_dataset.location = location
            client.create_dataset(bq_dataset, exists_ok=True)
            logger.info("Dataset created: %s (%s)", dataset_ref, location)

    # 2. Create each table
    for table_name, ddl_template in TABLES:
        ddl = ddl_template.format(project=project, dataset=dataset)
        if dry_run:
            logger.info("[DRY RUN] Would create: %s.%s", dataset, table_name)
            continue
        try:
            client.query(ddl).result()
            logger.info("✓ Table ready: %s.%s", dataset, table_name)
        except Exception as exc:
            logger.error("✗ Failed to create %s.%s: %s", dataset, table_name, exc)
            raise

    if not dry_run:
        logger.info("Bootstrap complete — %d tables in %s", len(TABLES), dataset_ref)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Bootstrap BQ Delta Framework tables")
    parser.add_argument("--project",  required=True,                   help="GCP project ID")
    parser.add_argument("--dataset",  default="framework_config",      help="BQ dataset name")
    parser.add_argument("--location", default="US",                    help="BQ dataset location")
    parser.add_argument("--env",      default="",                      help="Environment tag (info only)")
    parser.add_argument("--dry-run",  action="store_true",             help="Print DDL without executing")
    args = parser.parse_args()

    if args.env:
        logger.info("Environment: %s", args.env)

    bootstrap(
        project=args.project,
        dataset=args.dataset,
        location=args.location,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
