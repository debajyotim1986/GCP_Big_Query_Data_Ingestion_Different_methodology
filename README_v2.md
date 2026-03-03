# BQ Delta Ingestion Framework

> **Production-grade, metadata-driven delta ingestion framework for BigQuery.**
> Orchestrated by Cloud Composer 2, with built-in DQ validation, quarantine management,
> watermark-based incremental loads, and a Cloud Run observability dashboard.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Project Structure](#3-project-structure)
4. [Prerequisites](#4-prerequisites)
5. [Quick Start — Local Development](#5-quick-start--local-development)
6. [BigQuery Schema](#6-bigquery-schema)
7. [Framework Components](#7-framework-components)
   - [ConfigLoader](#71-configloader)
   - [WatermarkManager](#72-watermarkmanager)
   - [DeltaDetector](#73-deltadector)
   - [DQValidator](#74-dqvalidator)
   - [QuarantineManager](#75-quarantinemanager)
   - [IngestionEngine](#76-ingestionengine)
   - [AuditLogger](#77-auditlogger)
   - [PipelineRunner](#78-pipelinerunner)
8. [Pipeline Execution Flow](#8-pipeline-execution-flow)
9. [Watermark & Concurrency Logic](#9-watermark--concurrency-logic)
10. [Data Quality Rules](#10-data-quality-rules)
11. [Cloud Composer DAG](#11-cloud-composer-dag)
12. [Dashboard — Cloud Run](#12-dashboard--cloud-run)
13. [Docker Image](#13-docker-image)
14. [CI/CD — Cloud Build](#14-cicd--cloud-build)
15. [IAM & Permissions](#15-iam--permissions)
16. [Configuration Reference](#16-configuration-reference)
17. [Onboarding a New Table](#17-onboarding-a-new-table)
18. [Testing](#18-testing)
19. [Troubleshooting](#19-troubleshooting)
20. [Glossary](#20-glossary)

---

## 1. Overview

The **BQ Delta Ingestion Framework** solves a common enterprise data engineering problem: reliably ingesting only *changed* rows from a source BigQuery table into a target table, with full audit trails, data quality gates, and no data gaps — even across failures.

### Core capabilities

| Capability | Description |
|---|---|
| **Metadata-driven** | All tables registered in `table_registry` — no code changes to add new tables |
| **Delta detection** | SHA256 row hashing — only inserts/updates are merged, unchanged rows are skipped |
| **Watermark management** | Atomic lock-and-watermark via BigQuery MERGE — guarantees no data window is skipped |
| **DQ validation** | 6 rule types (NOT_NULL, RANGE, REGEX, UNIQUENESS, REFERENTIAL, CUSTOM_SQL) with REJECT / QUARANTINE / WARN actions |
| **Quarantine** | Failing rows isolated to `quarantine_records` and removed from ingestion — does not block clean rows |
| **Audit trail** | Every run writes to `run_audit`; every DQ failure writes to `dq_error_log` |
| **Observability** | Cloud Run dashboard with KPIs, run history, table status, quarantine view |
| **Idempotent** | Safe to re-run — staging tables are always WRITE_TRUNCATE, MERGE is upsert-safe |

### Technology stack

| Layer | Technology |
|---|---|
| Data warehouse | Google BigQuery |
| Orchestration | Cloud Composer 2 (Apache Airflow 2.8) |
| Framework runtime | Python 3.11 + `google-cloud-bigquery` |
| Package management | [uv](https://github.com/astral-sh/uv) |
| Dashboard | FastAPI + Cloud Run |
| CI/CD | Cloud Build |
| Container registry | Artifact Registry |

---

## 2. Architecture

```
Cloud Scheduler
      │  (daily trigger)
      ▼
Cloud Composer 2 (Airflow DAG)
      │
      ├── discover_tables  ─── reads table_registry ───► list of registry_ids
      │
      ├── run_pipeline[table_A]  ─┐
      ├── run_pipeline[table_B]  ─┤  (parallel — up to MAX_PAR concurrent)
      └── run_pipeline[table_N]  ─┘
                │
                ▼
         PipelineRunner (7 steps)
                │
    ┌───────────▼───────────────────────────────────────┐
    │  Step 1  ConfigLoader      reads table_registry   │
    │  Step 2  WatermarkManager  acquire_lock (MERGE)   │
    │  Step 3  DeltaDetector     extract → _staging_*   │
    │  Step 4  DQValidator       evaluate all DQ rules  │
    │  Step 5  QuarantineManager isolate failing rows   │
    │  Step 6  IngestionEngine   MERGE staging → target │
    │  Step 7  WatermarkManager  advance watermark      │
    └───────────────────────────────────────────────────┘
                │
                ▼
         framework_config dataset (BigQuery)
    ┌───────────────────────────────────────────────────┐
    │  table_registry     process_control               │
    │  dq_rules           run_audit                     │
    │  dq_error_log       quarantine_records            │
    └───────────────────────────────────────────────────┘
                │
                ▼
         Cloud Run Dashboard ──► team / on-call visibility
```

### Key design decisions

- **No Dataflow / no Spark** — All computation runs inside BigQuery via SQL jobs. No data ever leaves BigQuery.
- **Atomic locking via MERGE** — The `acquire_lock` MERGE is a single atomic BQ DML operation. No external lock service is needed.
- **Watermark-first design** — `last_successful_run` only advances on `SUCCESS`. `FAILED` and `DQ_BLOCKED` runs always re-scan the same window, guaranteeing completeness.
- **Staging-then-merge pattern** — A temporary staging table isolates each run's delta. Quarantine rows are deleted from staging before the final MERGE, so they never land in the target.

---

## 3. Project Structure

```
bq-delta-framework/
│
├── framework/                          # Core framework engine
│   ├── __init__.py                     # Public API surface
│   ├── exceptions.py                   # Typed exception hierarchy
│   ├── config_loader.py                # Reads table_registry + dq_rules
│   ├── watermark_manager.py            # Lock acquisition + watermark updates
│   ├── delta_detector.py               # SHA256 delta extraction → staging table
│   ├── dq_validator.py                 # DQ rule evaluation (6 rule types)
│   ├── quarantine_manager.py           # QUARANTINE row isolation
│   ├── ingestion_engine.py             # Final MERGE staging → target
│   ├── audit_logger.py                 # Writes run_audit + dq_error_log
│   └── pipeline_runner.py              # 7-step orchestrator (top-level entry point)
│
├── dags/
│   └── bq_delta_ingestion_dag.py       # Cloud Composer 2 / Airflow 2.8 DAG
│
├── dashboard/
│   ├── __init__.py
│   └── app.py                          # FastAPI — 10 endpoints + embedded SPA
│
├── infra/
│   ├── bq/
│   │   └── bootstrap.py                # Creates BQ dataset + 6 framework tables
│   └── deploy/
│       ├── cloudbuild.yaml             # Cloud Build CI/CD pipeline
│       └── service.yaml                # Cloud Run declarative service spec
│
├── tests/
│   └── test_pipeline.py                # Unit tests (pytest, mocked BQ client)
│
├── Dockerfile                          # Multi-stage build (builder + runtime)
├── pyproject.toml                      # uv project — dependency groups
├── .env.example                        # Local dev environment template
└── README.md                           # This file
```

---

## 4. Prerequisites

### Required tooling

| Tool | Version | Purpose |
|---|---|---|
| Python | ≥ 3.11 | Runtime |
| [uv](https://github.com/astral-sh/uv) | ≥ 0.5 | Package manager |
| [gcloud CLI](https://cloud.google.com/sdk/docs/install) | latest | GCP authentication + deployment |
| Docker | ≥ 24 | Local image build |

### Required GCP services

| Service | Purpose |
|---|---|
| BigQuery | All data storage and processing |
| Cloud Composer 2 | DAG orchestration |
| Cloud Run | Dashboard hosting |
| Artifact Registry | Docker image storage |
| Cloud Build | CI/CD |
| Cloud Monitoring | Alerting (optional) |

### Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv --version   # verify: uv 0.5.x
```

---

## 5. Quick Start — Local Development

### Step 1 — Clone and set up Python environment

```bash
git clone https://github.com/your-org/bq-delta-framework.git
cd bq-delta-framework

# Pin Python 3.11 (matches Cloud Composer 2.x)
uv python pin 3.11

# Create virtual environment
uv venv --python 3.11
source .venv/bin/activate

# Install all dependency groups (framework + dashboard + dev)
uv sync --all-groups
```

### Step 2 — Configure environment

```bash
cp .env.example .env.dev
# Edit .env.dev and set GCP_PROJECT at minimum
source .env.dev
```

### Step 3 — Authenticate with GCP

```bash
gcloud auth application-default login
gcloud config set project $GCP_PROJECT
```

### Step 4 — Bootstrap BigQuery tables

```bash
uv run python infra/bq/bootstrap.py \
  --project $GCP_PROJECT \
  --location US \
  --env dev
```

Expected output:
```
✓ Dataset framework_config created
✓ Table ready: framework_config.table_registry
✓ Table ready: framework_config.dq_rules
✓ Table ready: framework_config.process_control
✓ Table ready: framework_config.run_audit
✓ Table ready: framework_config.dq_error_log
✓ Table ready: framework_config.quarantine_records
Bootstrap complete — 6 tables in my-project.framework_config
```

### Step 5 — Run the dashboard locally

```bash
uv run uvicorn dashboard.app:app --reload --port 8080
# Open http://localhost:8080
```

### Step 6 — Run a pipeline manually

```python
from framework import PipelineRunner

runner = PipelineRunner(project="my-project-dev")
result = runner.run("sales_orders_v1", dag_run_id="manual_test_001")

print(result.status)          # SUCCESS | NO_DELTA | FAILED | DQ_BLOCKED
print(result.rows_affected)   # rows merged to target
print(result.duration_secs)   # wall time
```

---

## 6. BigQuery Schema

All framework state is stored in a single BigQuery dataset: **`framework_config`**

### 6.1 `table_registry` — registered source/target pairs

| Column | Type | Description |
|---|---|---|
| `registry_id` | STRING | Unique identifier, e.g. `sales_orders_v1` |
| `source_project` | STRING | GCP project of source table |
| `source_dataset` | STRING | Dataset of source table |
| `source_table` | STRING | Source table name |
| `target_project` | STRING | GCP project of target table |
| `target_dataset` | STRING | Dataset of target table |
| `target_table` | STRING | Target table name |
| `primary_key_cols` | ARRAY\<STRING\> | PK columns used for MERGE join |
| `delta_hash_cols` | ARRAY\<STRING\> | Columns to hash for change detection (empty = hash PKs) |
| `watermark_col` | STRING | Timestamp column for incremental extraction |
| `batch_size` | INT64 | Max rows per run (NULL = unlimited) |
| `is_active` | BOOL | Whether the table is included in DAG runs |
| `priority` | INT64 | Lower = runs first (default: 100) |
| `owner_team` | STRING | Team owning this table (for alerting routing) |
| `tags` | ARRAY\<STRING\> | Free-form tags, e.g. `['finance','critical']` |

**Sample insert:**

```sql
INSERT INTO `my-project.framework_config.table_registry`
VALUES (
  'sales_orders_v1',
  'src-project', 'raw', 'sales_orders',
  'tgt-project', 'curated', 'sales_orders',
  ['order_id'],
  ['order_id', 'status', 'amount', 'updated_at'],
  'updated_at',
  NULL,   -- no batch_size limit
  TRUE,
  100,
  'data-engineering',
  ['finance', 'critical'],
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);
```

### 6.2 `dq_rules` — data quality rules per table

| Column | Type | Description |
|---|---|---|
| `rule_id` | STRING | Unique rule identifier, e.g. `dq_so_001` |
| `registry_id` | STRING | FK → `table_registry` |
| `rule_name` | STRING | Human-readable label |
| `rule_type` | STRING | `NOT_NULL` \| `RANGE` \| `REGEX` \| `UNIQUENESS` \| `REFERENTIAL` \| `CUSTOM_SQL` |
| `target_column` | STRING | Column to validate (NULL for multi-col / CUSTOM_SQL) |
| `rule_params` | JSON | Rule-specific parameters (see Section 10) |
| `action_on_fail` | STRING | `REJECT` \| `QUARANTINE` \| `WARN` |
| `rule_order` | INT64 | Execution sequence (lower = runs first) |
| `is_active` | BOOL | Whether rule is included in validation |

### 6.3 `process_control` — watermark and lock state

| Column | Type | Description |
|---|---|---|
| `registry_id` | STRING | PK |
| `last_successful_run` | TIMESTAMP | Last SUCCESS watermark — NULL on first run |
| `last_run_id` | STRING | UUID of the last successful run |
| `last_row_count` | INT64 | Row count of the last successful run |
| `status` | STRING | `RUNNING` \| `SUCCESS` \| `FAILED` \| `DQ_BLOCKED` |
| `locked_at` | TIMESTAMP | When the current lock was acquired (NULL if unlocked) |
| `locked_by` | STRING | `run_id` holding the current lock |
| `lock_expiry_hours` | INT64 | Hours before a stale lock auto-expires (default: 4) |

### 6.4 `run_audit` — per-run audit trail (partitioned by `started_at`)

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | UUID per pipeline execution |
| `registry_id` | STRING | Table this run belongs to |
| `status` | STRING | `RUNNING` \| `SUCCESS` \| `NO_DELTA` \| `FAILED` \| `DQ_BLOCKED` \| `LOCK_CONFLICT` |
| `load_mode` | STRING | `FULL_LOAD` \| `INCREMENTAL` |
| `watermark_from` | TIMESTAMP | Extraction window start |
| `watermark_to` | TIMESTAMP | Extraction window end |
| `rows_extracted` | INT64 | Rows written to staging table |
| `rows_affected` | INT64 | Rows inserted + updated in target |
| `rows_quarantined` | INT64 | Rows sent to quarantine |
| `dq_rules_passed` | INT64 | Number of DQ rules that passed |
| `dq_rules_failed` | INT64 | Number of DQ rules that failed |
| `duration_secs` | FLOAT64 | Wall time in seconds |
| `error_message` | STRING | Error detail on failure |
| `started_at` | TIMESTAMP | Run start time (partition column) |
| `ended_at` | TIMESTAMP | Run end time |

### 6.5 `dq_error_log` — DQ rule failures per run (partitioned by `logged_at`)

| Column | Type | Description |
|---|---|---|
| `error_id` | STRING | UUID per DQ failure event |
| `run_id` | STRING | FK → `run_audit` |
| `rule_id` | STRING | Which rule failed |
| `rule_type` | STRING | Rule type |
| `action_on_fail` | STRING | `REJECT` \| `QUARANTINE` \| `WARN` |
| `failed_row_count` | INT64 | Number of rows that failed the rule |
| `sample_failing_sql` | STRING | SQL that identified failing rows |

### 6.6 `quarantine_records` — quarantined rows (partitioned by `quarantined_at`)

| Column | Type | Description |
|---|---|---|
| `quarantine_id` | STRING | UUID per quarantined row |
| `registry_id` | STRING | Source table |
| `run_id` | STRING | Run that quarantined this row |
| `rule_id` | STRING | DQ rule that triggered quarantine |
| `status` | STRING | `PENDING` \| `RESOLVED` \| `REJECTED` |
| `payload` | JSON | Full original row as JSON |
| `quarantined_at` | TIMESTAMP | When the row was quarantined |
| `resolved_at` | TIMESTAMP | When the row was resolved/rejected |
| `resolved_by` | STRING | Who resolved (user or system) |
| `resolution_note` | STRING | Free-text resolution note |

---

## 7. Framework Components

### 7.1 ConfigLoader

**File:** `framework/config_loader.py`

Reads a table's complete configuration from BigQuery. Returns a typed `TableConfig` dataclass and a list of `DQRule` dataclasses.

```python
from google.cloud import bigquery
from framework.config_loader import ConfigLoader

client = bigquery.Client(project="my-project")
loader = ConfigLoader(client, project="my-project", dataset="framework_config")
config = loader.load("sales_orders_v1")

print(config.load_mode)         # FULL_LOAD or INCREMENTAL
print(config.watermark_from)    # None on first run
print(config.watermark_to)      # datetime.now() at load time
print(config.dq_rules)          # list[DQRule] ordered by rule_order
```

**Key properties on `TableConfig`:**

| Property | Description |
|---|---|
| `source_table_ref` | Full BQ reference: `` `project.dataset.table` `` |
| `target_table_ref` | Full BQ reference for the target table |
| `is_full_load` | `True` when `watermark_from` is `None` |
| `load_mode` | `"FULL_LOAD"` or `"INCREMENTAL"` |
| `effective_hash_cols` | `delta_hash_cols` if non-empty, else `primary_key_cols` |

---

### 7.2 WatermarkManager

**File:** `framework/watermark_manager.py`

Owns all mutations to `process_control`. Uses BigQuery MERGE for atomic lock acquisition.

```python
from framework.watermark_manager import WatermarkManager

mgr = WatermarkManager(client, project="my-project", lock_expiry_hours=4)

# Acquire lock — returns a new run_id UUID
run_id = mgr.acquire_lock("sales_orders_v1", dag_run_id="airflow_run_001")

# On success — advance watermark
mgr.advance_watermark("sales_orders_v1", run_id, watermark_to=config.watermark_to, row_count=1500)

# On failure (in finally block)
mgr.release_lock("sales_orders_v1", run_id, status="FAILED")
```

**Lock conflict behaviour:** If another run holds the lock and has not yet expired (`TIMESTAMP_DIFF(NOW, locked_at, HOUR) < lock_expiry_hours`), the MERGE is a no-op and `_check_lock` detects we don't hold it — `LockConflictError` is raised. The Airflow task completes with `LOCK_CONFLICT` status (non-retried).

---

### 7.3 DeltaDetector

**File:** `framework/delta_detector.py`

Builds and runs the BigQuery SQL that extracts changed rows into a staging table.

**Staging table naming:** `_delta_staging_{registry_id}` (special characters replaced with `_`)

**Change detection logic:**

```sql
-- Simplified representation
INSERT INTO _delta_staging_sales_orders_v1
SELECT
    src.*,
    TO_HEX(SHA256(
        COALESCE(CAST(order_id AS STRING), '') || '|' ||
        COALESCE(CAST(status   AS STRING), '') || '|' ||
        COALESCE(CAST(amount   AS STRING), '')
    )) AS _row_hash,
    CASE WHEN tgt.order_id IS NULL THEN 'INSERT' ELSE 'UPDATE' END AS _change_type,
    CURRENT_TIMESTAMP() AS _extracted_at
FROM `src-project.raw.sales_orders` src
LEFT JOIN `tgt-project.curated.sales_orders` tgt ON src.order_id = tgt.order_id
WHERE src.updated_at > TIMESTAMP('2024-01-01T00:00:00Z')
  AND src.updated_at <= TIMESTAMP('2024-01-02T00:00:00Z')
  AND (tgt.order_id IS NULL OR <hash> != tgt._row_hash)
```

Returns **0** when no changed rows exist — the pipeline skips the MERGE but still advances the watermark (no gap created).

---

### 7.4 DQValidator

**File:** `framework/dq_validator.py`

Evaluates all active DQ rules against the staging table rows. Returns a `DQSummary`.

```python
from framework.dq_validator import DQValidator

validator = DQValidator(client, project="my-project")
summary = validator.validate(config, staging_ref="`proj.fw._stg_sales_orders_v1`", run_id=run_id)

if summary.reject_failures:
    raise DQBlockedError(...)

if summary.quarantine_failures:
    # handle quarantine rows

if summary.warn_failures:
    # log warnings, continue
```

**Rule type summary:**

| Rule Type | `rule_params` | SQL generated |
|---|---|---|
| `NOT_NULL` | `{}` | `WHERE column IS NULL` |
| `RANGE` | `{"min": 0, "max": 1000}` | `WHERE column < 0 OR column > 1000` |
| `REGEX` | `{"pattern": "^[A-Z]{2}$"}` | `WHERE NOT REGEXP_CONTAINS(...)` |
| `UNIQUENESS` | `{}` | `GROUP BY ... HAVING COUNT(*) > 1` |
| `REFERENTIAL` | `{"ref_table": "proj.ds.tbl", "ref_col": "id"}` | `WHERE col NOT IN (SELECT ref_col FROM ref_table)` |
| `CUSTOM_SQL` | `{"sql": "SELECT * FROM {{staging}} WHERE ..."}` | Uses `{{staging}}` placeholder replaced at runtime |

---

### 7.5 QuarantineManager

**File:** `framework/quarantine_manager.py`

Handles rows failing a `QUARANTINE`-action DQ rule. Writes them to `quarantine_records` and removes them from the staging table so they are **never merged to the target**.

```python
from framework.quarantine_manager import QuarantineManager

mgr = QuarantineManager(client, project="my-project")

# Quarantine rows for a failed rule
count = mgr.quarantine_rows(
    config=config,
    run_id=run_id,
    rule_result=dq_result,
    failing_rows=failing_rows,     # list[dict] from DQValidator.get_failing_rows()
    staging_ref=staging_ref,
)

# Downstream remediation
mgr.resolve(quarantine_ids=["uuid-1", "uuid-2"], resolved_by="data-team@company.com", note="Fixed upstream")
mgr.reject(quarantine_ids=["uuid-3"], resolved_by="data-team@company.com", note="Invalid record, will not process")
```

---

### 7.6 IngestionEngine

**File:** `framework/ingestion_engine.py`

Executes the final `MERGE` from staging into the target table.

- Fetches staging table schema dynamically — handles tables with different column counts
- Excludes framework columns (`_row_hash`, `_change_type`, `_extracted_at`) from the INSERT/UPDATE payload
- `WHEN MATCHED AND _row_hash != S._row_hash` — only updates actually changed rows
- `WHEN NOT MATCHED BY TARGET` — inserts new rows

```python
from framework.ingestion_engine import IngestionEngine

engine = IngestionEngine(client, project="my-project")
rows_affected = engine.merge(config, staging_ref="`proj.fw._stg_sales_orders_v1`")
print(rows_affected)   # inserted + updated row count
```

---

### 7.7 AuditLogger

**File:** `framework/audit_logger.py`

Writes structured run events to `run_audit` and `dq_error_log` using BigQuery streaming inserts.

```python
from framework.audit_logger import AuditLogger

logger_bq = AuditLogger(client, project="my-project")

# On run start
logger_bq.write_run_start(registry_id, run_id, dag_run_id, watermark_from, watermark_to, load_mode)

# On run end
logger_bq.write_run_end(
    registry_id, run_id,
    status="SUCCESS",
    started_at=started_at,
    rows_extracted=1500,
    rows_affected=1200,
    rows_quarantined=12,
    dq_summary=dq_summary,
)

# After DQ validation
logger_bq.write_dq_errors(registry_id, run_id, dq_summary)
```

---

### 7.8 PipelineRunner

**File:** `framework/pipeline_runner.py`

Top-level entry point. Wires together all components. **Never raises** — all exceptions are caught, reflected in `PipelineResult.status`, and written to `run_audit`.

```python
from framework.pipeline_runner import PipelineRunner

runner = PipelineRunner(
    project="my-project",
    dataset="framework_config",
    location="US",
    lock_expiry_hours=4,
)

result = runner.run("sales_orders_v1", dag_run_id="airflow_run_20250101")

# PipelineResult fields
result.registry_id       # 'sales_orders_v1'
result.run_id            # UUID
result.status            # SUCCESS | NO_DELTA | FAILED | DQ_BLOCKED | LOCK_CONFLICT
result.load_mode         # FULL_LOAD | INCREMENTAL
result.rows_extracted    # delta count written to staging
result.rows_affected     # rows inserted + updated in target
result.rows_quarantined  # rows sent to quarantine
result.dq_summary        # DQSummary object with per-rule results
result.error_message     # populated on failure
result.duration_secs     # wall time
```

---

## 8. Pipeline Execution Flow

```
PipelineRunner.run("sales_orders_v1")
        │
        ├── Step 1 ── ConfigLoader.load()
        │             Reads table_registry + dq_rules
        │             Resolves watermark window (from process_control)
        │             Returns: TableConfig
        │
        ├── Step 2 ── WatermarkManager.acquire_lock()
        │             Atomic MERGE on process_control
        │             Sets status=RUNNING, locked_by=run_id
        │             Raises: LockConflictError if another run active
        │             Returns: new run_id (UUID)
        │
        ├── Step 3 ── DeltaDetector.extract_delta()
        │             Creates staging table (schema from source)
        │             Runs SHA256 diff query: source LEFT JOIN target
        │             Writes changed rows to _delta_staging_*
        │             Returns: row_count
        │             → If 0: advance watermark + return NO_DELTA
        │
        ├── Step 4 ── DQValidator.validate()
        │             Evaluates all active DQ rules in rule_order sequence
        │             Returns: DQSummary with per-rule pass/fail
        │
        ├── Step 5 ── QuarantineManager (if QUARANTINE failures)
        │             Fetches failing rows from staging
        │             Inserts to quarantine_records
        │             Deletes from staging table
        │
        │         ── If REJECT failures → raise DQBlockedError
        │
        ├── Step 6 ── IngestionEngine.merge()
        │             MERGE _delta_staging_* → target table
        │             WHEN MATCHED + hash changed → UPDATE
        │             WHEN NOT MATCHED → INSERT
        │             Returns: rows_affected
        │
        ├── Step 7 ── WatermarkManager.advance_watermark()
        │             UPDATE process_control SET
        │               last_successful_run = watermark_to
        │               status = SUCCESS
        │             AuditLogger.write_run_end(status=SUCCESS)
        │
        └── [FINALLY — always executes]
              If lock held + not SUCCESS:
                WatermarkManager.release_lock(status=FAILED|DQ_BLOCKED)
                AuditLogger.write_run_end(status=<final_status>)
              DeltaDetector.drop_staging_table()
```

---

## 9. Watermark & Concurrency Logic

### Watermark state transitions

| Run Outcome | `last_successful_run` | `status` | Next run behaviour |
|---|---|---|---|
| `SUCCESS` | Advances to `watermark_to` | `SUCCESS` | Picks up from `watermark_to` |
| `NO_DELTA` | Advances to `watermark_to` | `SUCCESS` | Picks up from `watermark_to` (no gap) |
| `FAILED` | **Unchanged** | `FAILED` | Re-scans the same window |
| `DQ_BLOCKED` | **Unchanged** | `DQ_BLOCKED` | Re-scans the same window |
| `LOCK_CONFLICT` | **Unchanged** | *(held by other run)* | Retries after lock expiry |

> **Key invariant:** `last_successful_run` **only moves forward, and only on SUCCESS**. This guarantees that no data window is ever skipped — even across multiple consecutive failures.

### Lock conflict resolution

Locks auto-expire after `lock_expiry_hours` (default: 4 hours). The `acquire_lock` MERGE condition is:

```sql
WHEN MATCHED AND (
    status NOT IN ('RUNNING')
    OR TIMESTAMP_DIFF(NOW, locked_at, HOUR) >= lock_expiry_hours
) THEN UPDATE ...
```

This means a stale `RUNNING` lock older than 4 hours is automatically overwritten by the next run.

---

## 10. Data Quality Rules

### Rule types reference

**NOT_NULL** — fails if any row has `NULL` in `target_column`

```sql
INSERT INTO dq_rules VALUES (
  'dq_so_001', 'sales_orders_v1', 'Order ID must not be null',
  'NOT_NULL', 'order_id', '{}', 'REJECT', 10, TRUE, NULL, ...
);
```

**RANGE** — fails if values fall outside `[min, max]`

```sql
INSERT INTO dq_rules VALUES (
  'dq_so_002', 'sales_orders_v1', 'Amount must be non-negative',
  'RANGE', 'amount', '{"min": 0, "max": 10000000}', 'QUARANTINE', 20, TRUE, NULL, ...
);
```

**REGEX** — fails if value does not match the pattern

```sql
INSERT INTO dq_rules VALUES (
  'dq_so_003', 'sales_orders_v1', 'Country code must be 2 uppercase letters',
  'REGEX', 'country_code', '{"pattern": "^[A-Z]{2}$"}', 'WARN', 30, TRUE, NULL, ...
);
```

**UNIQUENESS** — fails if `target_column` has duplicate values in the staging batch

```sql
INSERT INTO dq_rules VALUES (
  'dq_so_004', 'sales_orders_v1', 'Order ID must be unique in batch',
  'UNIQUENESS', 'order_id', '{}', 'REJECT', 5, TRUE, NULL, ...
);
```

**REFERENTIAL** — fails if values don't exist in a reference table

```sql
INSERT INTO dq_rules VALUES (
  'dq_so_005', 'sales_orders_v1', 'Customer must exist',
  'REFERENTIAL', 'customer_id',
  '{"ref_table": "src-project.raw.customers", "ref_col": "customer_id"}',
  'QUARANTINE', 40, TRUE, NULL, ...
);
```

**CUSTOM_SQL** — arbitrary SQL; rows returned are failures. Use `{{staging}}` as placeholder for the staging table reference.

```sql
INSERT INTO dq_rules VALUES (
  'dq_so_006', 'sales_orders_v1', 'Shipped orders must have a ship date',
  'CUSTOM_SQL', NULL,
  '{"sql": "SELECT * FROM {{staging}} WHERE status = ''SHIPPED'' AND ship_date IS NULL"}',
  'WARN', 50, TRUE, NULL, ...
);
```

### Action on failure

| Action | Behaviour |
|---|---|
| `REJECT` | Pipeline **halts immediately**. Watermark unchanged. Source data must be fixed. |
| `QUARANTINE` | Failing rows written to `quarantine_records` and **removed from staging**. Clean rows proceed to MERGE. |
| `WARN` | Failure logged to `dq_error_log`. Pipeline **continues**. |

---

## 11. Cloud Composer DAG

**File:** `dags/bq_delta_ingestion_dag.py`

### Configuration via Airflow Variables

| Variable | Default | Description |
|---|---|---|
| `bq_delta_project` | env `GCP_PROJECT` | GCP project ID |
| `bq_delta_dataset` | `framework_config` | Framework BQ dataset |
| `bq_delta_location` | `US` | BQ dataset location |
| `bq_delta_cron` | `0 2 * * *` | DAG schedule (daily at 02:00 UTC) |
| `bq_delta_max_par` | `8` | Max parallel table tasks |

### Set Airflow variables

```bash
gcloud composer environments run $COMPOSER_ENV \
  --location $COMPOSER_REGION \
  variables set -- \
    bq_delta_project my-project-prod \
    bq_delta_dataset framework_config \
    bq_delta_max_par 10
```

### Upload DAG to Composer

```bash
# Get the DAGs bucket
DAGS_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
  --location $COMPOSER_REGION \
  --format "value(config.dagGcsPrefix)")

gsutil cp dags/bq_delta_ingestion_dag.py $DAGS_BUCKET/
```

### Upload framework package to Composer

```bash
# Upload as a Python package to Composer's plugins folder
gsutil -m cp -r framework/ $DAGS_BUCKET/../plugins/
```

### DAG structure

```
discover_tables
      │
      ├── run_pipeline[sales_orders_v1]
      ├── run_pipeline[customers_v2]
      ├── run_pipeline[products_v1]
      │        ...
      └── run_pipeline[N]
                │
                └── dag_summary
```

- `discover_tables` — queries `table_registry` ordered by priority, returns `registry_ids`
- `run_pipeline` — dynamically mapped via `expand(registry_id=table_ids)` — one Airflow task per table
- `dag_summary` — aggregates all results, logs counts by status

### Trigger manually

```bash
gcloud composer environments run $COMPOSER_ENV \
  --location $COMPOSER_REGION \
  dags trigger -- bq_delta_ingestion
```

---

## 12. Dashboard — Cloud Run

**File:** `dashboard/app.py`

FastAPI application serving a real-time observability dashboard.

### API endpoints

| Method | Endpoint | Description | Cache TTL |
|---|---|---|---|
| `GET` | `/` | Embedded SPA dashboard HTML | — |
| `GET` | `/health` | Liveness probe (no BQ call) | — |
| `GET` | `/api/summary` | Today's KPI aggregates | 60s |
| `GET` | `/api/runs` | Recent runs (filterable) | 60s |
| `GET` | `/api/runs/{run_id}` | Single run full detail | 300s |
| `GET` | `/api/dq/{run_id}` | DQ failures for a run | 120s |
| `GET` | `/api/quarantine` | Quarantine records | 30s |
| `GET` | `/api/quarantine/summary` | PENDING counts by table | 60s |
| `GET` | `/api/tables` | All tables + watermark + lock status | 120s |
| `GET` | `/api/trend` | Daily run trend (N days) | 300s |

### Run dashboard locally

```bash
export GCP_PROJECT=my-project-dev
export FRAMEWORK_DATASET=framework_config
export LOG_FORMAT=text
export CACHE_TTL_SECONDS=10   # faster refresh for dev

uv run uvicorn dashboard.app:app --reload --port 8080
```

### Deploy to Cloud Run (one-time setup)

```bash
PROJECT=my-project-dev
REGION=us-central1
REPO=bq-delta-images

# Create service account
gcloud iam service-accounts create dashboard-sa \
  --display-name "BQ Delta Dashboard SA"

# Grant BigQuery read permissions
gcloud projects add-iam-policy-binding $PROJECT \
  --member "serviceAccount:dashboard-sa@$PROJECT.iam.gserviceaccount.com" \
  --role "roles/bigquery.jobUser"

bq update --source_dataset_access_list \
  "{\"entries\": [{\"role\": \"roles/bigquery.dataViewer\", \"userByEmail\": \"dashboard-sa@$PROJECT.iam.gserviceaccount.com\"}]}" \
  $PROJECT:framework_config

# Create Artifact Registry repository
gcloud artifacts repositories create $REPO \
  --repository-format docker \
  --location $REGION
```

### Build and deploy manually

```bash
IMAGE=$REGION-docker.pkg.dev/$PROJECT/$REPO/bq-delta-dashboard
SHA=$(git rev-parse --short HEAD)

# Authenticate Docker
gcloud auth configure-docker $REGION-docker.pkg.dev

# Build and push
docker build -t $IMAGE:$SHA -t $IMAGE:latest .
docker push $IMAGE:$SHA
docker push $IMAGE:latest

# Deploy to Cloud Run
gcloud run deploy bq-delta-dashboard \
  --image=$IMAGE:$SHA \
  --platform=managed \
  --region=$REGION \
  --service-account=dashboard-sa@$PROJECT.iam.gserviceaccount.com \
  --min-instances=1 \
  --max-instances=3 \
  --memory=512Mi \
  --cpu=1 \
  --port=8080 \
  --no-allow-unauthenticated \
  --set-env-vars="GCP_PROJECT=$PROJECT,FRAMEWORK_DATASET=framework_config,BQ_LOCATION=US,LOG_FORMAT=json,CACHE_TTL_SECONDS=60"
```

### Verify deployment

```bash
gcloud run services describe bq-delta-dashboard --region $REGION
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
     $(gcloud run services describe bq-delta-dashboard --region $REGION --format "value(status.url)")/health
```

---

## 13. Docker Image

**File:** `Dockerfile`

Multi-stage build — final image is ~178 MB with no build tools.

### Stage 1 — Builder

- Base: `python:3.11-slim`
- Installs `uv`, copies `pyproject.toml` + `uv.lock`
- Runs `uv sync --frozen --no-group dev` — installs framework + dashboard groups only
- Virtual environment at `/app/.venv`

### Stage 2 — Runtime

- Base: `python:3.11-slim` (fresh — no build tools)
- Copies `/app/.venv` from builder
- Copies `framework/` and `dashboard/` source
- Creates non-root user `appuser`
- Exposes port 8080, starts `uvicorn` with 2 workers

### Build locally

```bash
docker build -t bq-delta-dashboard:local .
docker run --rm -p 8080:8080 \
  -e GCP_PROJECT=my-project-dev \
  -e GOOGLE_APPLICATION_CREDENTIALS=/creds/key.json \
  -v ~/.config/gcloud:/creds \
  bq-delta-dashboard:local
```

---

## 14. CI/CD — Cloud Build

**File:** `infra/deploy/cloudbuild.yaml`

Triggered automatically on push to `main` branch.

### Pipeline steps

| Step | Tool | Action |
|---|---|---|
| 1 | `docker build` | Builds image, tags with `$SHORT_SHA` and `latest`, uses `--cache-from latest` for faster builds |
| 2 | `docker push` | Pushes both tags to Artifact Registry |
| 3 | `gcloud run deploy` | Deploys SHA-tagged image to Cloud Run |

### Set up Cloud Build trigger

```bash
gcloud builds triggers create github \
  --repo-owner=your-org \
  --repo-name=bq-delta-framework \
  --branch-pattern="^main$" \
  --build-config=infra/deploy/cloudbuild.yaml
```

### Grant Cloud Build permissions

```bash
CB_SA=$(gcloud projects describe $PROJECT --format "value(projectNumber)")@cloudbuild.gserviceaccount.com

gcloud projects add-iam-policy-binding $PROJECT \
  --member "serviceAccount:$CB_SA" --role "roles/run.admin"

gcloud projects add-iam-policy-binding $PROJECT \
  --member "serviceAccount:$CB_SA" --role "roles/artifactregistry.writer"

gcloud iam service-accounts add-iam-policy-binding \
  dashboard-sa@$PROJECT.iam.gserviceaccount.com \
  --member "serviceAccount:$CB_SA" \
  --role "roles/iam.serviceAccountUser"
```

---

## 15. IAM & Permissions

### Framework pipeline SA (for PipelineRunner / Airflow tasks)

| Role | Resource | Purpose |
|---|---|---|
| `roles/bigquery.dataEditor` | `framework_config` dataset | Read/write all 6 framework tables |
| `roles/bigquery.dataEditor` | Source dataset | Read source tables |
| `roles/bigquery.dataEditor` | Target dataset | MERGE into target tables |
| `roles/bigquery.jobUser` | Project | Execute BigQuery jobs |

### Dashboard SA (`dashboard-sa`)

| Role | Resource | Purpose |
|---|---|---|
| `roles/bigquery.dataViewer` | `framework_config` dataset | Read-only SELECT on all framework tables |
| `roles/bigquery.jobUser` | Project | Run SELECT queries |

### Cloud Build SA

| Role | Resource | Purpose |
|---|---|---|
| `roles/run.admin` | Project | Deploy Cloud Run services |
| `roles/artifactregistry.writer` | Project | Push Docker images |
| `roles/iam.serviceAccountUser` | `dashboard-sa` | Act as service account during deployment |

### Team members (dashboard access via IAP)

```bash
gcloud projects add-iam-policy-binding $PROJECT \
  --member "user:engineer@company.com" \
  --role "roles/iap.httpsResourceAccessor"
```

---

## 16. Configuration Reference

### Environment variables (runtime)

| Variable | Required | Default | Description |
|---|---|---|---|
| `GCP_PROJECT` | **Yes** | — | GCP project ID |
| `FRAMEWORK_DATASET` | No | `framework_config` | BigQuery dataset name |
| `BQ_LOCATION` | No | `US` | BigQuery dataset location |
| `LOCK_EXPIRY_HOURS` | No | `4` | Hours before stale lock auto-expires |
| `MAX_PARALLEL_TABLES` | No | `8` | Max concurrent Airflow table tasks |
| `LOG_FORMAT` | No | `json` | `json` (Cloud Run) or `text` (local dev) |
| `LOG_LEVEL` | No | `INFO` | `DEBUG` / `INFO` / `WARNING` |
| `CACHE_TTL_SECONDS` | No | `60` | Dashboard API in-process cache TTL |
| `PORT` | No | `8080` | HTTP port (injected by Cloud Run) |

### PipelineRunner constructor parameters

| Parameter | Default | Description |
|---|---|---|
| `project` | `$GCP_PROJECT` | GCP project ID |
| `dataset` | `framework_config` | Framework BQ dataset |
| `location` | `US` | BQ dataset location |
| `lock_expiry_hours` | `4` | Stale lock expiry threshold |

---

## 17. Onboarding a New Table

To add a new table to the framework, no code changes are needed — insert rows into BigQuery.

### Step 1 — Register the table

```sql
INSERT INTO `my-project.framework_config.table_registry`
VALUES (
  'inventory_items_v1',                      -- registry_id
  'src-project', 'raw', 'inventory_items',   -- source
  'tgt-project', 'curated', 'inventory',     -- target
  ['item_id', 'warehouse_id'],               -- primary_key_cols
  ['item_id', 'warehouse_id', 'quantity', 'status', 'updated_at'],  -- delta_hash_cols
  'updated_at',                              -- watermark_col
  NULL,                                      -- batch_size (no limit)
  TRUE,                                      -- is_active
  50,                                        -- priority (runs before priority=100)
  'supply-chain-team',                       -- owner_team
  ['supply-chain', 'inventory'],             -- tags
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);
```

### Step 2 — Add DQ rules (optional)

```sql
-- Rule 1: item_id must not be null (REJECT)
INSERT INTO `my-project.framework_config.dq_rules`
VALUES (
  'dq_inv_001', 'inventory_items_v1', 'Item ID must not be null',
  'NOT_NULL', 'item_id', '{}', 'REJECT', 10, TRUE, 'Hard requirement',
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Rule 2: quantity must be non-negative (QUARANTINE)
INSERT INTO `my-project.framework_config.dq_rules`
VALUES (
  'dq_inv_002', 'inventory_items_v1', 'Quantity must be >= 0',
  'RANGE', 'quantity', '{"min": 0}', 'QUARANTINE', 20, TRUE, 'Negative stock is invalid',
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
```

### Step 3 — Test locally

```python
from framework import PipelineRunner

runner = PipelineRunner(project="my-project-dev")
result = runner.run("inventory_items_v1")
print(result.status, result.rows_affected)
```

### Step 4 — Activate

The DAG's `discover_tables` task will automatically pick up the new table on the next scheduled run (or trigger manually). No DAG code changes needed.

---

## 18. Testing

### Run all unit tests

```bash
uv run pytest tests/ -v
```

### Run with coverage

```bash
uv run pytest tests/ -v --cov=framework --cov-report=term-missing
```

### Lint and format

```bash
uv run ruff check framework/ dashboard/ dags/ tests/
uv run ruff format framework/ dashboard/ dags/ tests/
```

### Test a specific component

```bash
uv run pytest tests/test_pipeline.py::TestDQValidator -v
uv run pytest tests/test_pipeline.py::TestDeltaDetector::test_watermark_filter_full_load -v
```

### Test the dashboard API locally

```bash
# Start dashboard
uv run uvicorn dashboard.app:app --reload --port 8080 &

# Test endpoints
curl http://localhost:8080/health
curl http://localhost:8080/api/summary
curl http://localhost:8080/api/tables
curl http://localhost:8080/api/runs?limit=10&hours=24
```

---

## 19. Troubleshooting

### Lock stuck in RUNNING state

```sql
-- Check who holds the lock and when it was acquired
SELECT registry_id, status, locked_by, locked_at,
       TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), locked_at, HOUR) AS hours_held
FROM `my-project.framework_config.process_control`
WHERE status = 'RUNNING'
ORDER BY locked_at;

-- Manually release a stale lock (use only when the run is confirmed dead)
UPDATE `my-project.framework_config.process_control`
SET status = 'FAILED', locked_at = NULL, locked_by = NULL, updated_at = CURRENT_TIMESTAMP()
WHERE registry_id = 'sales_orders_v1'
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), locked_at, HOUR) > 4;
```

### Staging table left orphaned

```sql
-- List all staging tables
SELECT table_name
FROM `my-project.framework_config.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE '_delta_staging_%';

-- Drop a specific orphaned staging table
DROP TABLE IF EXISTS `my-project.framework_config._delta_staging_sales_orders_v1`;
```

### DQ_BLOCKED — identify which rows are failing

```sql
-- Find the failing rule and row count
SELECT rule_id, rule_name, action_on_fail, failed_row_count, sample_failing_sql
FROM `my-project.framework_config.dq_error_log`
WHERE run_id = '<your_run_id>'
ORDER BY logged_at;
```

### Dashboard returns stale data

The dashboard uses an in-process TTL cache. Force a cache miss by restarting the Cloud Run instance:

```bash
gcloud run services update bq-delta-dashboard \
  --region us-central1 \
  --update-env-vars CACHE_TTL_SECONDS=10
```

Or reduce `CACHE_TTL_SECONDS` in `service.yaml` and redeploy.

### Watermark not advancing after successful run

```sql
-- Check process_control for the table
SELECT registry_id, status, last_successful_run, last_run_id, updated_at
FROM `my-project.framework_config.process_control`
WHERE registry_id = 'sales_orders_v1';

-- Check run_audit for the run
SELECT run_id, status, watermark_from, watermark_to, rows_affected, error_message
FROM `my-project.framework_config.run_audit`
WHERE registry_id = 'sales_orders_v1'
ORDER BY started_at DESC
LIMIT 5;
```

---

## 20. Glossary

| Term | Definition |
|---|---|
| **registry_id** | Unique string key identifying a source/target table pair, e.g. `sales_orders_v1` |
| **watermark** | The `last_successful_run` timestamp stored in `process_control` — the lower bound for the next incremental extraction |
| **watermark_from** | Start of the current extraction window (= `last_successful_run`, or NULL on first run) |
| **watermark_to** | End of the current extraction window (= `NOW()` at config load time) |
| **delta** | The set of rows that have changed (inserted or updated) between `watermark_from` and `watermark_to` |
| **row hash** | SHA256 hex digest of selected column values for a row, used to detect field-level changes |
| **staging table** | Temporary BigQuery table (`_delta_staging_*`) holding the current run's delta rows before MERGE |
| **FULL LOAD** | When `watermark_from` is NULL (first ever run) — extracts entire source table |
| **INCREMENTAL** | All subsequent runs — extracts only rows changed since last successful run |
| **REJECT** | DQ action that halts the pipeline entirely — watermark does not advance |
| **QUARANTINE** | DQ action that isolates failing rows from the run — clean rows proceed to MERGE |
| **WARN** | DQ action that logs the failure but allows the pipeline to continue |
| **lock expiry** | Automatic mechanism to release stale locks — configurable via `lock_expiry_hours` (default: 4h) |
| **NO_DELTA** | Status when delta extraction returns 0 rows — watermark still advances |

---

*BQ Delta Ingestion Framework · v1.0.0 · Cloud Composer 2 + BigQuery + Python 3.11 + Cloud Run*
