# BQ Delta Ingestion Framework

Generic, config-driven batch ingestion framework for BigQuery.
Runs on Cloud Composer (Airflow 2.x) — zero code changes when onboarding new tables.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Cloud Composer (Airflow)                         │
│                                                                         │
│  DAG: bq_delta_ingestion_framework                                      │
│                                                                         │
│  [start] → [discover_tables] → [table_A] ──┐                           │
│                                 [table_B] ──┤→ [write_dag_report] →[end]│
│                                 [table_C] ──┘                           │
└─────────────────────────────────────────────────────────────────────────┘
              │                     │
              ▼                     ▼
   ┌──────────────────┐   ┌──────────────────────────────────────────────┐
   │ framework_config │   │         PipelineRunner (per table)           │
   │ BQ Dataset       │   │                                              │
   │                  │   │  1. Load config from table_registry          │
   │ table_registry   │──▶│  2. Acquire process control lock             │
   │ dq_rules         │   │  3. Get watermark from process_control       │
   │ process_control  │   │  4. DeltaDetector → staging table            │
   │ run_audit        │   │  5. DQValidator → rules per config           │
   │ dq_error_log     │   │     ├── REJECT  → block pipeline             │
   │ quarantine_rec.  │   │     ├── QUARANTINE → remove rows             │
   └──────────────────┘   │     └── WARN    → log, continue             │
                          │  6. IngestionEngine → MERGE to target        │
                          │  7. Advance watermark                        │
                          │  8. Write audit record                       │
                          │  9. Cleanup staging table                    │
                          └──────────────────────────────────────────────┘
```

---

## Project Structure

```
bq_delta_framework/
├── framework/
│   ├── config/
│   │   ├── config_loader.py        # Reads table_registry + dq_rules from BQ
│   │   └── process_control.py      # Watermark + concurrency lock management
│   ├── delta/
│   │   └── delta_detector.py       # Dynamic hash-based delta detection
│   ├── dq/
│   │   └── dq_validator.py         # DQ rule execution (5 rule types)
│   ├── ingestion/
│   │   └── ingestion_engine.py     # Dynamic MERGE to target table
│   ├── audit/
│   │   └── audit_writer.py         # run_audit table writer
│   ├── utils/
│   │   └── bq_client.py            # BQ client wrapper + schema introspection
│   └── pipeline_runner.py          # Main orchestrator per table
├── dags/
│   └── delta_ingestion_dag.py      # Composer DAG — dynamic task generation
├── sql/
│   ├── framework_schema.sql        # DDL for all framework tables
│   └── templates/                  # SQL templates (for reference)
├── config_samples/
│   └── register_table_sample.sql   # How to register a new table
└── tests/
    └── ...
```

---

## Framework Tables (BQ Dataset: `framework_config`)

| Table | Purpose |
|---|---|
| `table_registry` | One row per source→target table pair |
| `dq_rules` | DQ rules per table (any number, any type) |
| `process_control` | Watermark + concurrency lock per table |
| `run_audit` | Full run history (partitioned by date) |
| `dq_error_log` | Failed rule details + sample rows |
| `quarantine_records` | Rows removed due to QUARANTINE DQ rules |

---

## Supported DQ Rule Types

| Rule Type | Config | Action Options |
|---|---|---|
| `NOT_NULL` | No params | REJECT / WARN / QUARANTINE |
| `RANGE` | min, max, inclusive | REJECT / WARN / QUARANTINE |
| `REGEX` | pattern | REJECT / WARN / QUARANTINE |
| `UNIQUENESS` | scope_cols list | REJECT / WARN / QUARANTINE |
| `REFERENTIAL` | ref table + column | REJECT / WARN / QUARANTINE |
| `CUSTOM_SQL` | arbitrary SQL | REJECT / WARN / QUARANTINE |

---

## Onboarding a New Table (Zero Code Changes)

```sql
-- 1. Register the table
INSERT INTO `framework_config.table_registry` VALUES (
  'my_new_table_v1',       -- registry_id (unique name)
  'source-project', 'raw', 'my_table',    -- source
  'target-project', 'curated', 'my_table', -- target
  ['id'],                  -- primary key column(s)
  [],                      -- hash cols (empty = all columns)
  'updated_at',            -- watermark column
  TRUE, NULL,
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- 2. Add DQ rules (as many as needed)
INSERT INTO `framework_config.dq_rules` VALUES
  ('rule_001', 'my_new_table_v1', 'id_not_null', 'NOT_NULL', 'id', NULL, 'REJECT', TRUE, CURRENT_TIMESTAMP());

-- 3. Run the DAG — it discovers and processes the new table automatically
```

---

## Delta Hash Mode

| Config | Behavior |
|---|---|
| `delta_hash_cols = ['col1', 'col2', 'col3']` | Hash only specified columns |
| `delta_hash_cols = []` (empty) | Hash entire row via `TO_JSON_STRING` |

---

## Process Control — Watermark Logic

```
Run 1 (first run):   watermark_from = NULL    → full table load
Run 2:               watermark_from = Run1.completed_at
Run 3:               watermark_from = Run2.completed_at
...
```

If a run **fails**, the watermark is **NOT advanced** — next run re-processes the same window.

---

## DQ Action Matrix

| Action | Effect on Pipeline | Effect on Staging | Watermark |
|---|---|---|---|
| `REJECT` | **BLOCKED** — entire run fails | No change | NOT advanced |
| `QUARANTINE` | Continues | Failed rows **removed** | Advanced |
| `WARN` | Continues | No change | Advanced |
