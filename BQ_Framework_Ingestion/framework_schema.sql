-- ============================================================
-- BQ DELTA INGESTION FRAMEWORK — Schema DDL
-- Dataset: <project>.framework_config
-- ============================================================

-- 1. TABLE REGISTRY
--    One row per source→target table pair registered in the framework
CREATE TABLE IF NOT EXISTS `{project}.framework_config.table_registry`
(
  registry_id         STRING NOT NULL,          -- UUID, e.g. "sales_orders_v1"
  source_project      STRING NOT NULL,
  source_dataset      STRING NOT NULL,
  source_table        STRING NOT NULL,
  target_project      STRING NOT NULL,
  target_dataset      STRING NOT NULL,
  target_table        STRING NOT NULL,
  primary_key_cols    ARRAY<STRING> NOT NULL,   -- columns for MERGE key
  delta_hash_cols     ARRAY<STRING>,            -- empty/null = use all columns ('*')
  watermark_col       STRING NOT NULL,          -- e.g. "updated_at", "created_at"
  is_active           BOOL NOT NULL DEFAULT TRUE,
  batch_size          INT64,                    -- optional row limit per run
  created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (description = 'Registry of all tables managed by the delta ingestion framework');


-- 2. DQ RULES CONFIG
--    Flexible, config-driven DQ rules per table
CREATE TABLE IF NOT EXISTS `{project}.framework_config.dq_rules`
(
  rule_id             STRING NOT NULL,
  registry_id         STRING NOT NULL,          -- FK to table_registry
  rule_name           STRING NOT NULL,          -- human-readable
  rule_type           STRING NOT NULL,          -- NOT_NULL | RANGE | REGEX | UNIQUENESS | REFERENTIAL | CUSTOM_SQL
  target_column       STRING,                   -- null for CUSTOM_SQL or UNIQUENESS multi-col
  rule_params         JSON,                     -- flexible params per rule type (see examples below)
  action_on_fail      STRING NOT NULL,          -- REJECT | WARN | QUARANTINE
  is_active           BOOL NOT NULL DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (description = 'DQ rule definitions per registered table');

/*
  rule_params examples by rule_type:

  NOT_NULL:       {}   -- no params needed
  RANGE:          {"min": 0, "max": 1000000, "inclusive": true}
  REGEX:          {"pattern": "^[A-Z]{2}[0-9]{6}$"}
  UNIQUENESS:     {"scope_cols": ["col_a", "col_b"]}   -- multi-col unique check
  REFERENTIAL:    {"ref_project": "p", "ref_dataset": "d", "ref_table": "t", "ref_column": "id"}
  CUSTOM_SQL:     {"sql": "SELECT COUNT(*) FROM {table} WHERE amount < 0 AND status = 'APPROVED'",
                   "expected_count": 0, "operator": "eq"}
*/


-- 3. PROCESS CONTROL TABLE
--    Watermark tracking — one row per registry_id
CREATE TABLE IF NOT EXISTS `{project}.framework_config.process_control`
(
  registry_id           STRING NOT NULL,
  last_successful_run   TIMESTAMP,              -- watermark: last completed extraction end time
  last_run_id           STRING,                 -- run UUID of last successful execution
  last_row_count        INT64,                  -- rows ingested in last run
  status                STRING,                 -- RUNNING | SUCCESS | FAILED | PARTIAL
  locked_at             TIMESTAMP,              -- concurrency lock timestamp
  locked_by             STRING                  -- DAG run ID holding the lock
)
OPTIONS (description = 'Watermark and process control per registered table');


-- 4. RUN AUDIT / REPORT TABLE
--    Full detail for every run — one row per table per DAG run
CREATE TABLE IF NOT EXISTS `{project}.framework_config.run_audit`
(
  run_id                STRING NOT NULL,        -- UUID generated per DAG run
  dag_run_id            STRING,                 -- Composer DAG run ID
  registry_id           STRING NOT NULL,
  source_table_fqn      STRING,                 -- fully qualified source table
  target_table_fqn      STRING,                 -- fully qualified target table
  watermark_from        TIMESTAMP,              -- extraction window start
  watermark_to          TIMESTAMP,              -- extraction window end
  source_row_count      INT64,                  -- rows in snapshot window
  delta_row_count       INT64,                  -- rows detected as delta
  dq_passed_count       INT64,
  dq_failed_count       INT64,
  ingested_row_count    INT64,                  -- rows written to target
  status                STRING,                 -- SUCCESS | FAILED | PARTIAL | DQ_BLOCKED
  error_message         STRING,
  started_at            TIMESTAMP,
  completed_at          TIMESTAMP,
  duration_seconds      FLOAT64,
  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS (description = 'Full audit log for every pipeline run');


-- 5. DQ ERROR LOG
--    Stores counts + sample failed records per rule per run
CREATE TABLE IF NOT EXISTS `{project}.framework_config.dq_error_log`
(
  error_id              STRING NOT NULL,
  run_id                STRING NOT NULL,
  registry_id           STRING NOT NULL,
  rule_id               STRING NOT NULL,
  rule_name             STRING,
  rule_type             STRING,
  target_column         STRING,
  action_on_fail        STRING,
  failed_row_count      INT64,
  sample_failed_rows    JSON,                   -- up to 10 sample rows as JSON
  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS (description = 'DQ failure details per rule per run');


-- 6. QUARANTINE TABLE (generic — one per framework, stores failed rows as JSON)
CREATE TABLE IF NOT EXISTS `{project}.framework_config.quarantine_records`
(
  quarantine_id         STRING NOT NULL,
  run_id                STRING NOT NULL,
  registry_id           STRING NOT NULL,
  rule_id               STRING,
  raw_record            JSON NOT NULL,           -- original row serialized as JSON
  failure_reason        STRING,
  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS (description = 'Quarantined records that failed DQ checks');
