"""
dags/bq_delta_ingestion_dag.py
================================
Cloud Composer 2 / Apache Airflow 2.8 DAG for the BQ Delta Ingestion Framework.

DAG overview
------------
1. ``discover_tables``  — Query table_registry for all active tables ordered
                           by priority.  Returns a list of registry_ids.

2. Per-table TaskGroup  — One parallel group per registry_id containing:
   a. ``run_pipeline``  — Calls PipelineRunner.run(); XCom-pushes PipelineResult.
   b. ``log_summary``   — Logs final status, raises on FAILED / DQ_BLOCKED.

3. ``dag_summary``      — Aggregates all results and logs a run report.

Scheduling
----------
Default: daily at 02:00 UTC.  Override via Airflow Variable ``bq_delta_cron``.

Concurrency
-----------
The framework's WatermarkManager handles table-level locking within BigQuery,
so Airflow-level table concurrency can be set to max_active_tis_per_dag.
Multiple tables run in parallel (controlled by ``max_active_tasks``).

Configuration
-------------
Airflow Variables:
  bq_delta_project  — GCP project ID (falls back to GCP_PROJECT env var)
  bq_delta_dataset  — Framework BQ dataset (default: framework_config)
  bq_delta_location — BQ location (default: US)
  bq_delta_cron     — Cron schedule (default: 0 2 * * *)
  bq_delta_max_par  — Max parallel table tasks (default: 8)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# ── DAG-level defaults ─────────────────────────────────────────────────────────

def _var(name: str, default: str = "") -> str:
    """Safely read an Airflow Variable, falling back to env or default."""
    try:
        return Variable.get(name, default_var=default)
    except Exception:
        return os.environ.get(name.upper(), default)


GCP_PROJECT = _var("bq_delta_project") or os.environ.get("GCP_PROJECT", "")
BQ_DATASET  = _var("bq_delta_dataset", "framework_config")
BQ_LOCATION = _var("bq_delta_location", "US")
CRON        = _var("bq_delta_cron", "0 2 * * *")
MAX_PAR     = int(_var("bq_delta_max_par", "8"))


# ── DAG definition ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="bq_delta_ingestion",
    description="BQ Delta Ingestion Framework — daily delta load for all registered tables",
    schedule_interval=CRON,
    start_date=days_ago(1),
    catchup=False,
    max_active_tasks=MAX_PAR,
    max_active_runs=1,
    default_args=default_args,
    tags=["delta", "bigquery", "framework"],
    doc_md=__doc__,
)
def bq_delta_ingestion_dag():

    # ── Step 1: Discover active tables ────────────────────────────────────────

    @task(task_id="discover_tables")
    def discover_tables() -> list[str]:
        """
        Query table_registry for all active registry_ids ordered by priority.
        Returns a list used by dynamic task mapping downstream.
        """
        from google.cloud import bigquery as bq

        client = bq.Client(project=GCP_PROJECT, location=BQ_LOCATION)
        sql = f"""
        SELECT registry_id
        FROM `{GCP_PROJECT}.{BQ_DATASET}.table_registry`
        WHERE is_active = TRUE
        ORDER BY priority ASC, registry_id ASC
        """
        rows = list(client.query(sql).result())
        registry_ids = [r["registry_id"] for r in rows]

        if not registry_ids:
            logger.warning("No active tables found in table_registry")
        else:
            logger.info("Discovered %d active tables: %s", len(registry_ids), registry_ids)

        return registry_ids

    # ── Step 2: Per-table pipeline execution (dynamically mapped) ─────────────

    @task(task_id="run_pipeline")
    def run_pipeline(registry_id: str) -> dict:
        """
        Execute the full 7-step delta ingestion pipeline for one table.
        Returns a serialisable dict from PipelineResult.
        """
        context = get_current_context()
        dag_run_id = context["run_id"]

        from framework.pipeline_runner import PipelineRunner

        runner = PipelineRunner(
            project=GCP_PROJECT,
            dataset=BQ_DATASET,
            location=BQ_LOCATION,
        )

        result = runner.run(registry_id=registry_id, dag_run_id=dag_run_id)

        result_dict = {
            "registry_id": result.registry_id,
            "run_id": result.run_id,
            "status": result.status,
            "load_mode": result.load_mode,
            "rows_extracted": result.rows_extracted,
            "rows_affected": result.rows_affected,
            "rows_quarantined": result.rows_quarantined,
            "duration_secs": round(result.duration_secs, 2),
            "error_message": result.error_message,
        }

        logger.info(
            "Pipeline result: registry_id=%s status=%s rows=%d duration=%.1fs",
            registry_id, result.status, result.rows_affected, result.duration_secs,
        )

        # Raise on hard failures so Airflow marks task as failed and retries
        if result.status in ("FAILED",):
            raise RuntimeError(
                f"Pipeline FAILED for {registry_id}: {result.error_message}"
            )

        # DQ_BLOCKED is a data issue — fail the task but don't retry blindly
        if result.status == "DQ_BLOCKED":
            raise RuntimeError(
                f"DQ_BLOCKED for {registry_id}: {result.error_message}"
            )

        return result_dict

    # ── Step 3: DAG-level summary ─────────────────────────────────────────────

    @task(task_id="dag_summary")
    def dag_summary(results: list[dict]) -> None:
        """
        Log a structured summary of all table runs for this DAG execution.
        Can be extended to push metrics to Cloud Monitoring or Slack.
        """
        total = len(results)
        success = sum(1 for r in results if r["status"] in ("SUCCESS", "NO_DELTA"))
        failed = sum(1 for r in results if r["status"] == "FAILED")
        blocked = sum(1 for r in results if r["status"] == "DQ_BLOCKED")
        lock_conflict = sum(1 for r in results if r["status"] == "LOCK_CONFLICT")
        total_rows = sum(r.get("rows_affected", 0) for r in results)
        total_quarantined = sum(r.get("rows_quarantined", 0) for r in results)

        logger.info("=" * 60)
        logger.info("DAG SUMMARY")
        logger.info("  Total tables  : %d", total)
        logger.info("  SUCCESS       : %d", success)
        logger.info("  FAILED        : %d", failed)
        logger.info("  DQ_BLOCKED    : %d", blocked)
        logger.info("  LOCK_CONFLICT : %d", lock_conflict)
        logger.info("  Rows affected : %d", total_rows)
        logger.info("  Rows quarant. : %d", total_quarantined)
        logger.info("=" * 60)

        if failed > 0 or blocked > 0:
            failed_ids = [
                r["registry_id"] for r in results
                if r["status"] in ("FAILED", "DQ_BLOCKED")
            ]
            logger.error("FAILURES: %s", failed_ids)

    # ── Wire up the DAG ───────────────────────────────────────────────────────

    table_ids = discover_tables()

    pipeline_results = run_pipeline.expand(registry_id=table_ids)

    dag_summary(pipeline_results)


# Instantiate
bq_delta_ingestion_dag()
