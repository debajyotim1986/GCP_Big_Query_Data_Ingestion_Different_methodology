"""
dags/delta_ingestion_dag.py

Cloud Composer (Airflow 2.x) DAG for the BQ Delta Ingestion Framework.

Features:
  - Dynamically discovers all active tables from table_registry at parse time
  - Generates one task per table (parallelism controlled by max_active_tasks)
  - Sends failure alerts via Cloud Monitoring / email
  - Pushes run summaries to XCom for downstream monitoring tasks
  - TaskGroup per table for clean UI grouping
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT", "your-gcp-project")
FRAMEWORK_DATASET = "framework_config"
MAX_PARALLEL_TABLES = 5      # Tune based on BQ slot availability

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@yourcompany.com"],
}


# ── Task functions ─────────────────────────────────────────────

def discover_active_tables(**context) -> list[str]:
    """
    Fetch all active registry_ids from BQ at DAG runtime.
    Pushed to XCom for downstream tasks.
    """
    from framework.config.config_loader import ConfigLoader
    from framework.utils.bq_client import BQClient

    bq = BQClient.get_instance(GCP_PROJECT)
    configs = ConfigLoader(bq).load_all_active_configs()
    registry_ids = [c.registry_id for c in configs]

    logger.info("Discovered %d active tables: %s", len(registry_ids), registry_ids)
    context["ti"].xcom_push(key="registry_ids", value=registry_ids)
    return registry_ids


def run_pipeline_for_table(registry_id: str, **context) -> dict:
    """
    Entry point for each per-table PythonOperator task.
    Runs the full pipeline and pushes summary to XCom.
    """
    from framework.pipeline_runner import PipelineRunner

    dag_run_id = context["run_id"]

    logger.info("Starting pipeline task: registry_id=%s dag_run_id=%s", registry_id, dag_run_id)

    runner = PipelineRunner(gcp_project=GCP_PROJECT)
    summary = runner.run(registry_id=registry_id, dag_run_id=dag_run_id)

    context["ti"].xcom_push(key=f"summary_{registry_id}", value=summary)
    return summary


def write_dag_level_report(**context) -> None:
    """
    Collects per-table summaries and writes a consolidated DAG-level report.
    Runs at end of DAG regardless of individual task success/failure.
    """
    from framework.utils.bq_client import BQClient

    bq = BQClient.get_instance(GCP_PROJECT)
    ti = context["ti"]
    dag_run_id = context["run_id"]

    # Collect XCom summaries from all table tasks
    registry_ids = ti.xcom_pull(task_ids="discover_tables", key="registry_ids") or []
    summaries = []

    for rid in registry_ids:
        try:
            summary = ti.xcom_pull(
                task_ids=f"table_{rid}.run_pipeline_{rid}",
                key=f"summary_{rid}"
            )
            if summary:
                summaries.append(summary)
        except Exception as e:
            logger.warning("Could not pull XCom for %s: %s", rid, e)

    success = sum(1 for s in summaries if s.get("status") == "SUCCESS")
    failed = len(summaries) - success
    total_ingested = sum(s.get("ingested_row_count", 0) for s in summaries)

    logger.info(
        "DAG Run Report | dag_run_id=%s | tables=%d | success=%d | failed=%d | total_ingested=%d",
        dag_run_id, len(summaries), success, failed, total_ingested
    )

    # Optionally write consolidated report to BQ
    if summaries:
        try:
            from google.cloud import bigquery
            report_fqn = f"{GCP_PROJECT}.{FRAMEWORK_DATASET}.dag_run_report"
            report_row = {
                "dag_run_id": dag_run_id,
                "total_tables": len(summaries),
                "success_count": success,
                "failed_count": failed,
                "total_ingested_rows": total_ingested,
                "created_at": datetime.utcnow().isoformat()
            }
            bq.insert_rows_json(report_fqn, [report_row])
        except Exception as e:
            logger.warning("Could not write DAG report row: %s", e)


# ── Dynamic table discovery at parse time ──────────────────────

def _get_active_registry_ids() -> list[str]:
    """
    Called at DAG parse time to generate tasks dynamically.
    Uses a lightweight BQ call to get active registry IDs.
    Falls back to empty list if BQ unavailable (e.g. during CI).
    """
    try:
        from framework.config.config_loader import ConfigLoader
        from framework.utils.bq_client import BQClient

        bq = BQClient.get_instance(GCP_PROJECT)
        configs = ConfigLoader(bq).load_all_active_configs()
        return [c.registry_id for c in configs]
    except Exception as e:
        logger.warning("Could not load registry IDs at parse time: %s", e)
        return []


# ── DAG definition ─────────────────────────────────────────────

with DAG(
    dag_id="bq_delta_ingestion_framework",
    description="Generic BQ delta ingestion — snapshot, DQ, MERGE",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",     # Daily 2am UTC — adjust per requirement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=MAX_PARALLEL_TABLES,
    max_active_runs=1,                 # Prevent concurrent DAG runs
    tags=["data-engineering", "delta-ingestion", "bigquery"],
    doc_md="""
## BQ Delta Ingestion Framework

Generic, config-driven pipeline that:
1. Scans source tables in extraction window (watermark-based)
2. Detects delta records via MD5 hash (specific cols or full row)
3. Runs configurable DQ rules (REJECT / WARN / QUARANTINE)
4. MERGEs clean delta records into target tables
5. Updates process control (watermark advance)
6. Writes full audit record

**Config location:** `framework_config.table_registry` and `framework_config.dq_rules`
""",
) as dag:

    # ── Start marker
    start = EmptyOperator(task_id="start")

    # ── Discover active tables at runtime
    discover_task = PythonOperator(
        task_id="discover_tables",
        python_callable=discover_active_tables,
    )

    # ── Dynamically generate one TaskGroup per registered table
    active_ids = _get_active_registry_ids()
    pipeline_tasks = []

    for registry_id in active_ids:
        with TaskGroup(group_id=f"table_{registry_id}") as tg:
            run_task = PythonOperator(
                task_id=f"run_pipeline_{registry_id}",
                python_callable=run_pipeline_for_table,
                op_kwargs={"registry_id": registry_id},
                execution_timeout=timedelta(hours=2),
                retries=1,
                retry_delay=timedelta(minutes=10),
                doc_md=f"Delta ingestion pipeline for table: `{registry_id}`"
            )
        pipeline_tasks.append(tg)

    # ── Final report — runs even if some table tasks fail
    report_task = PythonOperator(
        task_id="write_dag_report",
        python_callable=write_dag_level_report,
        trigger_rule=TriggerRule.ALL_DONE,  # Runs regardless of upstream failures
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # ── Dependencies
    start >> discover_task >> pipeline_tasks >> report_task >> end
