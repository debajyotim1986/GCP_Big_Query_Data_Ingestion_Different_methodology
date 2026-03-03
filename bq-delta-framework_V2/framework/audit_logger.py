"""
framework/audit_logger.py
===========================
AuditLogger writes structured pipeline run events to two BigQuery tables:

  ``run_audit``    — one row per pipeline run (start, end, status, row counts)
  ``dq_error_log`` — one row per DQ rule failure within a run

Both use streaming inserts (insert_rows_json) for low-latency writes
from Cloud Run / local processes.  The DAG itself may also call
``write_run_summary`` at the very end for a batch perspective.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from .dq_validator import DQResult, DQSummary

logger = logging.getLogger(__name__)


class AuditLogger:
    """
    Parameters
    ----------
    client : bigquery.Client
    project : str
    dataset : str
        Framework dataset (default: ``framework_config``).
    """

    _RUN_AUDIT_TABLE = "run_audit"
    _DQ_ERROR_LOG_TABLE = "dq_error_log"

    def __init__(
        self,
        client: bigquery.Client,
        project: str,
        dataset: str = "framework_config",
    ) -> None:
        self._client = client
        self._project = project
        self._dataset = dataset

    # ── Run Audit ─────────────────────────────────────────────────────────────

    def write_run_start(
        self,
        registry_id: str,
        run_id: str,
        dag_run_id: str,
        watermark_from: datetime | None,
        watermark_to: datetime,
        load_mode: str,
    ) -> None:
        """Insert a run_audit row in RUNNING status."""
        row = {
            "run_id": run_id,
            "registry_id": registry_id,
            "dag_run_id": dag_run_id,
            "status": "RUNNING",
            "load_mode": load_mode,
            "watermark_from": watermark_from.isoformat() if watermark_from else None,
            "watermark_to": watermark_to.isoformat(),
            "started_at": datetime.now(tz=timezone.utc).isoformat(),
            "ended_at": None,
            "rows_extracted": None,
            "rows_inserted": None,
            "rows_updated": None,
            "rows_quarantined": None,
            "dq_rules_passed": None,
            "dq_rules_failed": None,
            "error_message": None,
            "error_type": None,
        }
        self._stream(self._RUN_AUDIT_TABLE, [row])
        logger.debug("Run start written: run_id=%s registry_id=%s", run_id, registry_id)

    def write_run_end(
        self,
        registry_id: str,
        run_id: str,
        status: str,
        started_at: datetime,
        rows_extracted: int = 0,
        rows_affected: int = 0,
        rows_quarantined: int = 0,
        dq_summary: DQSummary | None = None,
        error_message: str = "",
        error_type: str = "",
    ) -> None:
        """Update run_audit with final status. Uses a DML UPDATE for accuracy."""
        ended_at = datetime.now(tz=timezone.utc)
        duration_secs = (ended_at - started_at).total_seconds()

        dq_passed = len([r for r in (dq_summary.results if dq_summary else []) if r.passed])
        dq_failed = len([r for r in (dq_summary.results if dq_summary else []) if not r.passed])

        sql = f"""
        UPDATE `{self._project}.{self._dataset}.{self._RUN_AUDIT_TABLE}`
        SET
            status            = @status,
            ended_at          = @ended_at,
            duration_secs     = @duration_secs,
            rows_extracted    = @rows_extracted,
            rows_affected     = @rows_affected,
            rows_quarantined  = @rows_quarantined,
            dq_rules_passed   = @dq_passed,
            dq_rules_failed   = @dq_failed,
            error_message     = @error_message,
            error_type        = @error_type
        WHERE run_id = @run_id
        """
        params = [
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("ended_at", "TIMESTAMP", ended_at),
            bigquery.ScalarQueryParameter("duration_secs", "FLOAT64", duration_secs),
            bigquery.ScalarQueryParameter("rows_extracted", "INT64", rows_extracted),
            bigquery.ScalarQueryParameter("rows_affected", "INT64", rows_affected),
            bigquery.ScalarQueryParameter("rows_quarantined", "INT64", rows_quarantined),
            bigquery.ScalarQueryParameter("dq_passed", "INT64", dq_passed),
            bigquery.ScalarQueryParameter("dq_failed", "INT64", dq_failed),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("error_type", "STRING", error_type),
        ]
        try:
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            self._client.query(sql, job_config=job_config).result()
            logger.info(
                "Run end written: run_id=%s status=%s duration=%.1fs",
                run_id, status, duration_secs,
            )
        except Exception as exc:
            logger.exception("Failed to write run_end for run_id=%s: %s", run_id, exc)

    # ── DQ Error Log ──────────────────────────────────────────────────────────

    def write_dq_errors(
        self,
        registry_id: str,
        run_id: str,
        dq_summary: DQSummary,
    ) -> None:
        """
        Write one dq_error_log row per failed DQ rule.
        Called after validation regardless of action type.
        """
        failed_results = [r for r in dq_summary.results if not r.passed]
        if not failed_results:
            return

        rows = []
        now = datetime.now(tz=timezone.utc).isoformat()
        for result in failed_results:
            rows.append({
                "error_id": str(uuid.uuid4()),
                "run_id": run_id,
                "registry_id": registry_id,
                "rule_id": result.rule_id,
                "rule_name": result.rule_name,
                "rule_type": result.rule_type,
                "action_on_fail": result.action_on_fail,
                "failed_row_count": result.failed_row_count,
                "sample_failing_sql": result.sample_failing_sql[:2048],  # truncate
                "error_message": result.error_message[:1024] if result.error_message else None,
                "logged_at": now,
            })

        self._stream(self._DQ_ERROR_LOG_TABLE, rows)
        logger.info(
            "DQ errors written: run_id=%s count=%d", run_id, len(rows)
        )

    # ── Private ───────────────────────────────────────────────────────────────

    def _stream(self, table_name: str, rows: list[dict]) -> None:
        if not rows:
            return
        table_ref = f"{self._project}.{self._dataset}.{table_name}"
        try:
            table = self._client.get_table(table_ref)
            errors = self._client.insert_rows_json(table, rows)
            if errors:
                logger.error("Streaming insert errors to %s: %s", table_name, errors)
        except Exception as exc:
            logger.exception("Failed to stream to %s: %s", table_name, exc)
