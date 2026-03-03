"""
framework/audit/audit_writer.py

Writes run summary to run_audit table at end of every pipeline execution.
Called regardless of success/failure.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from framework.config.config_loader import TableConfig
from framework.dq.dq_validator import DQSummary
from framework.utils.bq_client import BQClient

logger = logging.getLogger(__name__)

FRAMEWORK_PROJECT = "your-gcp-project"
FRAMEWORK_DATASET = "framework_config"
AUDIT_FQN = f"{FRAMEWORK_PROJECT}.{FRAMEWORK_DATASET}.run_audit"


class AuditWriter:
    def __init__(self, bq: BQClient):
        self.bq = bq

    def write_success(
        self,
        run_id: str,
        dag_run_id: str,
        config: TableConfig,
        watermark_from: Optional[datetime],
        watermark_to: datetime,
        source_row_count: int,
        delta_row_count: int,
        dq_summary: DQSummary,
        ingested_row_count: int,
        started_at: datetime
    ) -> None:
        completed_at = datetime.now(timezone.utc)
        duration = (completed_at - started_at).total_seconds()

        row = {
            "run_id": run_id,
            "dag_run_id": dag_run_id,
            "registry_id": config.registry_id,
            "source_table_fqn": config.source_fqn,
            "target_table_fqn": config.target_fqn,
            "watermark_from": watermark_from.isoformat() if watermark_from else None,
            "watermark_to": watermark_to.isoformat(),
            "source_row_count": source_row_count,
            "delta_row_count": delta_row_count,
            "dq_passed_count": dq_summary.passed_rules,
            "dq_failed_count": len(dq_summary.rule_results) - dq_summary.passed_rules,
            "ingested_row_count": ingested_row_count,
            "status": "SUCCESS",
            "error_message": None,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_seconds": round(duration, 2),
            "created_at": completed_at.isoformat()
        }
        self.bq.insert_rows_json(AUDIT_FQN, [row])
        logger.info("Audit written: run_id=%s status=SUCCESS duration=%.1fs", run_id, duration)

    def write_failure(
        self,
        run_id: str,
        dag_run_id: str,
        config: TableConfig,
        watermark_from: Optional[datetime],
        watermark_to: datetime,
        error_message: str,
        started_at: datetime,
        source_row_count: int = 0,
        delta_row_count: int = 0,
        dq_summary: Optional[DQSummary] = None,
        status: str = "FAILED"
    ) -> None:
        completed_at = datetime.now(timezone.utc)
        duration = (completed_at - started_at).total_seconds()

        row = {
            "run_id": run_id,
            "dag_run_id": dag_run_id,
            "registry_id": config.registry_id,
            "source_table_fqn": config.source_fqn,
            "target_table_fqn": config.target_fqn,
            "watermark_from": watermark_from.isoformat() if watermark_from else None,
            "watermark_to": watermark_to.isoformat(),
            "source_row_count": source_row_count,
            "delta_row_count": delta_row_count,
            "dq_passed_count": dq_summary.passed_rules if dq_summary else 0,
            "dq_failed_count": (len(dq_summary.rule_results) - dq_summary.passed_rules) if dq_summary else 0,
            "ingested_row_count": 0,
            "status": status,
            "error_message": str(error_message)[:500],
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_seconds": round(duration, 2),
            "created_at": completed_at.isoformat()
        }
        self.bq.insert_rows_json(AUDIT_FQN, [row])
        logger.error("Audit written: run_id=%s status=%s", run_id, status)
