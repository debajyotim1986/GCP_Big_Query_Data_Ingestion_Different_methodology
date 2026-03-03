"""
framework/config/process_control.py

Manages watermark timestamps and concurrency locking per table.
Prevents duplicate runs and enables incremental extraction windows.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple

from framework.utils.bq_client import BQClient

logger = logging.getLogger(__name__)

FRAMEWORK_PROJECT = "your-gcp-project"
FRAMEWORK_DATASET = "framework_config"
CONTROL_FQN = f"{FRAMEWORK_PROJECT}.{FRAMEWORK_DATASET}.process_control"


class ProcessControl:
    """
    Manages the process control table:
    - Reads last successful watermark for incremental extraction
    - Acquires/releases concurrency lock
    - Updates status and watermark after completion
    """

    def __init__(self, bq: BQClient):
        self.bq = bq

    def get_watermark(self, registry_id: str) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Returns (last_successful_run timestamp, last_run_id).
        Returns (None, None) if first run — will do full load.
        """
        sql = f"""
            SELECT last_successful_run, last_run_id
            FROM `{CONTROL_FQN}`
            WHERE registry_id = '{registry_id}'
            LIMIT 1
        """
        rows = self.bq.run_rows_query(sql)
        if not rows or rows[0]["last_successful_run"] is None:
            logger.info("First run detected for %s — full load mode", registry_id)
            return None, None

        ts = rows[0]["last_successful_run"]
        run_id = rows[0]["last_run_id"]
        logger.info("Watermark for %s: %s", registry_id, ts)
        return ts, run_id

    def acquire_lock(self, registry_id: str, dag_run_id: str) -> bool:
        """
        Try to acquire a processing lock. Returns False if already locked.
        Uses MERGE for atomic check-and-set.
        """
        locked_at = datetime.now(timezone.utc).isoformat()
        sql = f"""
            MERGE `{CONTROL_FQN}` T
            USING (SELECT '{registry_id}' AS registry_id) S
            ON T.registry_id = S.registry_id
            WHEN MATCHED AND (T.status NOT IN ('RUNNING') OR T.locked_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR)) THEN
              UPDATE SET
                status = 'RUNNING',
                locked_at = TIMESTAMP('{locked_at}'),
                locked_by = '{dag_run_id}'
            WHEN NOT MATCHED THEN
              INSERT (registry_id, status, locked_at, locked_by)
              VALUES ('{registry_id}', 'RUNNING', TIMESTAMP('{locked_at}'), '{dag_run_id}')
        """
        self.bq.run_query(sql)

        # Verify we actually hold the lock
        check_sql = f"""
            SELECT locked_by FROM `{CONTROL_FQN}`
            WHERE registry_id = '{registry_id}'
        """
        rows = self.bq.run_rows_query(check_sql)
        if rows and rows[0]["locked_by"] == dag_run_id:
            logger.info("Lock acquired for %s by %s", registry_id, dag_run_id)
            return True

        logger.warning("Lock NOT acquired for %s — already held by another run", registry_id)
        return False

    def update_success(
        self,
        registry_id: str,
        run_id: str,
        watermark_to: datetime,
        row_count: int
    ) -> None:
        """Update process control after successful run — advances watermark."""
        sql = f"""
            UPDATE `{CONTROL_FQN}`
            SET
              last_successful_run = TIMESTAMP('{watermark_to.isoformat()}'),
              last_run_id = '{run_id}',
              last_row_count = {row_count},
              status = 'SUCCESS',
              locked_at = NULL,
              locked_by = NULL
            WHERE registry_id = '{registry_id}'
        """
        self.bq.run_query(sql)
        logger.info("Process control updated for %s — new watermark: %s", registry_id, watermark_to)

    def update_failure(self, registry_id: str, error_message: str) -> None:
        """Mark run as failed — watermark NOT advanced."""
        safe_msg = error_message.replace("'", "\\'")[:500]
        sql = f"""
            UPDATE `{CONTROL_FQN}`
            SET
              status = 'FAILED',
              locked_at = NULL,
              locked_by = NULL
            WHERE registry_id = '{registry_id}'
        """
        self.bq.run_query(sql)
        logger.error("Process control marked FAILED for %s: %s", registry_id, error_message)

    def release_lock(self, registry_id: str) -> None:
        """Force-release lock (used in finally blocks)."""
        sql = f"""
            UPDATE `{CONTROL_FQN}`
            SET locked_at = NULL, locked_by = NULL
            WHERE registry_id = '{registry_id}'
        """
        self.bq.run_query(sql)
