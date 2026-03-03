"""
framework/watermark_manager.py
================================
WatermarkManager owns all mutations to ``process_control``.

Responsibilities
----------------
1. **acquire_lock** – Atomic MERGE that inserts or updates the row to
   status=RUNNING.  Raises LockConflictError if another run is active
   and its lock has not expired.

2. **release_lock** – Called in the PipelineRunner ``finally`` block.
   Updates status to FAILED / DQ_BLOCKED / SUCCESS.

3. **advance_watermark** – Called only on SUCCESS.  Moves
   ``last_successful_run`` forward to ``watermark_to``.

Key invariant: ``last_successful_run`` only moves forward, and only on SUCCESS.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

from .exceptions import LockConflictError, LockReleaseError, WatermarkUpdateError

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Parameters
    ----------
    client : bigquery.Client
    project : str
    dataset : str
        Framework dataset name (default: ``framework_config``).
    lock_expiry_hours : int
        Number of hours after which a stale lock is considered expired.
    """

    # language=SQL
    ACQUIRE_LOCK_SQL = """
    MERGE `{project}.{dataset}.process_control` AS T
    USING (
        SELECT
            @registry_id          AS registry_id,
            @run_id               AS run_id,
            @dag_run_id           AS dag_run_id,
            CURRENT_TIMESTAMP()   AS now
    ) AS S
    ON T.registry_id = S.registry_id
    WHEN MATCHED AND (
            T.status NOT IN ('RUNNING')
            OR TIMESTAMP_DIFF(S.now, T.locked_at, HOUR) >= @lock_expiry_hours
    ) THEN UPDATE SET
        T.status        = 'RUNNING',
        T.locked_at     = S.now,
        T.locked_by     = S.run_id,
        T.dag_run_id    = S.dag_run_id,
        T.updated_at    = S.now
    WHEN NOT MATCHED THEN INSERT (
        registry_id, status, locked_at, locked_by, dag_run_id,
        last_successful_run, last_run_id, last_row_count,
        lock_expiry_hours, created_at, updated_at
    ) VALUES (
        S.registry_id, 'RUNNING', S.now, S.run_id, S.dag_run_id,
        NULL, NULL, NULL,
        @lock_expiry_hours, S.now, S.now
    )
    """

    # language=SQL
    CHECK_LOCK_SQL = """
    SELECT status, locked_by, locked_at,
           TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), locked_at, HOUR) AS hours_held
    FROM `{project}.{dataset}.process_control`
    WHERE registry_id = @registry_id
    LIMIT 1
    """

    # language=SQL
    RELEASE_LOCK_SQL = """
    UPDATE `{project}.{dataset}.process_control`
    SET
        status      = @status,
        locked_at   = NULL,
        locked_by   = NULL,
        last_run_id = @run_id,
        updated_at  = CURRENT_TIMESTAMP()
    WHERE registry_id = @registry_id
    """

    # language=SQL
    ADVANCE_WATERMARK_SQL = """
    UPDATE `{project}.{dataset}.process_control`
    SET
        last_successful_run = @watermark_to,
        last_run_id         = @run_id,
        last_row_count      = @row_count,
        status              = 'SUCCESS',
        locked_at           = NULL,
        locked_by           = NULL,
        updated_at          = CURRENT_TIMESTAMP()
    WHERE registry_id = @registry_id
    """

    def __init__(
        self,
        client: bigquery.Client,
        project: str,
        dataset: str = "framework_config",
        lock_expiry_hours: int = 4,
    ) -> None:
        self._client = client
        self._project = project
        self._dataset = dataset
        self._lock_expiry_hours = lock_expiry_hours

    def _table(self, name: str) -> str:
        return f"`{self._project}.{self._dataset}.{name}`"

    def _run_sql(self, sql: str, params: list) -> bigquery.QueryJob:
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        return self._client.query(
            sql.format(project=self._project, dataset=self._dataset),
            job_config=job_config,
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def acquire_lock(
        self,
        registry_id: str,
        dag_run_id: str = "",
    ) -> str:
        """
        Atomically acquire the lock for *registry_id*.

        Returns
        -------
        str
            A fresh ``run_id`` (UUID4) for this pipeline execution.

        Raises
        ------
        LockConflictError
            If another run is currently RUNNING and the lock has not expired.
        """
        run_id = str(uuid.uuid4())
        logger.info("Acquiring lock: registry_id=%s run_id=%s", registry_id, run_id)

        params = [
            bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id),
            bigquery.ScalarQueryParameter("lock_expiry_hours", "INT64", self._lock_expiry_hours),
        ]
        job = self._run_sql(self.ACQUIRE_LOCK_SQL, params)
        job.result()  # wait and raise on BQ error

        # Verify we actually hold the lock (MERGE may have been a no-op)
        lock_row = self._check_lock(registry_id)
        if lock_row is None:
            # Should not happen after INSERT WHEN NOT MATCHED
            raise LockConflictError(
                f"Lock acquisition returned no row for {registry_id}",
                registry_id=registry_id,
                run_id=run_id,
            )

        if lock_row["locked_by"] != run_id:
            raise LockConflictError(
                f"Lock held by {lock_row['locked_by']} "
                f"for {lock_row['hours_held']:.1f}h — not acquired.",
                registry_id=registry_id,
                run_id=run_id,
                locked_by=lock_row["locked_by"],
            )

        logger.info("Lock acquired: registry_id=%s run_id=%s", registry_id, run_id)
        return run_id

    def release_lock(
        self,
        registry_id: str,
        run_id: str,
        status: str,
    ) -> None:
        """
        Release the lock and set a terminal status (FAILED or DQ_BLOCKED).
        Called in the ``finally`` block — must not raise.

        Parameters
        ----------
        status : str
            One of ``FAILED``, ``DQ_BLOCKED``.  SUCCESS is set by
            ``advance_watermark`` instead.
        """
        logger.info(
            "Releasing lock: registry_id=%s run_id=%s status=%s",
            registry_id, run_id, status,
        )
        try:
            params = [
                bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("status", "STRING", status),
            ]
            job = self._run_sql(self.RELEASE_LOCK_SQL, params)
            job.result()
        except Exception as exc:
            # Non-fatal in finally — log and continue
            logger.exception(
                "Failed to release lock for registry_id=%s run_id=%s: %s",
                registry_id, run_id, exc,
            )

    def advance_watermark(
        self,
        registry_id: str,
        run_id: str,
        watermark_to: datetime,
        row_count: int,
    ) -> None:
        """
        Advance ``last_successful_run`` to *watermark_to* and set status=SUCCESS.
        Called ONLY on successful ingestion.

        Raises
        ------
        WatermarkUpdateError
            If the BQ UPDATE fails.
        """
        logger.info(
            "Advancing watermark: registry_id=%s run_id=%s watermark_to=%s row_count=%d",
            registry_id, run_id, watermark_to.isoformat(), row_count,
        )
        try:
            params = [
                bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("watermark_to", "TIMESTAMP", watermark_to),
                bigquery.ScalarQueryParameter("row_count", "INT64", row_count),
            ]
            job = self._run_sql(self.ADVANCE_WATERMARK_SQL, params)
            job.result()
        except Exception as exc:
            raise WatermarkUpdateError(
                f"Failed to advance watermark for {registry_id}: {exc}",
                registry_id=registry_id,
                run_id=run_id,
            ) from exc

    def is_locked(self, registry_id: str) -> bool:
        """Check whether another run currently holds the lock (non-expired)."""
        row = self._check_lock(registry_id)
        if row is None:
            return False
        return row["status"] == "RUNNING" and row["hours_held"] < self._lock_expiry_hours

    # ── Private helpers ───────────────────────────────────────────────────────

    def _check_lock(self, registry_id: str) -> dict | None:
        params = [bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id)]
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        sql = self.CHECK_LOCK_SQL.format(
            project=self._project, dataset=self._dataset
        )
        rows = list(self._client.query(sql, job_config=job_config).result())
        return dict(rows[0]) if rows else None
