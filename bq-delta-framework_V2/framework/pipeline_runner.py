"""
framework/pipeline_runner.py
==============================
PipelineRunner is the top-level orchestrator. It wires together every
framework component and executes the 7-step pipeline for a single table.

Step sequence
-------------
Step 1  ConfigLoader.load()             → resolve config + watermark window
Step 2  WatermarkManager.acquire_lock() → atomic lock via MERGE
Step 3  DeltaDetector.extract_delta()   → write changed rows to staging table
Step 4  DQValidator.validate()          → evaluate all active DQ rules
Step 5  QuarantineManager              → isolate QUARANTINE-action failures
Step 6  IngestionEngine.merge()         → MERGE staging → target
Step 7  WatermarkManager.advance_watermark() + AuditLogger.write_run_end()

Error handling
--------------
The ``finally`` block in ``run()`` always fires regardless of how the
pipeline exits. It:
  - Releases the lock (sets status=FAILED or DQ_BLOCKED)
  - Drops the staging table
  - Writes the terminal run_audit row

This guarantees that no lock is left permanently held and no staging
table is left orphaned even on unhandled exceptions.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from google.cloud import bigquery

from .audit_logger import AuditLogger
from .config_loader import ConfigLoader, TableConfig
from .delta_detector import DeltaDetector
from .dq_validator import DQSummary, DQValidator
from .exceptions import (
    DQBlockedError,
    FrameworkError,
    LockConflictError,
)
from .ingestion_engine import IngestionEngine
from .quarantine_manager import QuarantineManager
from .watermark_manager import WatermarkManager

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    registry_id: str
    run_id: str
    status: str                   # SUCCESS | FAILED | DQ_BLOCKED | LOCK_CONFLICT | NO_DELTA
    load_mode: str                # FULL_LOAD | INCREMENTAL
    rows_extracted: int = 0
    rows_affected: int = 0
    rows_quarantined: int = 0
    dq_summary: DQSummary | None = None
    error_message: str = ""
    duration_secs: float = 0.0


class PipelineRunner:
    """
    Single-table delta ingestion pipeline.

    Parameters
    ----------
    project : str
        GCP project ID.
    dataset : str
        Framework BigQuery dataset (default: ``framework_config``).
    location : str
        BigQuery dataset location (default: ``US``).
    lock_expiry_hours : int
        Hours before a stale lock is auto-expired.

    Example
    -------
    ::

        from google.cloud import bigquery
        from framework.pipeline_runner import PipelineRunner

        client = bigquery.Client(project="my-project")
        runner = PipelineRunner(project="my-project")
        result = runner.run("sales_orders_v1", dag_run_id="manual_20250101")
        print(result.status, result.rows_affected)
    """

    def __init__(
        self,
        project: str | None = None,
        dataset: str = "framework_config",
        location: str = "US",
        lock_expiry_hours: int = 4,
    ) -> None:
        self._project = project or os.environ["GCP_PROJECT"]
        self._dataset = dataset
        self._location = location

        # Shared BQ client — singleton per runner instance
        self._client = bigquery.Client(
            project=self._project,
            location=self._location,
        )

        # Initialise all components with the shared client
        self._config_loader = ConfigLoader(self._client, self._project, self._dataset)
        self._watermark_mgr = WatermarkManager(
            self._client, self._project, self._dataset, lock_expiry_hours
        )
        self._delta_detector = DeltaDetector(self._client, self._project, self._dataset)
        self._dq_validator = DQValidator(self._client, self._project, self._dataset)
        self._quarantine_mgr = QuarantineManager(self._client, self._project, self._dataset)
        self._ingestion_engine = IngestionEngine(self._client, self._project, self._dataset)
        self._audit_logger = AuditLogger(self._client, self._project, self._dataset)

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        registry_id: str,
        dag_run_id: str = "",
    ) -> PipelineResult:
        """
        Execute the full 7-step delta ingestion pipeline for *registry_id*.

        Returns a PipelineResult with the final status and metrics.
        Never raises — all exceptions are caught and reflected in the result.
        """
        started_at = datetime.now(tz=timezone.utc)
        run_id = ""
        config: TableConfig | None = None
        dq_summary: DQSummary | None = None
        staging_ref = ""
        rows_extracted = 0
        rows_affected = 0
        rows_quarantined = 0
        final_status = "FAILED"
        error_message = ""
        lock_acquired = False

        logger.info("=" * 60)
        logger.info("PIPELINE START: registry_id=%s dag_run_id=%s", registry_id, dag_run_id)

        try:
            # ── Step 1: Load configuration ────────────────────────────────────
            logger.info("[Step 1/7] Loading config: %s", registry_id)
            config = self._config_loader.load(registry_id)
            staging_ref = self._delta_detector._staging_ref(registry_id)

            # ── Step 2: Acquire concurrency lock ──────────────────────────────
            logger.info("[Step 2/7] Acquiring lock: %s", registry_id)
            run_id = self._watermark_mgr.acquire_lock(registry_id, dag_run_id)
            lock_acquired = True

            # Write run start to audit log
            self._audit_logger.write_run_start(
                registry_id=registry_id,
                run_id=run_id,
                dag_run_id=dag_run_id,
                watermark_from=config.watermark_from,
                watermark_to=config.watermark_to,
                load_mode=config.load_mode,
            )

            logger.info(
                "[Step 2/7] Lock acquired — mode=%s watermark_from=%s watermark_to=%s",
                config.load_mode, config.watermark_from, config.watermark_to,
            )

            # ── Step 3: Extract delta into staging ────────────────────────────
            logger.info("[Step 3/7] Extracting delta: %s", registry_id)
            rows_extracted = self._delta_detector.extract_delta(config)
            logger.info("[Step 3/7] Extracted %d rows", rows_extracted)

            if rows_extracted == 0:
                logger.info(
                    "[Step 3/7] Zero delta rows — advancing watermark and exiting cleanly"
                )
                self._watermark_mgr.advance_watermark(
                    registry_id, run_id, config.watermark_to, row_count=0
                )
                self._audit_logger.write_run_end(
                    registry_id=registry_id,
                    run_id=run_id,
                    status="SUCCESS",
                    started_at=started_at,
                    rows_extracted=0,
                )
                duration = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
                return PipelineResult(
                    registry_id=registry_id,
                    run_id=run_id,
                    status="NO_DELTA",
                    load_mode=config.load_mode,
                    duration_secs=duration,
                )

            # ── Step 4: DQ Validation ─────────────────────────────────────────
            logger.info("[Step 4/7] Running DQ validation: %s", registry_id)
            dq_summary = self._dq_validator.validate(config, staging_ref, run_id)
            self._audit_logger.write_dq_errors(registry_id, run_id, dq_summary)

            # ── Step 5: Quarantine handling ───────────────────────────────────
            if dq_summary.quarantine_failures:
                logger.info(
                    "[Step 5/7] Quarantining %d rule failures", len(dq_summary.quarantine_failures)
                )
                for result in dq_summary.quarantine_failures:
                    failing_rows = self._dq_validator.get_failing_rows(
                        rule=next(r for r in config.dq_rules if r.rule_id == result.rule_id),
                        config=config,
                        staging_ref=staging_ref,
                    )
                    rows_quarantined += self._quarantine_mgr.quarantine_rows(
                        config=config,
                        run_id=run_id,
                        rule_result=result,
                        failing_rows=failing_rows,
                        staging_ref=staging_ref,
                    )
            else:
                logger.info("[Step 5/7] No quarantine action needed")

            # Check for REJECT-action failures → halt the pipeline
            if dq_summary.reject_failures:
                reject_names = [r.rule_name for r in dq_summary.reject_failures]
                raise DQBlockedError(
                    f"REJECT rules failed: {reject_names}",
                    registry_id=registry_id,
                    run_id=run_id,
                    failed_rules=[
                        {"rule_id": r.rule_id, "failed_rows": r.failed_row_count}
                        for r in dq_summary.reject_failures
                    ],
                )

            # ── Step 6: Merge into target ─────────────────────────────────────
            logger.info("[Step 6/7] Merging staging → target: %s", registry_id)
            rows_affected = self._ingestion_engine.merge(config, staging_ref)
            logger.info("[Step 6/7] Merge complete — %d rows affected", rows_affected)

            # ── Step 7: Advance watermark + finalise audit ────────────────────
            logger.info("[Step 7/7] Advancing watermark: %s", registry_id)
            self._watermark_mgr.advance_watermark(
                registry_id=registry_id,
                run_id=run_id,
                watermark_to=config.watermark_to,
                row_count=rows_affected,
            )
            self._audit_logger.write_run_end(
                registry_id=registry_id,
                run_id=run_id,
                status="SUCCESS",
                started_at=started_at,
                rows_extracted=rows_extracted,
                rows_affected=rows_affected,
                rows_quarantined=rows_quarantined,
                dq_summary=dq_summary,
            )
            final_status = "SUCCESS"
            logger.info("PIPELINE SUCCESS: registry_id=%s run_id=%s", registry_id, run_id)

        except LockConflictError as exc:
            final_status = "LOCK_CONFLICT"
            error_message = str(exc)
            logger.warning("LOCK CONFLICT: registry_id=%s — %s", registry_id, exc)
            lock_acquired = False  # We don't hold the lock — skip release

        except DQBlockedError as exc:
            final_status = "DQ_BLOCKED"
            error_message = str(exc)
            logger.error("DQ BLOCKED: registry_id=%s — %s", registry_id, exc)

        except FrameworkError as exc:
            final_status = "FAILED"
            error_message = str(exc)
            logger.exception("FRAMEWORK ERROR: registry_id=%s — %s", registry_id, exc)

        except Exception as exc:
            final_status = "FAILED"
            error_message = str(exc)
            logger.exception("UNEXPECTED ERROR: registry_id=%s — %s", registry_id, exc)

        finally:
            # Always: release lock (if held) and drop staging table
            if lock_acquired and run_id and final_status not in ("SUCCESS", "NO_DELTA"):
                terminal_lock_status = (
                    "DQ_BLOCKED" if final_status == "DQ_BLOCKED" else "FAILED"
                )
                self._watermark_mgr.release_lock(registry_id, run_id, terminal_lock_status)
                self._audit_logger.write_run_end(
                    registry_id=registry_id,
                    run_id=run_id,
                    status=final_status,
                    started_at=started_at,
                    rows_extracted=rows_extracted,
                    rows_affected=rows_affected,
                    rows_quarantined=rows_quarantined,
                    dq_summary=dq_summary,
                    error_message=error_message,
                    error_type=final_status,
                )

            if staging_ref:
                self._delta_detector.drop_staging_table(registry_id)

            logger.info(
                "PIPELINE END: registry_id=%s status=%s", registry_id, final_status
            )

        duration = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
        return PipelineResult(
            registry_id=registry_id,
            run_id=run_id,
            status=final_status,
            load_mode=config.load_mode if config else "UNKNOWN",
            rows_extracted=rows_extracted,
            rows_affected=rows_affected,
            rows_quarantined=rows_quarantined,
            dq_summary=dq_summary,
            error_message=error_message,
            duration_secs=duration,
        )
