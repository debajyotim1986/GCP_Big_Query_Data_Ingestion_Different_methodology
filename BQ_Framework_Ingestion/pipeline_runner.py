"""
framework/pipeline_runner.py

Core orchestrator for one table's delta ingestion pipeline.
Called by the Composer PythonOperator per table.

Flow:
  1. Load config from BQ
  2. Acquire process control lock + get watermark
  3. Detect delta → staging table
  4. Run DQ rules
  5. If DQ passes → MERGE to target
  6. Update process control (advance watermark)
  7. Write audit record
  8. Cleanup staging table
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from framework.audit.audit_writer import AuditWriter
from framework.config.config_loader import ConfigLoader
from framework.config.process_control import ProcessControl
from framework.delta.delta_detector import DeltaDetector
from framework.dq.dq_validator import DQSummary, DQValidator
from framework.ingestion.ingestion_engine import IngestionEngine
from framework.utils.bq_client import BQClient

logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Executes the full delta ingestion pipeline for a single registered table.
    Designed to be instantiated fresh per DAG task execution.
    """

    def __init__(self, gcp_project: str):
        self.bq = BQClient.get_instance(gcp_project)
        self.config_loader = ConfigLoader(self.bq)
        self.process_control = ProcessControl(self.bq)
        self.delta_detector = DeltaDetector(self.bq)
        self.dq_validator = DQValidator(self.bq)
        self.ingestion_engine = IngestionEngine(self.bq)
        self.audit_writer = AuditWriter(self.bq)

    def run(self, registry_id: str, dag_run_id: str) -> dict:
        """
        Execute the full pipeline for a single table.
        
        Args:
            registry_id:  Unique ID from table_registry
            dag_run_id:   Airflow DAG run ID (for audit + locking)
        
        Returns:
            dict with run summary (logged and returned to XCom)
        """
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        watermark_to = started_at  # Snapshot point-in-time
        dq_summary: Optional[DQSummary] = None
        source_count = 0
        delta_count = 0

        logger.info("=" * 60)
        logger.info("Pipeline START: registry_id=%s run_id=%s", registry_id, run_id)

        # ── 1. Load config ─────────────────────────────────────
        config = self.config_loader.load_config(registry_id)
        logger.info("Config loaded: %s → %s", config.source_fqn, config.target_fqn)

        # ── 2. Acquire lock + get watermark ────────────────────
        lock_acquired = self.process_control.acquire_lock(registry_id, dag_run_id)
        if not lock_acquired:
            raise RuntimeError(f"Could not acquire lock for {registry_id} — another run may be active")

        watermark_from, _ = self.process_control.get_watermark(registry_id)

        try:
            # ── 3. Delta detection ─────────────────────────────
            source_count, delta_count = self.delta_detector.detect_and_stage(
                config, watermark_from, watermark_to
            )

            if delta_count == 0:
                logger.info("No delta records found — skipping DQ and ingestion")
                self.process_control.update_success(registry_id, run_id, watermark_to, 0)
                self.audit_writer.write_success(
                    run_id, dag_run_id, config, watermark_from, watermark_to,
                    source_count, 0, DQSummary(0, 0, 0, 0, 0), 0, started_at
                )
                return self._build_summary(run_id, registry_id, "SUCCESS", source_count, 0, None, 0)

            # ── 4. DQ validation ───────────────────────────────
            dq_summary = self.dq_validator.validate(config, run_id)

            if dq_summary.blocked:
                logger.error(
                    "DQ REJECT rules failed — pipeline BLOCKED for %s. "
                    "Failed rules: %d. Watermark NOT advanced.",
                    registry_id,
                    dq_summary.failed_reject_count
                )
                self.process_control.update_failure(
                    registry_id,
                    f"DQ blocked: {dq_summary.failed_reject_count} REJECT rule(s) failed"
                )
                self.audit_writer.write_failure(
                    run_id, dag_run_id, config, watermark_from, watermark_to,
                    f"DQ BLOCKED: {dq_summary.failed_reject_count} REJECT rules failed",
                    started_at, source_count, delta_count, dq_summary, status="DQ_BLOCKED"
                )
                raise RuntimeError(
                    f"DQ validation blocked pipeline for {registry_id}. "
                    f"See dq_error_log for run_id={run_id}"
                )

            # ── 5. Ingest delta to target ──────────────────────
            # After QUARANTINE rows removed from staging, remaining rows are clean
            ingested_count = self.ingestion_engine.merge_to_target(config)

            # ── 6. Advance watermark ───────────────────────────
            self.process_control.update_success(
                registry_id, run_id, watermark_to, ingested_count
            )

            # ── 7. Write audit ─────────────────────────────────
            self.audit_writer.write_success(
                run_id, dag_run_id, config, watermark_from, watermark_to,
                source_count, delta_count, dq_summary, ingested_count, started_at
            )

            logger.info(
                "Pipeline SUCCESS: registry_id=%s ingested=%d duration=%.1fs",
                registry_id, ingested_count,
                (datetime.now(timezone.utc) - started_at).total_seconds()
            )
            return self._build_summary(run_id, registry_id, "SUCCESS", source_count, delta_count, dq_summary, ingested_count)

        except RuntimeError:
            raise  # Already handled above

        except Exception as e:
            logger.exception("Pipeline FAILED for %s: %s", registry_id, str(e))
            self.process_control.update_failure(registry_id, str(e))
            self.audit_writer.write_failure(
                run_id, dag_run_id, config, watermark_from, watermark_to,
                str(e), started_at, source_count, delta_count, dq_summary
            )
            raise

        finally:
            # ── 8. Cleanup staging ─────────────────────────────
            self.bq.drop_table_if_exists(config.staging_fqn)
            logger.info("Staging table cleaned: %s", config.staging_fqn)

    def _build_summary(
        self,
        run_id: str,
        registry_id: str,
        status: str,
        source_count: int,
        delta_count: int,
        dq_summary: Optional[DQSummary],
        ingested_count: int
    ) -> dict:
        return {
            "run_id": run_id,
            "registry_id": registry_id,
            "status": status,
            "source_row_count": source_count,
            "delta_row_count": delta_count,
            "dq_passed_rules": dq_summary.passed_rules if dq_summary else 0,
            "dq_failed_rules": len(dq_summary.rule_results) - dq_summary.passed_rules if dq_summary else 0,
            "quarantined_rows": dq_summary.quarantined_count if dq_summary else 0,
            "ingested_row_count": ingested_count
        }
