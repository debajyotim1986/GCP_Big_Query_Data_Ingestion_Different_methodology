"""
framework/exceptions.py
========================
Custom exception hierarchy for the BQ Delta Ingestion Framework.

Every exception carries a registry_id and run_id so the AuditLogger
can write a structured failure row even when the error propagates
outside the normal PipelineRunner flow.
"""

from __future__ import annotations


class FrameworkError(Exception):
    """Base class for all framework-raised exceptions."""

    def __init__(self, message: str, registry_id: str = "", run_id: str = "") -> None:
        super().__init__(message)
        self.registry_id = registry_id
        self.run_id = run_id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"registry_id={self.registry_id!r}, "
            f"run_id={self.run_id!r}, "
            f"message={str(self)!r})"
        )


# ── Configuration ────────────────────────────────────────────────────────────

class ConfigError(FrameworkError):
    """Raised when table_registry row is missing, incomplete, or invalid."""


class TableNotRegisteredError(ConfigError):
    """Raised when a registry_id is not found in table_registry."""


# ── Lock / Concurrency ───────────────────────────────────────────────────────

class LockConflictError(FrameworkError):
    """Raised when acquire_lock detects another run holds the lock."""

    def __init__(
        self,
        message: str,
        registry_id: str = "",
        run_id: str = "",
        locked_by: str = "",
    ) -> None:
        super().__init__(message, registry_id, run_id)
        self.locked_by = locked_by


class LockReleaseError(FrameworkError):
    """Raised when releasing a lock fails (BQ error in finally block)."""


# ── Extraction / Delta Detection ─────────────────────────────────────────────

class ExtractionError(FrameworkError):
    """Raised when the delta extraction query fails on BigQuery."""


class StagingTableError(FrameworkError):
    """Raised when creating or dropping the staging table fails."""


# ── Data Quality ─────────────────────────────────────────────────────────────

class DQBlockedError(FrameworkError):
    """
    Raised when a REJECT-action DQ rule fails.
    Pipeline halts; watermark does NOT advance.
    """

    def __init__(
        self,
        message: str,
        registry_id: str = "",
        run_id: str = "",
        failed_rules: list[dict] | None = None,
    ) -> None:
        super().__init__(message, registry_id, run_id)
        self.failed_rules: list[dict] = failed_rules or []


class DQWarningError(FrameworkError):
    """
    Raised when a WARN-action DQ rule fails.
    Pipeline continues; warning is written to dq_error_log.
    """


# ── Ingestion ────────────────────────────────────────────────────────────────

class IngestionError(FrameworkError):
    """Raised when the final MERGE into the target table fails."""


# ── Audit / Watermark ────────────────────────────────────────────────────────

class AuditWriteError(FrameworkError):
    """Raised when writing to run_audit or dq_error_log fails."""


class WatermarkUpdateError(FrameworkError):
    """Raised when updating process_control.last_successful_run fails."""
