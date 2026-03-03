"""
BQ Delta Ingestion Framework
=============================
Public API surface for the framework package.

Quick start
-----------
::

    from framework import PipelineRunner

    runner = PipelineRunner(project="my-gcp-project")
    result = runner.run("sales_orders_v1", dag_run_id="airflow_run_001")
    print(result.status)   # SUCCESS | FAILED | DQ_BLOCKED | LOCK_CONFLICT | NO_DELTA
"""

from .audit_logger import AuditLogger
from .config_loader import ConfigLoader, DQRule, TableConfig
from .delta_detector import DeltaDetector
from .dq_validator import DQSummary, DQValidator
from .exceptions import (
    AuditWriteError,
    ConfigError,
    DQBlockedError,
    DQWarningError,
    ExtractionError,
    FrameworkError,
    IngestionError,
    LockConflictError,
    LockReleaseError,
    StagingTableError,
    TableNotRegisteredError,
    WatermarkUpdateError,
)
from .ingestion_engine import IngestionEngine
from .pipeline_runner import PipelineResult, PipelineRunner
from .quarantine_manager import QuarantineManager
from .watermark_manager import WatermarkManager

__all__ = [
    # Runner (primary entry point)
    "PipelineRunner",
    "PipelineResult",
    # Components
    "ConfigLoader",
    "DeltaDetector",
    "DQValidator",
    "IngestionEngine",
    "WatermarkManager",
    "QuarantineManager",
    "AuditLogger",
    # Data models
    "TableConfig",
    "DQRule",
    "DQSummary",
    # Exceptions
    "FrameworkError",
    "ConfigError",
    "TableNotRegisteredError",
    "LockConflictError",
    "LockReleaseError",
    "ExtractionError",
    "StagingTableError",
    "DQBlockedError",
    "DQWarningError",
    "IngestionError",
    "AuditWriteError",
    "WatermarkUpdateError",
]
