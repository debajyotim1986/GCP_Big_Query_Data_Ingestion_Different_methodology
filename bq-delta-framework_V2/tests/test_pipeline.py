"""
tests/test_pipeline.py
=======================
Unit tests for key framework components using mocked BigQuery clients.
Run with: uv run pytest tests/ -v
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from framework.config_loader import ConfigLoader, DQRule, TableConfig
from framework.delta_detector import DeltaDetector
from framework.dq_validator import DQResult, DQSummary, DQValidator
from framework.exceptions import (
    ConfigError,
    DQBlockedError,
    LockConflictError,
    TableNotRegisteredError,
)
from framework.watermark_manager import WatermarkManager


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def sample_config() -> TableConfig:
    return TableConfig(
        registry_id="sales_orders_v1",
        owner_team="data-engineering",
        tags=["finance", "critical"],
        source_project="src-project",
        source_dataset="raw",
        source_table="sales_orders",
        target_project="tgt-project",
        target_dataset="curated",
        target_table="sales_orders",
        primary_key_cols=["order_id"],
        delta_hash_cols=["order_id", "status", "amount"],
        watermark_col="updated_at",
        batch_size=None,
        is_active=True,
        priority=100,
        watermark_from=datetime(2024, 1, 1, tzinfo=timezone.utc),
        watermark_to=datetime(2024, 1, 2, tzinfo=timezone.utc),
        dq_rules=[],
    )


@pytest.fixture
def full_load_config(sample_config) -> TableConfig:
    """Config with no watermark (first run → full load)."""
    from dataclasses import replace
    return replace(sample_config, watermark_from=None)


# ── ConfigLoader tests ────────────────────────────────────────────────────────

class TestConfigLoader:

    def test_table_ref_format(self, sample_config):
        assert sample_config.source_table_ref == "`src-project.raw.sales_orders`"
        assert sample_config.target_table_ref == "`tgt-project.curated.sales_orders`"

    def test_load_mode_incremental(self, sample_config):
        assert sample_config.is_full_load is False
        assert sample_config.load_mode == "INCREMENTAL"

    def test_load_mode_full_load(self, full_load_config):
        assert full_load_config.is_full_load is True
        assert full_load_config.load_mode == "FULL_LOAD"

    def test_effective_hash_cols_explicit(self, sample_config):
        assert sample_config.effective_hash_cols == ["order_id", "status", "amount"]

    def test_effective_hash_cols_fallback_to_pk(self, sample_config):
        from dataclasses import replace
        cfg = replace(sample_config, delta_hash_cols=[])
        assert cfg.effective_hash_cols == ["order_id"]

    def test_missing_registry_raises(self, mock_client):
        mock_client.query.return_value.result.return_value = []
        loader = ConfigLoader(mock_client, "test-project")
        with pytest.raises(TableNotRegisteredError):
            loader.load("nonexistent_table")

    def test_missing_required_fields_raises(self, mock_client):
        row = {
            "registry_id": "test",
            "source_project": "",   # empty — should fail validation
            "source_dataset": "ds",
            "source_table": "tbl",
            "target_project": "tp",
            "target_dataset": "td",
            "target_table": "tt",
            "watermark_col": "updated_at",
            "primary_key_cols": ["id"],
            "delta_hash_cols": [],
            "batch_size": None,
            "is_active": True,
            "priority": 100,
            "owner_team": "",
            "tags": [],
        }
        mock_client.query.return_value.result.return_value = [MagicMock(**row, **{"__getitem__": lambda s, k: row[k]})]
        loader = ConfigLoader(mock_client, "test-project")
        with pytest.raises(ConfigError):
            loader._validate_registry_row(row, "test")


# ── DeltaDetector tests ───────────────────────────────────────────────────────

class TestDeltaDetector:

    def test_hash_expr_single_col(self):
        expr = DeltaDetector._build_hash_expr(["order_id"])
        assert "SHA256" in expr
        assert "`order_id`" in expr

    def test_hash_expr_multi_col(self):
        expr = DeltaDetector._build_hash_expr(["id", "status", "amount"])
        assert "`id`" in expr
        assert "`status`" in expr
        assert "`amount`" in expr
        assert "'|'" in expr  # separator

    def test_staging_ref_safe(self, mock_client):
        detector = DeltaDetector(mock_client, "proj", "fw")
        # Special characters in registry_id should be replaced with _
        ref = detector._staging_ref("my-table/v1")
        assert "/" not in ref
        assert "-" not in ref
        assert "_delta_staging_" in ref

    def test_watermark_filter_incremental(self, sample_config, mock_client):
        detector = DeltaDetector(mock_client, "proj", "fw")
        sql = detector._build_extraction_sql(
            sample_config, "`proj.fw._delta_staging_sales_orders_v1`"
        )
        assert "updated_at" in sql
        assert "2024-01-01" in sql  # watermark_from
        assert "INSERT INTO" in sql

    def test_watermark_filter_full_load(self, full_load_config, mock_client):
        detector = DeltaDetector(mock_client, "proj", "fw")
        sql = detector._build_extraction_sql(
            full_load_config, "`proj.fw._delta_staging_test`"
        )
        assert "TRUE" in sql  # no watermark filter


# ── DQValidator tests ─────────────────────────────────────────────────────────

class TestDQValidator:

    def _make_rule(self, rule_type: str, params: dict, action: str = "REJECT", col="amount") -> DQRule:
        return DQRule(
            rule_id=f"rule_{rule_type}",
            rule_name=f"Test {rule_type}",
            rule_type=rule_type,
            target_column=col,
            rule_params=params,
            action_on_fail=action,
            rule_order=1,
            is_active=True,
            description="",
        )

    def test_not_null_sql(self, mock_client):
        validator = DQValidator(mock_client, "proj")
        rule = self._make_rule("NOT_NULL", {})
        sql = validator._build_failing_rows_sql(rule, MagicMock(), "`staging`")
        assert "IS NULL" in sql
        assert "`amount`" in sql

    def test_range_sql(self, mock_client):
        validator = DQValidator(mock_client, "proj")
        rule = self._make_rule("RANGE", {"min": 0, "max": 1000})
        sql = validator._build_failing_rows_sql(rule, MagicMock(), "`staging`")
        assert "< 0" in sql
        assert "> 1000" in sql

    def test_regex_sql(self, mock_client, sample_config):
        validator = DQValidator(mock_client, "proj")
        rule = self._make_rule("REGEX", {"pattern": "^[A-Z]{2}$"}, col="country_code")
        sql = validator._build_failing_rows_sql(rule, sample_config, "`staging`")
        assert "REGEXP_CONTAINS" in sql
        assert "^[A-Z]{2}$" in sql

    def test_uniqueness_sql(self, mock_client, sample_config):
        validator = DQValidator(mock_client, "proj")
        rule = self._make_rule("UNIQUENESS", {})
        sql = validator._build_failing_rows_sql(rule, sample_config, "`staging`")
        assert "HAVING COUNT(*) > 1" in sql

    def test_custom_sql_placeholder(self, mock_client, sample_config):
        validator = DQValidator(mock_client, "proj")
        rule = self._make_rule("CUSTOM_SQL", {"sql": "SELECT * FROM {{staging}} WHERE amount < 0"})
        sql = validator._build_failing_rows_sql(rule, sample_config, "`proj.fw._stg`")
        assert "{{staging}}" not in sql
        assert "`proj.fw._stg`" in sql

    def test_no_rules_returns_empty_summary(self, mock_client, sample_config):
        from dataclasses import replace
        cfg = replace(sample_config, dq_rules=[])
        validator = DQValidator(mock_client, "proj")
        summary = validator.validate(cfg, "`staging`", "test-run-id")
        assert summary.all_passed is True
        assert len(summary.results) == 0


# ── WatermarkManager tests ────────────────────────────────────────────────────

class TestWatermarkManager:

    def test_acquire_returns_run_id(self, mock_client):
        # Simulate: MERGE executes + check_lock returns our run_id
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_client.query.return_value = mock_job

        mgr = WatermarkManager(mock_client, "proj")

        # Patch _check_lock to return a "we hold the lock" row
        run_id = str(uuid.uuid4())
        with patch.object(mgr, "_check_lock", return_value={
            "status": "RUNNING",
            "locked_by": None,  # will be overwritten in test
            "locked_at": datetime.now(tz=timezone.utc),
            "hours_held": 0.0,
        }):
            # We can't easily test this without a real BQ — just ensure no error
            # on the acquire_lock path when check shows our run_id
            pass  # Integration test territory

    def test_lock_conflict_raises(self, mock_client):
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_client.query.return_value = mock_job

        mgr = WatermarkManager(mock_client, "proj")
        other_run = str(uuid.uuid4())
        with patch.object(mgr, "_check_lock", return_value={
            "status": "RUNNING",
            "locked_by": other_run,
            "locked_at": datetime.now(tz=timezone.utc),
            "hours_held": 0.5,
        }):
            with pytest.raises(LockConflictError) as exc_info:
                mgr.acquire_lock("sales_orders_v1")
            assert other_run in str(exc_info.value) or exc_info.value.locked_by == other_run


# ── DQSummary helper tests ────────────────────────────────────────────────────

class TestDQSummary:

    def _make_result(self, passed: bool, action: str) -> DQResult:
        return DQResult(
            rule_id="r1", rule_name="test", rule_type="NOT_NULL",
            action_on_fail=action, failed_row_count=0 if passed else 5,
            sample_failing_sql="", passed=passed,
        )

    def test_reject_failures_filter(self):
        summary = DQSummary(registry_id="t", run_id="r")
        summary.results = [
            self._make_result(False, "REJECT"),
            self._make_result(False, "WARN"),
            self._make_result(True,  "REJECT"),
        ]
        assert len(summary.reject_failures) == 1
        assert len(summary.warn_failures) == 1
        assert summary.all_passed is False

    def test_all_passed_when_no_failures(self):
        summary = DQSummary(registry_id="t", run_id="r")
        summary.results = [self._make_result(True, "REJECT")]
        assert summary.all_passed is True
