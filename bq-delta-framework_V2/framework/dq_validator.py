"""
framework/dq_validator.py
===========================
DQValidator runs every active DQ rule in ``rule_order`` sequence against
the staging table rows.

Rule types
----------
NOT_NULL        Check that ``target_column`` has no NULL values.
RANGE           Check that ``target_column`` values are within [min, max].
REGEX           Check that ``target_column`` matches the given pattern.
UNIQUENESS      Check that ``target_column`` (or composite) has no duplicates.
REFERENTIAL     Check that ``target_column`` values exist in a reference table.
CUSTOM_SQL      Run arbitrary SQL; rows returned are failures.

Actions on failure
------------------
REJECT          → raises DQBlockedError (pipeline halts, watermark unchanged)
QUARANTINE      → failed rows written to quarantine_records, removed from staging
WARN            → failure logged to dq_error_log, pipeline continues
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from .config_loader import DQRule, TableConfig
from .exceptions import DQBlockedError

logger = logging.getLogger(__name__)


@dataclass
class DQResult:
    rule_id: str
    rule_name: str
    rule_type: str
    action_on_fail: str
    failed_row_count: int
    sample_failing_sql: str
    passed: bool
    error_message: str = ""


@dataclass
class DQSummary:
    registry_id: str
    run_id: str
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    results: list[DQResult] = field(default_factory=list)

    @property
    def reject_failures(self) -> list[DQResult]:
        return [r for r in self.results if not r.passed and r.action_on_fail == "REJECT"]

    @property
    def quarantine_failures(self) -> list[DQResult]:
        return [r for r in self.results if not r.passed and r.action_on_fail == "QUARANTINE"]

    @property
    def warn_failures(self) -> list[DQResult]:
        return [r for r in self.results if not r.passed and r.action_on_fail == "WARN"]

    @property
    def all_passed(self) -> bool:
        return len(self.reject_failures) == 0


class DQValidator:
    """
    Validates staging table rows against all DQ rules for a table.

    Parameters
    ----------
    client : bigquery.Client
    project : str
    dataset : str
        Framework dataset (default: ``framework_config``).
    """

    def __init__(
        self,
        client: bigquery.Client,
        project: str,
        dataset: str = "framework_config",
    ) -> None:
        self._client = client
        self._project = project
        self._dataset = dataset

    # ── Public API ────────────────────────────────────────────────────────────

    def validate(
        self,
        config: TableConfig,
        staging_ref: str,
        run_id: str,
    ) -> DQSummary:
        """
        Run all DQ rules. Return a DQSummary.

        The caller should:
          - Check summary.reject_failures to decide whether to raise DQBlockedError
          - Handle quarantine rows via QuarantineManager
          - Write dq_error_log rows via AuditLogger
        """
        summary = DQSummary(registry_id=config.registry_id, run_id=run_id)

        if not config.dq_rules:
            logger.info("No DQ rules for %s — skipping validation", config.registry_id)
            return summary

        logger.info(
            "Running %d DQ rules for %s on staging=%s",
            len(config.dq_rules), config.registry_id, staging_ref,
        )

        for rule in config.dq_rules:
            result = self._evaluate_rule(rule, config, staging_ref)
            summary.results.append(result)

            if not result.passed:
                logger.warning(
                    "DQ rule FAILED: rule_id=%s action=%s failed_rows=%d",
                    rule.rule_id, rule.action_on_fail, result.failed_row_count,
                )

        return summary

    def get_failing_rows(
        self,
        rule: DQRule,
        config: TableConfig,
        staging_ref: str,
        limit: int = 1000,
    ) -> list[dict]:
        """Return up to *limit* failing rows for QUARANTINE handling."""
        failing_sql = self._build_failing_rows_sql(rule, config, staging_ref)
        limited_sql = f"""
        SELECT * FROM ({failing_sql}) AS failures
        LIMIT {limit}
        """
        try:
            rows = list(self._client.query(limited_sql).result())
            return [dict(row) for row in rows]
        except Exception as exc:
            logger.warning(
                "Failed to fetch failing rows for rule %s: %s", rule.rule_id, exc
            )
            return []

    # ── Rule evaluation ───────────────────────────────────────────────────────

    def _evaluate_rule(
        self,
        rule: DQRule,
        config: TableConfig,
        staging_ref: str,
    ) -> DQResult:
        count_sql = self._build_count_sql(rule, config, staging_ref)
        sample_sql = self._build_failing_rows_sql(rule, config, staging_ref)

        try:
            result = list(self._client.query(count_sql).result())
            failed_count = result[0]["failed_count"] if result else 0
        except Exception as exc:
            error_msg = str(exc)
            logger.exception("Error running DQ rule %s: %s", rule.rule_id, error_msg)
            return DQResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                rule_type=rule.rule_type,
                action_on_fail=rule.action_on_fail,
                failed_row_count=0,
                sample_failing_sql=count_sql,
                passed=False,
                error_message=f"DQ rule execution error: {error_msg}",
            )

        passed = failed_count == 0
        return DQResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            rule_type=rule.rule_type,
            action_on_fail=rule.action_on_fail,
            failed_row_count=failed_count,
            sample_failing_sql=sample_sql,
            passed=passed,
        )

    # ── SQL builders per rule type ────────────────────────────────────────────

    def _build_count_sql(
        self,
        rule: DQRule,
        config: TableConfig,
        staging_ref: str,
    ) -> str:
        failing_sql = self._build_failing_rows_sql(rule, config, staging_ref)
        return f"SELECT COUNT(*) AS failed_count FROM ({failing_sql}) AS __failures"

    def _build_failing_rows_sql(
        self,
        rule: DQRule,
        config: TableConfig,
        staging_ref: str,
    ) -> str:
        col = rule.target_column
        params = rule.rule_params

        if rule.rule_type == "NOT_NULL":
            return f"SELECT * FROM {staging_ref} WHERE `{col}` IS NULL"

        elif rule.rule_type == "RANGE":
            min_val = params.get("min")
            max_val = params.get("max")
            conditions = []
            if min_val is not None:
                conditions.append(f"`{col}` < {min_val}")
            if max_val is not None:
                conditions.append(f"`{col}` > {max_val}")
            where = " OR ".join(conditions) if conditions else "FALSE"
            return f"SELECT * FROM {staging_ref} WHERE {where}"

        elif rule.rule_type == "REGEX":
            pattern = params.get("pattern", "")
            return (
                f"SELECT * FROM {staging_ref} "
                f"WHERE NOT REGEXP_CONTAINS(CAST(`{col}` AS STRING), r'{pattern}')"
            )

        elif rule.rule_type == "UNIQUENESS":
            cols = [col] if col else config.primary_key_cols
            col_list = ", ".join(f"`{c}`" for c in cols)
            return f"""
            SELECT * FROM {staging_ref}
            WHERE ({col_list}) IN (
                SELECT {col_list}
                FROM {staging_ref}
                GROUP BY {col_list}
                HAVING COUNT(*) > 1
            )
            """

        elif rule.rule_type == "REFERENTIAL":
            ref_table = params.get("ref_table", "")
            ref_col = params.get("ref_col", col)
            return f"""
            SELECT src.* FROM {staging_ref} src
            WHERE src.`{col}` IS NOT NULL
              AND src.`{col}` NOT IN (
                  SELECT `{ref_col}` FROM `{ref_table}` WHERE `{ref_col}` IS NOT NULL
              )
            """

        elif rule.rule_type == "CUSTOM_SQL":
            custom = params.get("sql", "SELECT * FROM staging WHERE FALSE")
            # Replace {{staging}} placeholder with actual staging ref
            resolved = custom.replace("{{staging}}", staging_ref)
            return resolved

        else:
            logger.warning("Unknown rule_type '%s' — treating as always-pass", rule.rule_type)
            return f"SELECT * FROM {staging_ref} WHERE FALSE"
