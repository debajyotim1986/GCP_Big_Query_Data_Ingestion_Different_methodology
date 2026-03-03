"""
framework/dq/dq_validator.py

Executes all DQ rules for a table config against the staging table.
Handles REJECT, WARN, and QUARANTINE actions.
Produces DQ error log entries and removes failing rows from staging.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

from framework.config.config_loader import DQRule, TableConfig
from framework.utils.bq_client import BQClient

logger = logging.getLogger(__name__)

FRAMEWORK_PROJECT = "your-gcp-project"
FRAMEWORK_DATASET = "framework_config"
DQ_ERROR_LOG_FQN = f"{FRAMEWORK_PROJECT}.{FRAMEWORK_DATASET}.dq_error_log"
QUARANTINE_FQN = f"{FRAMEWORK_PROJECT}.{FRAMEWORK_DATASET}.quarantine_records"


@dataclass
class DQRuleResult:
    rule: DQRule
    failed_row_count: int
    passed: bool
    sample_rows: List[dict] = field(default_factory=list)


@dataclass
class DQSummary:
    total_rules: int
    passed_rules: int
    failed_reject_count: int     # rows that cause pipeline block
    failed_warn_count: int       # rows logged but allowed through
    quarantined_count: int       # rows removed to quarantine
    rule_results: List[DQRuleResult] = field(default_factory=list)
    blocked: bool = False        # True if any REJECT rule failed


class DQValidator:
    """
    Runs all DQ rules against the staging table.
    
    Rule Actions:
    - REJECT:      Pipeline is blocked if any rows fail. Fails entire run.
    - WARN:        Failure logged, pipeline continues with all records.
    - QUARANTINE:  Failing rows removed from staging → quarantine table.
    """

    def __init__(self, bq: BQClient):
        self.bq = bq

    def validate(self, config: TableConfig, run_id: str) -> DQSummary:
        """
        Run all DQ rules. Returns DQSummary.
        Modifies staging table in-place for QUARANTINE rules.
        """
        results = []

        # Run rules in priority order: REJECT first, then QUARANTINE, then WARN
        ordered_rules = (
            config.reject_rules +
            config.quarantine_rules +
            config.warn_rules
        )

        for rule in ordered_rules:
            logger.info("Running DQ rule: %s [%s / %s]", rule.rule_name, rule.rule_type, rule.action_on_fail)
            result = self._run_rule(rule, config)
            results.append(result)

            if not result.passed:
                logger.warning(
                    "DQ rule FAILED: %s — %d rows affected (action: %s)",
                    rule.rule_name, result.failed_row_count, rule.action_on_fail
                )
                self._handle_failure(rule, result, config, run_id)

        # Build summary
        summary = self._build_summary(results)
        self._write_dq_error_log(results, config, run_id)

        logger.info(
            "DQ complete: rules=%d, passed=%d, reject_fails=%d, quarantined=%d, warn_fails=%d",
            summary.total_rules,
            summary.passed_rules,
            summary.failed_reject_count,
            summary.quarantined_count,
            summary.failed_warn_count
        )
        return summary

    # ──────────────────────────────────────────────────────────
    # Rule execution
    # ──────────────────────────────────────────────────────────

    def _run_rule(self, rule: DQRule, config: TableConfig) -> DQRuleResult:
        """Dispatch to rule-type-specific SQL generator."""
        sql = self._build_rule_sql(rule, config)
        failed_count = self.bq.run_scalar_query(sql) or 0

        sample_rows = []
        if failed_count > 0:
            sample_rows = self._get_sample_rows(rule, config)

        return DQRuleResult(
            rule=rule,
            failed_row_count=failed_count,
            passed=(failed_count == 0),
            sample_rows=sample_rows
        )

    def _build_rule_sql(self, rule: DQRule, config: TableConfig) -> str:
        """Generate COUNT query for a given rule type."""
        fqn = config.staging_fqn
        col = rule.target_column
        p = rule.rule_params

        if rule.rule_type == "NOT_NULL":
            return f"SELECT COUNT(*) FROM `{fqn}` WHERE `{col}` IS NULL"

        elif rule.rule_type == "RANGE":
            conditions = []
            if "min" in p:
                op = ">=" if p.get("inclusive", True) else ">"
                conditions.append(f"`{col}` < {p['min']}")  # fails if below min
            if "max" in p:
                conditions.append(f"`{col}` > {p['max']}")  # fails if above max
            where = " OR ".join(conditions)
            return f"SELECT COUNT(*) FROM `{fqn}` WHERE `{col}` IS NOT NULL AND ({where})"

        elif rule.rule_type == "REGEX":
            pattern = p["pattern"].replace("'", "\\'")
            return (
                f"SELECT COUNT(*) FROM `{fqn}` "
                f"WHERE `{col}` IS NOT NULL "
                f"AND NOT REGEXP_CONTAINS(CAST(`{col}` AS STRING), r'{pattern}')"
            )

        elif rule.rule_type == "UNIQUENESS":
            scope_cols = ", ".join([f"`{c}`" for c in p["scope_cols"]])
            return f"""
                SELECT COUNT(*) FROM (
                  SELECT {scope_cols}, COUNT(*) AS cnt
                  FROM `{fqn}`
                  GROUP BY {scope_cols}
                  HAVING cnt > 1
                )
            """

        elif rule.rule_type == "REFERENTIAL":
            ref_fqn = f"{p['ref_project']}.{p['ref_dataset']}.{p['ref_table']}"
            return f"""
                SELECT COUNT(*) FROM `{fqn}` S
                WHERE S.`{col}` IS NOT NULL
                AND NOT EXISTS (
                  SELECT 1 FROM `{ref_fqn}` R WHERE R.`{p['ref_column']}` = S.`{col}`
                )
            """

        elif rule.rule_type == "CUSTOM_SQL":
            custom_sql = p["sql"].replace("{table}", f"`{fqn}`")
            return custom_sql

        else:
            raise ValueError(f"Unknown rule type: {rule.rule_type}")

    def _get_sample_rows(self, rule: DQRule, config: TableConfig) -> List[dict]:
        """Fetch up to 10 sample rows that failed this rule for logging."""
        fqn = config.staging_fqn
        col = rule.target_column

        # Build failure condition per rule type
        condition = self._build_failure_condition(rule, config)
        if not condition:
            return []

        sql = f"""
            SELECT TO_JSON_STRING(t) AS row_json
            FROM `{fqn}` t
            WHERE {condition}
            LIMIT 10
        """
        rows = self.bq.run_rows_query(sql)
        return [json.loads(r["row_json"]) for r in rows]

    def _build_failure_condition(self, rule: DQRule, config: TableConfig) -> Optional[str]:
        """Returns a WHERE condition string for rows failing this rule."""
        col = rule.target_column
        p = rule.rule_params

        if rule.rule_type == "NOT_NULL":
            return f"`{col}` IS NULL"
        elif rule.rule_type == "RANGE":
            conditions = []
            if "min" in p:
                conditions.append(f"`{col}` < {p['min']}")
            if "max" in p:
                conditions.append(f"`{col}` > {p['max']}")
            return " OR ".join(conditions) if conditions else None
        elif rule.rule_type == "REGEX":
            pattern = p["pattern"].replace("'", "\\'")
            return f"NOT REGEXP_CONTAINS(CAST(`{col}` AS STRING), r'{pattern}')"
        else:
            return None  # Complex rules — skip sample row capture

    # ──────────────────────────────────────────────────────────
    # Failure handlers
    # ──────────────────────────────────────────────────────────

    def _handle_failure(
        self,
        rule: DQRule,
        result: DQRuleResult,
        config: TableConfig,
        run_id: str
    ) -> None:
        """Handle rule failure based on action_on_fail."""
        if rule.action_on_fail == "QUARANTINE":
            self._quarantine_failed_rows(rule, result, config, run_id)
            self._remove_failed_rows_from_staging(rule, config)

        # REJECT and WARN — no row-level operation on staging at this point
        # REJECT will cause DQSummary.blocked = True → pipeline aborted

    def _quarantine_failed_rows(
        self,
        rule: DQRule,
        result: DQRuleResult,
        config: TableConfig,
        run_id: str
    ) -> None:
        """Copy failing rows to quarantine table as JSON."""
        condition = self._build_failure_condition(rule, config)
        if not condition:
            return

        quarantine_id = str(uuid.uuid4())
        sql = f"""
            INSERT INTO `{QUARANTINE_FQN}` (quarantine_id, run_id, registry_id, rule_id, raw_record, failure_reason, created_at)
            SELECT
              CONCAT('{quarantine_id}', '_', CAST(ROW_NUMBER() OVER() AS STRING)) AS quarantine_id,
              '{run_id}' AS run_id,
              '{config.registry_id}' AS registry_id,
              '{rule.rule_id}' AS rule_id,
              TO_JSON_STRING(t) AS raw_record,
              '{rule.rule_name}: {rule.rule_type} check failed' AS failure_reason,
              CURRENT_TIMESTAMP() AS created_at
            FROM `{config.staging_fqn}` t
            WHERE {condition}
        """
        self.bq.run_query(sql)

    def _remove_failed_rows_from_staging(self, rule: DQRule, config: TableConfig) -> None:
        """Delete quarantined rows from staging so they don't get ingested."""
        condition = self._build_failure_condition(rule, config)
        if not condition:
            return

        sql = f"DELETE FROM `{config.staging_fqn}` WHERE {condition}"
        self.bq.run_query(sql)
        logger.info("Removed quarantined rows from staging for rule: %s", rule.rule_name)

    # ──────────────────────────────────────────────────────────
    # Summary & logging
    # ──────────────────────────────────────────────────────────

    def _build_summary(self, results: List[DQRuleResult]) -> DQSummary:
        reject_fails = sum(1 for r in results if not r.passed and r.rule.action_on_fail == "REJECT")
        warn_fails = sum(r.failed_row_count for r in results if not r.passed and r.rule.action_on_fail == "WARN")
        quarantined = sum(r.failed_row_count for r in results if not r.passed and r.rule.action_on_fail == "QUARANTINE")

        return DQSummary(
            total_rules=len(results),
            passed_rules=sum(1 for r in results if r.passed),
            failed_reject_count=reject_fails,
            failed_warn_count=warn_fails,
            quarantined_count=quarantined,
            rule_results=results,
            blocked=(reject_fails > 0)
        )

    def _write_dq_error_log(
        self,
        results: List[DQRuleResult],
        config: TableConfig,
        run_id: str
    ) -> None:
        """Insert failed rule results into dq_error_log."""
        failed = [r for r in results if not r.passed]
        if not failed:
            return

        rows = []
        for r in failed:
            rows.append({
                "error_id": str(uuid.uuid4()),
                "run_id": run_id,
                "registry_id": config.registry_id,
                "rule_id": r.rule.rule_id,
                "rule_name": r.rule.rule_name,
                "rule_type": r.rule.rule_type,
                "target_column": r.rule.target_column,
                "action_on_fail": r.rule.action_on_fail,
                "failed_row_count": r.failed_row_count,
                "sample_failed_rows": json.dumps(r.sample_rows[:10]),
                "created_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
            })

        self.bq.insert_rows_json(DQ_ERROR_LOG_FQN, rows)
