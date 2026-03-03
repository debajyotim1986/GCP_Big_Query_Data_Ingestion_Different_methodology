"""
framework/config/config_loader.py

Loads table registry config and DQ rules from BQ framework config tables.
Supports per-table config with full DQ rule resolution.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from framework.utils.bq_client import BQClient

logger = logging.getLogger(__name__)

FRAMEWORK_PROJECT = "your-gcp-project"
FRAMEWORK_DATASET = "framework_config"


# ──────────────────────────────────────────────────────────────────
# Data classes — typed config objects
# ──────────────────────────────────────────────────────────────────

@dataclass
class DQRule:
    rule_id: str
    rule_name: str
    rule_type: str                   # NOT_NULL | RANGE | REGEX | UNIQUENESS | REFERENTIAL | CUSTOM_SQL
    target_column: Optional[str]
    rule_params: Dict[str, Any]
    action_on_fail: str              # REJECT | WARN | QUARANTINE
    is_active: bool = True


@dataclass
class TableConfig:
    registry_id: str
    source_project: str
    source_dataset: str
    source_table: str
    target_project: str
    target_dataset: str
    target_table: str
    primary_key_cols: List[str]
    delta_hash_cols: List[str]       # empty list = use all columns
    watermark_col: str
    is_active: bool = True
    batch_size: Optional[int] = None
    dq_rules: List[DQRule] = field(default_factory=list)

    @property
    def source_fqn(self) -> str:
        return f"{self.source_project}.{self.source_dataset}.{self.source_table}"

    @property
    def target_fqn(self) -> str:
        return f"{self.target_project}.{self.target_dataset}.{self.target_table}"

    @property
    def staging_table_name(self) -> str:
        return f"_delta_staging_{self.registry_id}"

    @property
    def staging_fqn(self) -> str:
        return f"{FRAMEWORK_PROJECT}.{FRAMEWORK_DATASET}.{self.staging_table_name}"

    @property
    def use_all_columns_for_hash(self) -> bool:
        return not self.delta_hash_cols

    @property
    def reject_rules(self) -> List[DQRule]:
        return [r for r in self.dq_rules if r.action_on_fail == "REJECT" and r.is_active]

    @property
    def warn_rules(self) -> List[DQRule]:
        return [r for r in self.dq_rules if r.action_on_fail == "WARN" and r.is_active]

    @property
    def quarantine_rules(self) -> List[DQRule]:
        return [r for r in self.dq_rules if r.action_on_fail == "QUARANTINE" and r.is_active]


# ──────────────────────────────────────────────────────────────────
# Config loader
# ──────────────────────────────────────────────────────────────────

class ConfigLoader:
    """
    Loads table configurations and DQ rules from BQ framework tables.
    Called at DAG task start — config is always fresh from BQ.
    """

    def __init__(self, bq: BQClient):
        self.bq = bq
        self._registry_fqn = f"{FRAMEWORK_PROJECT}.{FRAMEWORK_DATASET}.table_registry"
        self._rules_fqn = f"{FRAMEWORK_PROJECT}.{FRAMEWORK_DATASET}.dq_rules"

    def load_config(self, registry_id: str) -> TableConfig:
        """Load full config for a single registry_id."""
        registry_row = self._load_registry_row(registry_id)
        dq_rules = self._load_dq_rules(registry_id)
        return self._build_config(registry_row, dq_rules)

    def load_all_active_configs(self) -> List[TableConfig]:
        """Load all active table configs — used by DAG to discover tables."""
        sql = f"""
            SELECT registry_id
            FROM `{self._registry_fqn}`
            WHERE is_active = TRUE
            ORDER BY registry_id
        """
        rows = self.bq.run_rows_query(sql)
        return [self.load_config(row["registry_id"]) for row in rows]

    # ─────────────────────────────────
    # Private helpers
    # ─────────────────────────────────

    def _load_registry_row(self, registry_id: str) -> Dict:
        sql = f"""
            SELECT *
            FROM `{self._registry_fqn}`
            WHERE registry_id = '{registry_id}'
            AND is_active = TRUE
            LIMIT 1
        """
        rows = self.bq.run_rows_query(sql)
        if not rows:
            raise ValueError(f"No active registry entry found for: {registry_id}")
        return rows[0]

    def _load_dq_rules(self, registry_id: str) -> List[DQRule]:
        sql = f"""
            SELECT *
            FROM `{self._rules_fqn}`
            WHERE registry_id = '{registry_id}'
            AND is_active = TRUE
            ORDER BY rule_type, rule_id
        """
        rows = self.bq.run_rows_query(sql)
        return [self._parse_dq_rule(r) for r in rows]

    def _parse_dq_rule(self, row: Dict) -> DQRule:
        params = row.get("rule_params")
        if isinstance(params, str):
            params = json.loads(params)
        return DQRule(
            rule_id=row["rule_id"],
            rule_name=row["rule_name"],
            rule_type=row["rule_type"],
            target_column=row.get("target_column"),
            rule_params=params or {},
            action_on_fail=row["action_on_fail"],
            is_active=row.get("is_active", True)
        )

    def _build_config(self, row: Dict, dq_rules: List[DQRule]) -> TableConfig:
        return TableConfig(
            registry_id=row["registry_id"],
            source_project=row["source_project"],
            source_dataset=row["source_dataset"],
            source_table=row["source_table"],
            target_project=row["target_project"],
            target_dataset=row["target_dataset"],
            target_table=row["target_table"],
            primary_key_cols=list(row["primary_key_cols"]),
            delta_hash_cols=list(row["delta_hash_cols"] or []),
            watermark_col=row["watermark_col"],
            is_active=row.get("is_active", True),
            batch_size=row.get("batch_size"),
            dq_rules=dq_rules
        )
