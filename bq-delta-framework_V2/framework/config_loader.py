"""
framework/config_loader.py
===========================
ConfigLoader reads a single table's configuration from BigQuery
``framework_config.table_registry`` and the active DQ rules from
``framework_config.dq_rules``.

It also resolves the watermark window (watermark_from / watermark_to)
by reading ``process_control``, determining whether this is a FULL LOAD
(first ever run, watermark = NULL) or INCREMENTAL run.

Usage
-----
    loader = ConfigLoader(bq_client, project="my-proj", dataset="framework_config")
    config = loader.load("sales_orders_v1")
    print(config.watermark_from)   # None → full load
    print(config.dq_rules)         # list of DQRule dataclasses
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from .exceptions import ConfigError, TableNotRegisteredError

logger = logging.getLogger(__name__)


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class DQRule:
    rule_id: str
    rule_name: str
    rule_type: str                  # NOT_NULL | RANGE | REGEX | UNIQUENESS | REFERENTIAL | CUSTOM_SQL
    target_column: str | None
    rule_params: dict               # {"min": 0, "max": 1000000} | {"pattern": "^..."} | {"sql": "..."}
    action_on_fail: str             # REJECT | QUARANTINE | WARN
    rule_order: int
    is_active: bool
    description: str


@dataclass
class TableConfig:
    # Identity
    registry_id: str
    owner_team: str
    tags: list[str]

    # Source coordinates
    source_project: str
    source_dataset: str
    source_table: str

    # Target coordinates
    target_project: str
    target_dataset: str
    target_table: str

    # Delta detection
    primary_key_cols: list[str]
    delta_hash_cols: list[str]      # empty = hash ALL columns
    watermark_col: str

    # Runtime options
    batch_size: int | None          # None = no row limit
    is_active: bool
    priority: int

    # Watermark window (resolved from process_control)
    watermark_from: datetime | None  # None → FULL LOAD
    watermark_to: datetime           # always set to NOW at load time

    # DQ rules (ordered by rule_order)
    dq_rules: list[DQRule] = field(default_factory=list)

    @property
    def source_table_ref(self) -> str:
        return f"`{self.source_project}.{self.source_dataset}.{self.source_table}`"

    @property
    def target_table_ref(self) -> str:
        return f"`{self.target_project}.{self.target_dataset}.{self.target_table}`"

    @property
    def is_full_load(self) -> bool:
        return self.watermark_from is None

    @property
    def load_mode(self) -> str:
        return "FULL_LOAD" if self.is_full_load else "INCREMENTAL"

    @property
    def effective_hash_cols(self) -> list[str]:
        """If delta_hash_cols is empty, hash all PKs (caller should expand to full list)."""
        return self.delta_hash_cols if self.delta_hash_cols else self.primary_key_cols


# ── ConfigLoader ─────────────────────────────────────────────────────────────

class ConfigLoader:
    """
    Loads table configuration and DQ rules from BigQuery framework tables.

    Parameters
    ----------
    client : google.cloud.bigquery.Client
    project : str
        GCP project ID containing the framework dataset.
    dataset : str
        BigQuery dataset name (default: ``framework_config``).
    """

    REGISTRY_QUERY = """
        SELECT
            registry_id,
            source_project, source_dataset, source_table,
            target_project, target_dataset, target_table,
            primary_key_cols, delta_hash_cols,
            watermark_col, batch_size,
            is_active, priority,
            COALESCE(owner_team, '')  AS owner_team,
            COALESCE(tags, [])        AS tags
        FROM `{project}.{dataset}.table_registry`
        WHERE registry_id = @registry_id
          AND is_active = TRUE
        LIMIT 1
    """

    WATERMARK_QUERY = """
        SELECT last_successful_run
        FROM `{project}.{dataset}.process_control`
        WHERE registry_id = @registry_id
        LIMIT 1
    """

    DQ_RULES_QUERY = """
        SELECT
            rule_id, rule_name, rule_type,
            target_column,
            TO_JSON_STRING(rule_params) AS rule_params_json,
            action_on_fail,
            rule_order,
            is_active,
            COALESCE(description, '') AS description
        FROM `{project}.{dataset}.dq_rules`
        WHERE registry_id = @registry_id
          AND is_active = TRUE
        ORDER BY rule_order ASC
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

    def load(self, registry_id: str) -> TableConfig:
        """
        Load the full configuration for *registry_id*.

        Returns
        -------
        TableConfig
            Fully populated config including resolved watermark window and DQ rules.

        Raises
        ------
        TableNotRegisteredError
            If the registry_id is not found or is_active = FALSE.
        ConfigError
            If required fields are missing or malformed.
        """
        logger.info("Loading config for registry_id=%s", registry_id)

        row = self._fetch_registry(registry_id)
        watermark_from = self._fetch_watermark(registry_id)
        watermark_to = datetime.now(tz=timezone.utc)
        dq_rules = self._fetch_dq_rules(registry_id)

        config = TableConfig(
            registry_id=registry_id,
            owner_team=row["owner_team"],
            tags=list(row["tags"]),
            source_project=row["source_project"],
            source_dataset=row["source_dataset"],
            source_table=row["source_table"],
            target_project=row["target_project"],
            target_dataset=row["target_dataset"],
            target_table=row["target_table"],
            primary_key_cols=list(row["primary_key_cols"]),
            delta_hash_cols=list(row["delta_hash_cols"]) if row["delta_hash_cols"] else [],
            watermark_col=row["watermark_col"],
            batch_size=row["batch_size"],
            is_active=row["is_active"],
            priority=row["priority"],
            watermark_from=watermark_from,
            watermark_to=watermark_to,
            dq_rules=dq_rules,
        )

        logger.info(
            "Config loaded: registry_id=%s mode=%s watermark_from=%s dq_rules=%d",
            registry_id, config.load_mode, watermark_from, len(dq_rules),
        )
        return config

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_registry(self, registry_id: str) -> dict[str, Any]:
        sql = self.REGISTRY_QUERY.format(
            project=self._project, dataset=self._dataset
        )
        params = [bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id)]
        job_config = bigquery.QueryJobConfig(query_parameters=params)

        rows = list(self._client.query(sql, job_config=job_config).result())
        if not rows:
            raise TableNotRegisteredError(
                f"registry_id '{registry_id}' not found in table_registry or is_active=FALSE",
                registry_id=registry_id,
            )

        row = dict(rows[0])
        self._validate_registry_row(row, registry_id)
        return row

    def _validate_registry_row(self, row: dict, registry_id: str) -> None:
        required = [
            "source_project", "source_dataset", "source_table",
            "target_project", "target_dataset", "target_table",
            "watermark_col", "primary_key_cols",
        ]
        missing = [f for f in required if not row.get(f)]
        if missing:
            raise ConfigError(
                f"registry_id '{registry_id}' is missing required fields: {missing}",
                registry_id=registry_id,
            )

    def _fetch_watermark(self, registry_id: str) -> datetime | None:
        """Returns last_successful_run or None (→ FULL LOAD) if no row exists yet."""
        sql = self.WATERMARK_QUERY.format(
            project=self._project, dataset=self._dataset
        )
        params = [bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id)]
        job_config = bigquery.QueryJobConfig(query_parameters=params)

        rows = list(self._client.query(sql, job_config=job_config).result())
        if not rows:
            logger.info("No process_control row for %s → FULL LOAD", registry_id)
            return None

        watermark = rows[0]["last_successful_run"]
        if watermark is None:
            logger.info("process_control.last_successful_run is NULL for %s → FULL LOAD", registry_id)
        return watermark

    def _fetch_dq_rules(self, registry_id: str) -> list[DQRule]:
        import json

        sql = self.DQ_RULES_QUERY.format(
            project=self._project, dataset=self._dataset
        )
        params = [bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id)]
        job_config = bigquery.QueryJobConfig(query_parameters=params)

        rules: list[DQRule] = []
        for row in self._client.query(sql, job_config=job_config).result():
            try:
                params_dict = json.loads(row["rule_params_json"]) if row["rule_params_json"] else {}
            except (json.JSONDecodeError, TypeError):
                params_dict = {}

            rules.append(DQRule(
                rule_id=row["rule_id"],
                rule_name=row["rule_name"],
                rule_type=row["rule_type"],
                target_column=row["target_column"],
                rule_params=params_dict,
                action_on_fail=row["action_on_fail"],
                rule_order=row["rule_order"],
                is_active=row["is_active"],
                description=row["description"],
            ))

        logger.debug("Loaded %d DQ rules for %s", len(rules), registry_id)
        return rules
