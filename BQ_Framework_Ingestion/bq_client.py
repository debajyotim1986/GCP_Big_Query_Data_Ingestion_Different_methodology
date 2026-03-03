"""
framework/utils/bq_client.py

Singleton BQ client wrapper with schema introspection utilities.
Provides column metadata, query execution, and job management.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, SchemaField

logger = logging.getLogger(__name__)


@dataclass
class TableMeta:
    """Introspected metadata for a BQ table."""
    project: str
    dataset: str
    table: str
    columns: List[str] = field(default_factory=list)
    schema: List[SchemaField] = field(default_factory=list)

    @property
    def fqn(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"


class BQClient:
    """
    Wrapper around google-cloud-bigquery client.
    Provides schema introspection + typed query helpers.
    """

    _instance: Optional["BQClient"] = None

    def __init__(self, project: str):
        self.project = project
        self._client = bigquery.Client(project=project)

    @classmethod
    def get_instance(cls, project: str) -> "BQClient":
        if cls._instance is None:
            cls._instance = cls(project)
        return cls._instance

    # ──────────────────────────────────────────────────────────
    # Schema introspection
    # ──────────────────────────────────────────────────────────

    def get_table_meta(self, project: str, dataset: str, table: str) -> TableMeta:
        """Fetch full schema for a table."""
        ref = self._client.get_table(f"{project}.{dataset}.{table}")
        columns = [f.name for f in ref.schema]
        return TableMeta(
            project=project,
            dataset=dataset,
            table=table,
            columns=columns,
            schema=list(ref.schema)
        )

    def get_columns_excluding(
        self,
        meta: TableMeta,
        exclude: List[str],
        also_exclude_internal: bool = True
    ) -> List[str]:
        """Return column list minus PK cols and internal framework cols."""
        internal = {"_row_hash", "_change_type"} if also_exclude_internal else set()
        return [
            c for c in meta.columns
            if c not in exclude and c not in internal
        ]

    # ──────────────────────────────────────────────────────────
    # Query execution
    # ──────────────────────────────────────────────────────────

    def run_query(
        self,
        sql: str,
        job_config: Optional[QueryJobConfig] = None,
        timeout: int = 600
    ) -> bigquery.QueryJob:
        """Execute a query and wait for completion."""
        job = self._client.query(sql, job_config=job_config)
        job.result(timeout=timeout)
        logger.info(
            "BQ job completed: job_id=%s, bytes_processed=%s",
            job.job_id,
            job.total_bytes_processed
        )
        return job

    def run_query_to_dataframe(self, sql: str) -> Any:
        """Execute a query and return result as pandas DataFrame."""
        return self._client.query(sql).to_dataframe()

    def run_scalar_query(self, sql: str) -> Any:
        """Execute a query returning a single scalar value."""
        rows = list(self._client.query(sql).result())
        if not rows:
            return None
        return rows[0][0]

    def run_rows_query(self, sql: str) -> List[Dict]:
        """Execute a query and return list of dicts."""
        rows = self._client.query(sql).result()
        return [dict(row) for row in rows]

    def table_exists(self, fqn: str) -> bool:
        try:
            self._client.get_table(fqn)
            return True
        except Exception:
            return False

    def drop_table_if_exists(self, fqn: str) -> None:
        self._client.delete_table(fqn, not_found_ok=True)
        logger.info("Dropped table if exists: %s", fqn)

    def insert_rows_json(self, fqn: str, rows: List[Dict]) -> None:
        """Streaming insert for small audit records."""
        errors = self._client.insert_rows_json(fqn, rows)
        if errors:
            raise RuntimeError(f"Streaming insert errors: {errors}")
