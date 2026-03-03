"""
framework/ingestion/ingestion_engine.py

Generates and executes a fully dynamic MERGE statement.
Introspects target table schema from BQ metadata — works for any table.
"""

from __future__ import annotations

import logging
from typing import List

from framework.config.config_loader import TableConfig
from framework.utils.bq_client import BQClient, TableMeta

logger = logging.getLogger(__name__)


class IngestionEngine:
    """
    Generates a schema-agnostic MERGE from staging → target.
    Dynamically builds SET, INSERT column lists from BQ table metadata.
    """

    def __init__(self, bq: BQClient):
        self.bq = bq

    def merge_to_target(self, config: TableConfig) -> int:
        """
        Execute MERGE from staging to target.
        Returns number of rows ingested (affected rows).
        """
        target_meta = self.bq.get_table_meta(
            config.target_project, config.target_dataset, config.target_table
        )

        merge_sql = self._build_merge_sql(config, target_meta)
        logger.info("Executing MERGE: %s → %s", config.staging_fqn, config.target_fqn)
        logger.debug("MERGE SQL:\n%s", merge_sql)

        job = self.bq.run_query(merge_sql)

        # BQ DML stats
        stats = job.dml_stats
        ingested = 0
        if stats:
            ingested = (stats.inserted_row_count or 0) + (stats.updated_row_count or 0)
            logger.info(
                "MERGE complete: inserted=%d, updated=%d, deleted=%d",
                stats.inserted_row_count or 0,
                stats.updated_row_count or 0,
                stats.deleted_row_count or 0
            )

        return ingested

    # ──────────────────────────────────────────────────────────
    # SQL builders
    # ──────────────────────────────────────────────────────────

    def _build_merge_sql(self, config: TableConfig, target_meta: TableMeta) -> str:
        pk_cols = config.primary_key_cols
        framework_cols = {"_row_hash", "_change_type"}

        # All non-framework columns
        data_cols = [c for c in target_meta.columns if c not in framework_cols]
        # Non-PK data columns (for UPDATE SET)
        update_cols = [c for c in data_cols if c not in pk_cols]

        pk_join_expr = " AND ".join([f"T.`{c}` = S.`{c}`" for c in pk_cols])
        update_set_expr = ",\n      ".join([f"T.`{c}` = S.`{c}`" for c in update_cols])
        insert_col_list = ", ".join([f"`{c}`" for c in data_cols])
        insert_val_list = ", ".join([f"S.`{c}`" for c in data_cols])

        # Build SELECT excluding framework internal cols
        exclude_cols = ", ".join([f"`{c}`" for c in framework_cols])

        return f"""
MERGE `{config.target_fqn}` T
USING (
  SELECT * EXCEPT({exclude_cols})
  FROM `{config.staging_fqn}`
) S
ON {pk_join_expr}

WHEN MATCHED AND S._change_type = 'UPDATE' THEN
  UPDATE SET
    {update_set_expr}

WHEN NOT MATCHED BY TARGET AND S._change_type = 'INSERT' THEN
  INSERT ({insert_col_list})
  VALUES ({insert_val_list});
"""
