"""
framework/delta/delta_detector.py

Generates dynamic delta detection SQL based on table config.
Supports:
  - Specific column hash (delta_hash_cols list)
  - Full row hash (delta_hash_cols = [] / '*')
  - Watermark-based extraction window
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Tuple

from framework.config.config_loader import TableConfig
from framework.utils.bq_client import BQClient, TableMeta

logger = logging.getLogger(__name__)


class DeltaDetector:
    """
    Builds and executes delta detection query.
    Creates a staging table with only changed/new records.
    """

    def __init__(self, bq: BQClient):
        self.bq = bq

    def detect_and_stage(
        self,
        config: TableConfig,
        watermark_from: Optional[datetime],
        watermark_to: datetime
    ) -> Tuple[int, int]:
        """
        Runs delta detection and creates staging table.

        Returns:
            (source_row_count, delta_row_count)
        """
        source_meta = self.bq.get_table_meta(
            config.source_project, config.source_dataset, config.source_table
        )

        # Build SQL components dynamically
        hash_expr = self._build_hash_expr(config, source_meta)
        pk_join_expr = self._build_pk_join_expr(config.primary_key_cols)
        watermark_filter = self._build_watermark_filter(
            config.watermark_col, watermark_from, watermark_to
        )

        staging_sql = self._build_staging_sql(
            config=config,
            source_meta=source_meta,
            hash_expr=hash_expr,
            pk_join_expr=pk_join_expr,
            watermark_filter=watermark_filter
        )

        logger.info("Running delta detection for %s → %s", config.source_fqn, config.staging_fqn)
        logger.debug("Delta SQL:\n%s", staging_sql)

        self.bq.run_query(staging_sql)

        # Get counts
        source_count = self._get_source_count(config, watermark_filter)
        delta_count = self._get_staging_count(config)

        logger.info(
            "Delta detection complete: source=%d, delta=%d (%.1f%%)",
            source_count, delta_count,
            (delta_count / source_count * 100) if source_count else 0
        )
        return source_count, delta_count

    # ──────────────────────────────────────────────────────────
    # SQL builders
    # ──────────────────────────────────────────────────────────

    def _build_hash_expr(self, config: TableConfig, meta: TableMeta) -> str:
        """
        Generates MD5 hash expression.
        If delta_hash_cols is empty → hash full row via TO_JSON_STRING.
        If specific cols listed → hash concatenated column values.
        """
        if config.use_all_columns_for_hash:
            # Full row hash — works for any schema
            return "TO_HEX(MD5(TO_JSON_STRING(src)))"

        # Specific column hash
        cols = config.delta_hash_cols
        concat_parts = " || '||' || ".join(
            [f"IFNULL(CAST(src.`{c}` AS STRING), '__NULL__')" for c in cols]
        )
        return f"TO_HEX(MD5({concat_parts}))"

    def _build_pk_join_expr(self, pk_cols: list) -> str:
        """Build: T.col1 = S.col1 AND T.col2 = S.col2"""
        return " AND ".join([f"T.`{c}` = S.`{c}`" for c in pk_cols])

    def _build_watermark_filter(
        self,
        watermark_col: str,
        watermark_from: Optional[datetime],
        watermark_to: datetime
    ) -> str:
        """Build watermark WHERE clause for incremental extraction."""
        wm_to_str = watermark_to.strftime("%Y-%m-%dT%H:%M:%S")

        if watermark_from is None:
            # First run — full load (no lower bound)
            return f"src.`{watermark_col}` <= TIMESTAMP('{wm_to_str}')"

        wm_from_str = watermark_from.strftime("%Y-%m-%dT%H:%M:%S")
        return (
            f"src.`{watermark_col}` > TIMESTAMP('{wm_from_str}') "
            f"AND src.`{watermark_col}` <= TIMESTAMP('{wm_to_str}')"
        )

    def _build_staging_sql(
        self,
        config: TableConfig,
        source_meta: TableMeta,
        hash_expr: str,
        pk_join_expr: str,
        watermark_filter: str
    ) -> str:
        """Assembles the full CREATE OR REPLACE TABLE ... AS SELECT delta detection SQL."""
        pk_cols = config.primary_key_cols
        pk_select = ", ".join([f"T.`{c}`" for c in pk_cols])

        return f"""
CREATE OR REPLACE TABLE `{config.staging_fqn}` AS (

  WITH source_snapshot AS (
    SELECT
      src.*,
      {hash_expr} AS _row_hash
    FROM `{config.source_fqn}` src
    WHERE {watermark_filter}
  ),

  target_hashes AS (
    SELECT
      {pk_select},
      {hash_expr.replace('src.', 'T.')} AS _row_hash
    FROM `{config.target_fqn}` T
  ),

  delta AS (
    SELECT
      S.*,
      CASE
        WHEN TH.`{pk_cols[0]}` IS NULL THEN 'INSERT'
        WHEN S._row_hash != TH._row_hash  THEN 'UPDATE'
        ELSE NULL
      END AS _change_type
    FROM source_snapshot S
    LEFT JOIN target_hashes TH
      ON {pk_join_expr.replace('T.', 'TH.').replace('S.', 'S.')}
    WHERE
      TH.`{pk_cols[0]}` IS NULL
      OR S._row_hash != TH._row_hash
  )

  SELECT * FROM delta
  WHERE _change_type IS NOT NULL

);
"""

    def _get_source_count(self, config: TableConfig, watermark_filter: str) -> int:
        sql = f"""
            SELECT COUNT(*) FROM `{config.source_fqn}` src
            WHERE {watermark_filter}
        """
        return self.bq.run_scalar_query(sql) or 0

    def _get_staging_count(self, config: TableConfig) -> int:
        sql = f"SELECT COUNT(*) FROM `{config.staging_fqn}`"
        return self.bq.run_scalar_query(sql) or 0
