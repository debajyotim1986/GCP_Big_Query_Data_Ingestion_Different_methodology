"""
framework/delta_detector.py
=============================
DeltaDetector builds and runs the BigQuery query that:

  1. Reads from the **source** table (with optional watermark filter).
  2. Computes a SHA256 row-hash over ``delta_hash_cols`` (or all PK cols
     if delta_hash_cols is empty).
  3. Compares hashes against the **target** table.
  4. Writes **only changed/new rows** into ``_delta_staging_<registry_id>``.

The staging table is always WRITE_TRUNCATE so it is idempotent across retries.

Returns
-------
int
    Number of rows written to the staging table (delta count).
    If 0, the pipeline skips ingestion and advances the watermark anyway
    (no data gap is created for empty windows).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime

from google.cloud import bigquery

from .config_loader import TableConfig
from .exceptions import ExtractionError, StagingTableError

logger = logging.getLogger(__name__)


class DeltaDetector:
    """
    Parameters
    ----------
    client : bigquery.Client
    project : str
        GCP project hosting the framework tables.
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

    def extract_delta(self, config: TableConfig) -> int:
        """
        Extract changed rows from source into the staging table.

        Returns the number of delta rows written.  Caller treats 0 as
        "no-op run" — watermark still advances, nothing is merged.
        """
        staging_ref = self._staging_ref(config.registry_id)
        self._ensure_staging_table(config, staging_ref)

        sql = self._build_extraction_sql(config, staging_ref)
        logger.debug("Delta extraction SQL:\n%s", sql)

        try:
            job_config = bigquery.QueryJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                destination=None,  # using INSERT INTO ... SELECT pattern
                use_legacy_sql=False,
            )
            # Execute as a standard query job — results go into staging via INSERT
            job = self._client.query(sql)
            job.result()  # block until done, raises on BQ error

            row_count = self._count_staging(staging_ref)
            logger.info(
                "Delta extracted: registry_id=%s staging=%s rows=%d mode=%s",
                config.registry_id, staging_ref, row_count, config.load_mode,
            )
            return row_count

        except Exception as exc:
            raise ExtractionError(
                f"Delta extraction failed for {config.registry_id}: {exc}",
                registry_id=config.registry_id,
            ) from exc

    def drop_staging_table(self, registry_id: str) -> None:
        """Drop the staging table. Called in the PipelineRunner finally block."""
        staging_ref = self._staging_ref(registry_id)
        try:
            self._client.delete_table(staging_ref, not_found_ok=True)
            logger.info("Staging table dropped: %s", staging_ref)
        except Exception as exc:
            logger.warning("Failed to drop staging table %s: %s", staging_ref, exc)

    # ── SQL builder ───────────────────────────────────────────────────────────

    def _build_extraction_sql(self, config: TableConfig, staging_ref: str) -> str:
        """
        Builds:
            INSERT INTO <staging_ref>
            SELECT src.*, SHA256(<hash_cols>) AS _row_hash
            FROM <source> src
            LEFT JOIN <target> tgt ON <pk_join>
            WHERE <watermark_filter>
              AND (tgt.<pk> IS NULL OR SHA256(<hash_cols>) != tgt._row_hash)
            [LIMIT batch_size]
        """
        hash_cols = config.effective_hash_cols
        pk_cols = config.primary_key_cols

        # Hash expression over selected columns
        hash_expr = self._build_hash_expr(hash_cols)

        # Primary key join predicate
        pk_join = " AND ".join(
            f"src.{self._q(col)} = tgt.{self._q(col)}" for col in pk_cols
        )

        # NULL-check for new rows (target has no matching PK)
        pk_null_check = f"tgt.{self._q(pk_cols[0])} IS NULL"

        # Watermark filter
        if config.is_full_load:
            watermark_filter = "TRUE"
        else:
            wm_col = config.watermark_col
            wm_from = config.watermark_from.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            wm_to = config.watermark_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            watermark_filter = (
                f"src.{self._q(wm_col)} > TIMESTAMP('{wm_from}')\n"
                f"              AND src.{self._q(wm_col)} <= TIMESTAMP('{wm_to}')"
            )

        # Optional row limit
        limit_clause = f"\n            LIMIT {config.batch_size}" if config.batch_size else ""

        sql = f"""
        INSERT INTO {staging_ref}
        SELECT
            src.*,
            {hash_expr} AS _row_hash,
            CASE
                WHEN {pk_null_check} THEN 'INSERT'
                ELSE 'UPDATE'
            END AS _change_type,
            CURRENT_TIMESTAMP() AS _extracted_at
        FROM {config.source_table_ref} src
        LEFT JOIN {config.target_table_ref} tgt
          ON {pk_join}
        WHERE {watermark_filter}
          AND ({pk_null_check} OR {hash_expr} != tgt._row_hash){limit_clause}
        """
        return sql.strip()

    @staticmethod
    def _build_hash_expr(cols: list[str]) -> str:
        """SHA256 over concatenated column values, coerced to STRING."""
        coerced = [f"COALESCE(CAST({DeltaDetector._q(c)} AS STRING), '')" for c in cols]
        concat = " || '|' || ".join(coerced)
        return f"TO_HEX(SHA256({concat}))"

    # ── Staging table management ───────────────────────────────────────────────

    def _ensure_staging_table(self, config: TableConfig, staging_ref: str) -> None:
        """
        Creates the staging table with the same schema as the source table
        plus the three framework columns: _row_hash, _change_type, _extracted_at.
        Uses CREATE TABLE IF NOT EXISTS — safe for retries.
        """
        sql = f"""
        CREATE TABLE IF NOT EXISTS {staging_ref}
        AS SELECT
            src.*,
            '' AS _row_hash,
            '' AS _change_type,
            CURRENT_TIMESTAMP() AS _extracted_at
        FROM {config.source_table_ref} src
        WHERE FALSE
        """
        try:
            self._client.query(sql).result()
            logger.debug("Staging table ensured: %s", staging_ref)
        except Exception as exc:
            raise StagingTableError(
                f"Failed to create staging table for {config.registry_id}: {exc}",
                registry_id=config.registry_id,
            ) from exc

    def _count_staging(self, staging_ref: str) -> int:
        sql = f"SELECT COUNT(*) AS cnt FROM {staging_ref}"
        rows = list(self._client.query(sql).result())
        return rows[0]["cnt"] if rows else 0

    def _staging_ref(self, registry_id: str) -> str:
        """BQ table reference for the staging table."""
        safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", registry_id)
        return f"`{self._project}.{self._dataset}._delta_staging_{safe_id}`"

    @staticmethod
    def _q(col: str) -> str:
        """Backtick-quote a column name."""
        return f"`{col}`"
