"""
framework/ingestion_engine.py
===============================
IngestionEngine runs the final MERGE statement that upserts rows from
the staging table into the target production table.

MERGE semantics
---------------
- WHEN MATCHED AND _row_hash differs → UPDATE all non-PK columns
- WHEN NOT MATCHED BY TARGET        → INSERT the new row
- WHEN NOT MATCHED BY SOURCE        → no action (soft-delete not in scope)

The staging table is always the authoritative source for this run.
Rows quarantined by DQValidator have already been removed from staging,
so they are never merged.

Schema alignment
----------------
The MERGE is column-list aware.  It uses the staging table's schema
(minus framework columns _row_hash, _change_type, _extracted_at) to
build a precise column list, preventing "schema mismatch" errors when
the target has additional columns (e.g. _ingestion_ts).
"""

from __future__ import annotations

import logging
import re

from google.cloud import bigquery

from .config_loader import TableConfig
from .exceptions import IngestionError

logger = logging.getLogger(__name__)

# Framework columns added by DeltaDetector — excluded from MERGE payload
_FRAMEWORK_COLS = frozenset({"_row_hash", "_change_type", "_extracted_at"})


class IngestionEngine:
    """
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

    def merge(self, config: TableConfig, staging_ref: str) -> int:
        """
        Merge rows from *staging_ref* into the target table.

        Returns
        -------
        int
            Number of rows affected (inserted + updated).

        Raises
        ------
        IngestionError
            If the BigQuery MERGE job fails.
        """
        # Discover columns from staging schema
        data_cols = self._get_data_columns(staging_ref)
        pk_cols = config.primary_key_cols
        non_pk_cols = [c for c in data_cols if c not in set(pk_cols)]

        sql = self._build_merge_sql(config, staging_ref, pk_cols, non_pk_cols, data_cols)
        logger.debug("Merge SQL:\n%s", sql)

        try:
            job = self._client.query(sql)
            job.result()  # blocks until complete

            # Row count from DML statistics
            affected = job.num_dml_affected_rows or 0
            logger.info(
                "Merge complete: registry_id=%s target=%s rows_affected=%d",
                config.registry_id, config.target_table_ref, affected,
            )
            return affected

        except Exception as exc:
            raise IngestionError(
                f"MERGE failed for {config.registry_id}: {exc}",
                registry_id=config.registry_id,
            ) from exc

    # ── SQL builder ───────────────────────────────────────────────────────────

    def _build_merge_sql(
        self,
        config: TableConfig,
        staging_ref: str,
        pk_cols: list[str],
        non_pk_cols: list[str],
        all_data_cols: list[str],
    ) -> str:
        # ON clause: match on all primary key columns
        on_clause = "\n      AND ".join(
            f"T.{self._q(c)} = S.{self._q(c)}" for c in pk_cols
        )

        # UPDATE SET: update all non-PK columns
        update_set = ",\n          ".join(
            f"T.{self._q(c)} = S.{self._q(c)}" for c in non_pk_cols + ["_row_hash"]
        )

        # INSERT column and value lists
        insert_cols = ", ".join(self._q(c) for c in all_data_cols)
        insert_vals = ", ".join(f"S.{self._q(c)}" for c in all_data_cols)

        sql = f"""
        MERGE {config.target_table_ref} AS T
        USING (
            SELECT * FROM {staging_ref}
        ) AS S
        ON {on_clause}
        WHEN MATCHED AND T._row_hash != S._row_hash THEN
            UPDATE SET
                {update_set},
                T._ingestion_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols}, _ingestion_ts)
            VALUES ({insert_vals}, CURRENT_TIMESTAMP())
        """
        return sql.strip()

    # ── Schema helpers ────────────────────────────────────────────────────────

    def _get_data_columns(self, staging_ref: str) -> list[str]:
        """
        Returns column names from the staging table schema,
        excluding framework-added columns (_row_hash etc).
        """
        # staging_ref is like `project.dataset.table`
        table_id = staging_ref.strip("`")
        try:
            table = self._client.get_table(table_id)
            return [
                f.name for f in table.schema
                if f.name not in _FRAMEWORK_COLS
            ]
        except Exception as exc:
            raise IngestionError(
                f"Cannot fetch schema for staging table {staging_ref}: {exc}"
            ) from exc

    @staticmethod
    def _q(col: str) -> str:
        return f"`{col}`"
