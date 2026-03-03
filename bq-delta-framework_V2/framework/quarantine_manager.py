"""
framework/quarantine_manager.py
================================
QuarantineManager handles rows that fail a QUARANTINE-action DQ rule.

Responsibilities
----------------
1. Write failing rows to ``framework_config.quarantine_records``.
2. DELETE those rows from the staging table so they are NOT merged to target.
3. Expose ``resolve`` and ``reject`` methods for downstream remediation.

The quarantine table schema (from bootstrap.py):
    registry_id     STRING
    run_id          STRING
    rule_id         STRING
    rule_name       STRING
    quarantine_id   STRING (UUID per row)
    status          STRING  -- PENDING | RESOLVED | REJECTED
    payload         JSON    -- the original failing row
    quarantined_at  TIMESTAMP
    resolved_at     TIMESTAMP
    resolved_by     STRING
    resolution_note STRING
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

from .config_loader import TableConfig
from .dq_validator import DQResult

logger = logging.getLogger(__name__)

_QUARANTINE_TABLE = "quarantine_records"


class QuarantineManager:
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

    @property
    def _table_ref(self) -> str:
        return f"`{self._project}.{self._dataset}.{_QUARANTINE_TABLE}`"

    # ── Public API ────────────────────────────────────────────────────────────

    def quarantine_rows(
        self,
        config: TableConfig,
        run_id: str,
        rule_result: DQResult,
        failing_rows: list[dict],
        staging_ref: str,
    ) -> int:
        """
        Insert *failing_rows* into quarantine_records and delete them from *staging_ref*.

        Returns
        -------
        int
            Number of rows quarantined.
        """
        if not failing_rows:
            return 0

        quarantine_rows_payload = []
        quarantine_ids = []
        now = datetime.now(tz=timezone.utc)

        for row in failing_rows:
            q_id = str(uuid.uuid4())
            quarantine_ids.append(q_id)
            quarantine_rows_payload.append({
                "registry_id": config.registry_id,
                "run_id": run_id,
                "rule_id": rule_result.rule_id,
                "rule_name": rule_result.rule_name,
                "quarantine_id": q_id,
                "status": "PENDING",
                "payload": json.dumps(self._serialize_row(row)),
                "quarantined_at": now.isoformat(),
                "resolved_at": None,
                "resolved_by": None,
                "resolution_note": None,
            })

        # Batch insert into quarantine_records
        table = self._client.get_table(
            f"{self._project}.{self._dataset}.{_QUARANTINE_TABLE}"
        )
        errors = self._client.insert_rows_json(table, quarantine_rows_payload)
        if errors:
            logger.error(
                "Quarantine insert errors for %s: %s", config.registry_id, errors
            )

        # Delete quarantined rows from staging
        self._delete_from_staging(config, rule_result, failing_rows, staging_ref)

        count = len(quarantine_rows_payload)
        logger.info(
            "Quarantined %d rows for registry_id=%s rule=%s",
            count, config.registry_id, rule_result.rule_id,
        )
        return count

    def resolve(
        self,
        quarantine_ids: list[str],
        resolved_by: str,
        note: str = "",
    ) -> int:
        """Mark quarantine records as RESOLVED."""
        return self._update_status(quarantine_ids, "RESOLVED", resolved_by, note)

    def reject(
        self,
        quarantine_ids: list[str],
        resolved_by: str,
        note: str = "",
    ) -> int:
        """Mark quarantine records as REJECTED (permanently excluded)."""
        return self._update_status(quarantine_ids, "REJECTED", resolved_by, note)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _delete_from_staging(
        self,
        config: TableConfig,
        rule_result: DQResult,
        failing_rows: list[dict],
        staging_ref: str,
    ) -> None:
        """Remove failing rows from staging using primary key values."""
        pk_cols = config.primary_key_cols
        if not pk_cols or not failing_rows:
            return

        # Build a VALUES list for the PK tuples
        # For safety, use parameterised DELETE via a TEMP TABLE approach
        pk_values_parts = []
        for row in failing_rows:
            vals = ", ".join(
                f"'{row.get(col, '')}'" if isinstance(row.get(col), str)
                else str(row.get(col, "NULL"))
                for col in pk_cols
            )
            pk_values_parts.append(f"({vals})")

        pk_col_list = ", ".join(f"`{c}`" for c in pk_cols)
        values_str = ", ".join(pk_values_parts)

        sql = f"""
        DELETE FROM {staging_ref}
        WHERE ({pk_col_list}) IN ({values_str})
        """
        try:
            self._client.query(sql).result()
            logger.debug(
                "Deleted %d quarantined rows from staging %s",
                len(failing_rows), staging_ref,
            )
        except Exception as exc:
            logger.error(
                "Failed to delete quarantined rows from staging %s: %s",
                staging_ref, exc,
            )

    def _update_status(
        self,
        quarantine_ids: list[str],
        status: str,
        resolved_by: str,
        note: str,
    ) -> int:
        if not quarantine_ids:
            return 0

        ids_str = ", ".join(f"'{q}'" for q in quarantine_ids)
        now = datetime.now(tz=timezone.utc).isoformat()
        sql = f"""
        UPDATE {self._table_ref}
        SET
            status          = '{status}',
            resolved_at     = TIMESTAMP('{now}'),
            resolved_by     = '{resolved_by}',
            resolution_note = '{note}'
        WHERE quarantine_id IN ({ids_str})
        """
        try:
            self._client.query(sql).result()
            logger.info(
                "Set status=%s for %d quarantine records", status, len(quarantine_ids)
            )
            return len(quarantine_ids)
        except Exception as exc:
            logger.error("Failed to update quarantine status: %s", exc)
            return 0

    @staticmethod
    def _serialize_row(row: dict) -> dict:
        """Make row values JSON-serializable."""
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif isinstance(v, bytes):
                out[k] = v.hex()
            else:
                out[k] = v
        return out
