"""
dashboard/app.py
================
Cloud Run Dashboard for the BQ Delta Ingestion Framework.

FastAPI application that queries BigQuery framework_config tables
and renders a real-time pipeline health dashboard.

Endpoints
---------
GET  /                     → HTML dashboard (Jinja2 rendered)
GET  /api/summary          → KPI aggregates for today (cached 60s)
GET  /api/runs             → Last 50 pipeline runs (last 24 h, sorted DESC)
GET  /api/runs/{run_id}    → Single run full detail + DQ breakdown
GET  /api/dq/{run_id}      → All DQ rule failures for a given run_id
GET  /api/quarantine        → PENDING quarantine rows paginated
GET  /api/tables            → All active tables with last watermark
GET  /health               → Cloud Run liveness probe

Configuration (environment variables)
--------------------------------------
GCP_PROJECT          GCP project ID (required)
FRAMEWORK_DATASET    BQ dataset name (default: framework_config)
BQ_LOCATION          BigQuery location  (default: US)
LOG_LEVEL            DEBUG | INFO | WARNING (default: INFO)
LOG_FORMAT           text | json (default: json for Cloud Run)
PORT                 HTTP port (default: 8080)
CACHE_TTL_SECONDS    Result cache TTL in seconds (default: 60)

Auth
----
Cloud Run attaches a service account with roles/bigquery.dataViewer
on the framework_config dataset. Locally, ADC (gcloud auth
application-default login) is used automatically by the BQ client.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

GCP_PROJECT: str = os.environ["GCP_PROJECT"]
FRAMEWORK_DATASET: str = os.environ.get("FRAMEWORK_DATASET", "framework_config")
BQ_LOCATION: str = os.environ.get("BQ_LOCATION", "US")
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FORMAT: str = os.environ.get("LOG_FORMAT", "json")
CACHE_TTL_SECONDS: int = int(os.environ.get("CACHE_TTL_SECONDS", "60"))


# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

class _JSONFormatter(logging.Formatter):
    """Structured JSON formatter compatible with Cloud Logging."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _configure_logging() -> logging.Logger:
    handler = logging.StreamHandler()
    if LOG_FORMAT == "json":
        handler.setFormatter(_JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)-8s] %(name)s — %(message)s")
        )
    logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), handlers=[handler])
    return logging.getLogger("dashboard")


log = _configure_logging()


# ──────────────────────────────────────────────────────────────────────────────
# BigQuery client (singleton)
# ──────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _bq_client() -> bigquery.Client:
    log.info("Initialising BigQuery client", extra={"project": GCP_PROJECT})
    return bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


def _qualified(table: str) -> str:
    """Return fully-qualified BQ table ref."""
    return f"`{GCP_PROJECT}.{FRAMEWORK_DATASET}.{table}`"


def _run_query(sql: str, params: list[bigquery.ScalarQueryParameter] | None = None) -> list[dict]:
    """Execute BQ query and return rows as list of dicts."""
    client = _bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    try:
        rows = client.query(sql, job_config=job_config).result()
        return [dict(row) for row in rows]
    except Exception as exc:
        log.error("BQ query failed: %s | sql_preview=%s", exc, sql[:200])
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Cache — simple TTL wrapper around a dict
# ──────────────────────────────────────────────────────────────────────────────

_cache: dict[str, tuple[float, Any]] = {}


def _cached(key: str, fn, ttl: int = CACHE_TTL_SECONDS) -> Any:
    """Return cached result if still fresh, otherwise call fn() and store."""
    now = time.monotonic()
    if key in _cache:
        stored_at, value = _cache[key]
        if now - stored_at < ttl:
            return value
    value = fn()
    _cache[key] = (now, value)
    return value


def _invalidate(key: str) -> None:
    _cache.pop(key, None)


# ──────────────────────────────────────────────────────────────────────────────
# Query functions
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_summary() -> dict:
    """KPI aggregates: today's runs broken down by status."""
    sql = f"""
    SELECT
        COUNT(*)                                                AS total_runs,
        COUNTIF(status = 'SUCCESS')                            AS successful,
        COUNTIF(status = 'FAILED')                             AS failed,
        COUNTIF(status = 'DQ_BLOCKED')                         AS dq_blocked,
        COUNTIF(status = 'LOCK_CONFLICT')                      AS lock_conflicts,
        SUM(COALESCE(source_row_count, 0))                     AS total_source_rows,
        SUM(COALESCE(delta_count, 0))                          AS total_delta_rows,
        SUM(COALESCE(ingested_count, 0))                       AS total_ingested,
        ROUND(AVG(COALESCE(duration_seconds, 0)), 2)           AS avg_duration_seconds,
        ROUND(MAX(COALESCE(duration_seconds, 0)), 2)           AS max_duration_seconds,
        COUNT(DISTINCT registry_id)                            AS tables_processed
    FROM {_qualified("run_audit")}
    WHERE DATE(started_at) = CURRENT_DATE()
    """
    rows = _run_query(sql)
    return rows[0] if rows else {}


def _fetch_runs(limit: int = 50, hours: int = 24) -> list[dict]:
    """Recent runs sorted by started_at DESC."""
    sql = f"""
    SELECT
        run_id,
        registry_id,
        dag_run_id,
        status,
        watermark_from,
        watermark_to,
        source_row_count,
        delta_count,
        ingested_count,
        dq_rules_passed,
        dq_rules_warned,
        dq_rules_failed,
        error_message,
        duration_seconds,
        started_at,
        ended_at
    FROM {_qualified("run_audit")}
    WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
    ORDER BY started_at DESC
    LIMIT @limit
    """
    return _run_query(
        sql,
        [
            bigquery.ScalarQueryParameter("hours", "INT64", hours),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def _fetch_run_detail(run_id: str) -> dict | None:
    """Single run full record."""
    sql = f"""
    SELECT *
    FROM {_qualified("run_audit")}
    WHERE run_id = @run_id
    LIMIT 1
    """
    rows = _run_query(sql, [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)])
    return rows[0] if rows else None


def _fetch_dq_errors(run_id: str) -> list[dict]:
    """All DQ error log rows for a specific run."""
    sql = f"""
    SELECT
        error_id,
        run_id,
        registry_id,
        rule_id,
        action_on_fail,
        failed_row_count,
        failure_rate_pct,
        sample_failed_rows,
        rule_sql_used,
        logged_at
    FROM {_qualified("dq_error_log")}
    WHERE run_id = @run_id
    ORDER BY logged_at ASC
    """
    return _run_query(sql, [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)])


def _fetch_quarantine(
    status_filter: str = "PENDING",
    registry_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Quarantine records filtered by remediation_status."""
    filters = ["remediation_status = @status_filter"]
    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("status_filter", "STRING", status_filter),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]
    if registry_id:
        filters.append("registry_id = @registry_id")
        params.append(bigquery.ScalarQueryParameter("registry_id", "STRING", registry_id))

    where_clause = " AND ".join(filters)
    sql = f"""
    SELECT
        quarantine_id,
        run_id,
        rule_id,
        registry_id,
        failure_reason,
        remediation_status,
        remediated_by,
        remediated_at,
        created_at
    FROM {_qualified("quarantine_records")}
    WHERE {where_clause}
    ORDER BY created_at DESC
    LIMIT @limit
    """
    return _run_query(sql, params)


def _fetch_quarantine_summary() -> list[dict]:
    """Count of PENDING quarantine rows grouped by registry_id."""
    sql = f"""
    SELECT
        registry_id,
        COUNT(*) AS pending_count,
        MIN(created_at) AS oldest_pending
    FROM {_qualified("quarantine_records")}
    WHERE remediation_status = 'PENDING'
    GROUP BY registry_id
    ORDER BY pending_count DESC
    """
    return _run_query(sql)


def _fetch_tables() -> list[dict]:
    """All active tables with their last watermark from process_control."""
    sql = f"""
    SELECT
        tr.registry_id,
        tr.source_project,
        tr.source_dataset,
        tr.source_table,
        tr.target_project,
        tr.target_dataset,
        tr.target_table,
        tr.watermark_col,
        tr.is_active,
        tr.load_type,
        pc.last_successful_run,
        pc.status          AS lock_status,
        pc.locked_at,
        pc.locked_by,
        (
            SELECT MAX(started_at)
            FROM {_qualified("run_audit")} ra
            WHERE ra.registry_id = tr.registry_id
        )                  AS last_run_started_at
    FROM {_qualified("table_registry")} tr
    LEFT JOIN {_qualified("process_control")} pc
        ON pc.registry_id = tr.registry_id
    WHERE tr.is_active = TRUE
    ORDER BY tr.registry_id ASC
    """
    return _run_query(sql)


def _fetch_failure_trend(days: int = 7) -> list[dict]:
    """Daily failure counts for the last N days — used for trend chart."""
    sql = f"""
    SELECT
        DATE(started_at)          AS run_date,
        status,
        COUNT(*)                  AS run_count
    FROM {_qualified("run_audit")}
    WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
    GROUP BY run_date, status
    ORDER BY run_date ASC, status ASC
    """
    return _run_query(sql, [bigquery.ScalarQueryParameter("days", "INT64", days)])


# ──────────────────────────────────────────────────────────────────────────────
# Serialisation helper — BQ returns datetime/Decimal objects
# ──────────────────────────────────────────────────────────────────────────────

def _serialise(obj: Any) -> Any:
    """Recursively convert BQ result types to JSON-serialisable values."""
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialise(i) for i in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "__float__"):  # Decimal
        return float(obj)
    return obj


# ──────────────────────────────────────────────────────────────────────────────
# FastAPI application
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="BQ Delta Ingestion Framework — Dashboard",
    description="Real-time pipeline health dashboard for the BigQuery Delta Ingestion Framework.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


# ── Middleware: request logging ────────────────────────────────────────────────

@app.middleware("http")
async def _log_requests(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = round((time.monotonic() - start) * 1000, 1)
    log.info(
        "%s %s → %s (%.1f ms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


# ──────────────────────────────────────────────────────────────────────────────
# API Endpoints
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health() -> dict:
    """
    Cloud Run liveness / readiness probe.
    Returns 200 OK immediately — no BQ call.
    """
    return {
        "status": "ok",
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "project": GCP_PROJECT,
        "dataset": FRAMEWORK_DATASET,
    }


@app.get("/api/summary", tags=["dashboard"])
def api_summary() -> JSONResponse:
    """
    Today's KPI aggregates (cached CACHE_TTL_SECONDS seconds).

    Returns totals for: total_runs, successful, failed, dq_blocked,
    lock_conflicts, total_source_rows, total_delta_rows, total_ingested,
    avg_duration_seconds, max_duration_seconds, tables_processed.
    """
    data = _cached("summary", _fetch_summary)
    return JSONResponse(content=_serialise(data))


@app.get("/api/runs", tags=["dashboard"])
def api_runs(
    hours: int = Query(default=24, ge=1, le=168, description="Look-back window in hours"),
    limit: int = Query(default=50, ge=1, le=500, description="Maximum rows to return"),
) -> JSONResponse:
    """
    Recent pipeline runs sorted newest-first.
    Includes full row counts, DQ rule pass/warn/fail counts, and error_message.
    """
    cache_key = f"runs:{hours}:{limit}"
    data = _cached(cache_key, lambda: _fetch_runs(limit=limit, hours=hours))
    return JSONResponse(content=_serialise(data))


@app.get("/api/runs/{run_id}", tags=["dashboard"])
def api_run_detail(run_id: str) -> JSONResponse:
    """
    Full detail for a single pipeline run including all audit columns.
    Raises 404 if the run_id does not exist in run_audit.
    """
    cache_key = f"run:{run_id}"
    data = _cached(cache_key, lambda: _fetch_run_detail(run_id), ttl=300)
    if data is None:
        raise HTTPException(status_code=404, detail=f"run_id '{run_id}' not found in run_audit")
    return JSONResponse(content=_serialise(data))


@app.get("/api/dq/{run_id}", tags=["dashboard"])
def api_dq_errors(run_id: str) -> JSONResponse:
    """
    All DQ error log entries for a specific run.
    Includes rule_id, action_on_fail, failed_row_count, failure_rate_pct,
    sample_failed_rows (JSON string), and the rule_sql_used.
    """
    cache_key = f"dq:{run_id}"
    data = _cached(cache_key, lambda: _fetch_dq_errors(run_id), ttl=120)
    return JSONResponse(content=_serialise(data))


@app.get("/api/quarantine", tags=["dashboard"])
def api_quarantine(
    status: str = Query(
        default="PENDING",
        description="Filter by remediation_status: PENDING | REINGESTED | DISCARDED | UNDER_REVIEW",
    ),
    registry_id: str | None = Query(default=None, description="Filter to a specific table"),
    limit: int = Query(default=100, ge=1, le=1000),
) -> JSONResponse:
    """
    Quarantine records filtered by remediation_status (default PENDING).
    Includes the raw_record column and failure_reason for each quarantined row.
    Optionally filter to a specific registry_id.
    """
    cache_key = f"quarantine:{status}:{registry_id}:{limit}"
    data = _cached(
        cache_key,
        lambda: _fetch_quarantine(status_filter=status, registry_id=registry_id, limit=limit),
        ttl=30,
    )
    return JSONResponse(content=_serialise(data))


@app.get("/api/quarantine/summary", tags=["dashboard"])
def api_quarantine_summary() -> JSONResponse:
    """
    Count of PENDING quarantine rows grouped by registry_id.
    Used for the dashboard quarantine badge counts.
    """
    data = _cached("quarantine_summary", _fetch_quarantine_summary, ttl=60)
    return JSONResponse(content=_serialise(data))


@app.get("/api/tables", tags=["dashboard"])
def api_tables() -> JSONResponse:
    """
    All active tables from table_registry joined with their current
    process_control state (last watermark, lock status, locked_by).
    """
    data = _cached("tables", _fetch_tables, ttl=120)
    return JSONResponse(content=_serialise(data))


@app.get("/api/trend", tags=["dashboard"])
def api_trend(
    days: int = Query(default=7, ge=1, le=30, description="Number of days to look back"),
) -> JSONResponse:
    """
    Daily run counts broken down by status for the last N days.
    Used to render the trend chart on the dashboard.
    """
    cache_key = f"trend:{days}"
    data = _cached(cache_key, lambda: _fetch_failure_trend(days=days), ttl=300)
    return JSONResponse(content=_serialise(data))


@app.delete("/api/cache", tags=["ops"])
def clear_cache(key: str | None = Query(default=None, description="Specific cache key to clear, or all")) -> dict:
    """
    Clear the in-memory result cache.
    Pass ?key=summary to clear a specific key, or no key to clear all.
    Useful after manual data corrections.
    """
    if key:
        _invalidate(key)
        return {"cleared": key}
    _cache.clear()
    return {"cleared": "all"}


# ──────────────────────────────────────────────────────────────────────────────
# HTML Dashboard
# ──────────────────────────────────────────────────────────────────────────────

_HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BQ Delta Framework — Dashboard</title>
<style>
  :root {
    --bg: #f0f9ff; --surface: #ffffff; --border: #bae6fd;
    --blue: #1d4ed8; --cyan: #0891b2; --green: #15803d;
    --red: #b91c1c; --yellow: #a16207; --purple: #6d28d9;
    --text: #0f172a; --text2: #334155; --text3: #64748b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: var(--bg); color: var(--text); font-size: 15px; }

  /* ── Header ── */
  header {
    background: linear-gradient(135deg, #1d4ed8 0%, #0891b2 100%);
    color: #fff; padding: 18px 32px;
    display: flex; align-items: center; justify-content: space-between;
  }
  header h1 { font-size: 22px; font-weight: 700; letter-spacing: -0.5px; }
  header .meta { font-size: 12px; opacity: 0.8; margin-top: 4px; }
  .refresh-btn {
    background: rgba(255,255,255,0.18); border: 1px solid rgba(255,255,255,0.3);
    color: #fff; padding: 8px 18px; border-radius: 8px; cursor: pointer;
    font-size: 13px; font-weight: 600; transition: background 0.15s;
  }
  .refresh-btn:hover { background: rgba(255,255,255,0.28); }

  /* ── Layout ── */
  main { max-width: 1400px; margin: 0 auto; padding: 28px 32px 80px; }

  /* ── KPI cards ── */
  .kpi-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 32px; }
  .kpi {
    flex: 1; min-width: 160px; background: var(--surface);
    border: 2px solid var(--border); border-radius: 14px;
    padding: 20px 22px; box-shadow: 0 2px 12px rgba(56,189,248,0.08);
  }
  .kpi-label { font-size: 11px; text-transform: uppercase; letter-spacing: 1.5px; color: var(--text3); font-weight: 700; margin-bottom: 8px; }
  .kpi-value { font-size: 36px; font-weight: 800; line-height: 1; }
  .kpi-sub   { font-size: 12px; color: var(--text3); margin-top: 6px; }
  .kpi.success { border-color: #86efac; }
  .kpi.success .kpi-value { color: var(--green); }
  .kpi.danger  { border-color: #fca5a5; }
  .kpi.danger  .kpi-value { color: var(--red); }
  .kpi.warn    { border-color: #fde68a; }
  .kpi.warn    .kpi-value { color: var(--yellow); }
  .kpi.blue    { border-color: #bfdbfe; }
  .kpi.blue    .kpi-value { color: var(--blue); }

  /* ── Section titles ── */
  .section-title {
    font-size: 16px; font-weight: 700; color: var(--text);
    border-left: 4px solid var(--blue); padding-left: 12px;
    margin-bottom: 16px; margin-top: 36px;
  }

  /* ── Tables ── */
  .table-wrap { overflow-x: auto; border-radius: 12px; border: 2px solid var(--border); }
  table { width: 100%; border-collapse: collapse; background: var(--surface); font-size: 13px; }
  th {
    background: linear-gradient(135deg, #1d4ed8 0%, #0891b2 100%);
    color: #fff; text-align: left; padding: 12px 14px;
    font-size: 11px; text-transform: uppercase; letter-spacing: 1px; font-weight: 700;
  }
  td { padding: 11px 14px; border-bottom: 1px solid #e0f2fe; vertical-align: top; }
  tr:last-child td { border-bottom: none; }
  tr:nth-child(even) td { background: #f0f9ff; }
  tr:hover td { background: #dbeafe; cursor: pointer; }

  /* ── Status badges ── */
  .badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 11px; font-weight: 700; letter-spacing: 0.5px;
  }
  .badge-success   { background: #dcfce7; color: #14532d; border: 1px solid #86efac; }
  .badge-failed    { background: #fee2e2; color: #7f1d1d; border: 1px solid #fca5a5; }
  .badge-dqblock   { background: #f5f3ff; color: #3b0764; border: 1px solid #c4b5fd; }
  .badge-running   { background: #fef9c3; color: #713f12; border: 1px solid #fde68a; }
  .badge-lock      { background: #ffedd5; color: #7c2d12; border: 1px solid #fed7aa; }
  .badge-pending   { background: #fff7ed; color: #7c2d12; border: 1px solid #fed7aa; }
  .badge-active    { background: #dcfce7; color: #14532d; }
  .badge-inactive  { background: #f1f5f9; color: #475569; }

  /* ── Detail panel ── */
  #detail-panel {
    display: none; position: fixed; right: 0; top: 0; bottom: 0; width: 480px;
    background: var(--surface); border-left: 2px solid var(--border);
    box-shadow: -8px 0 32px rgba(0,0,0,0.12); overflow-y: auto;
    z-index: 100; padding: 28px;
  }
  #detail-panel.open { display: block; }
  #detail-panel h2 { font-size: 18px; margin-bottom: 6px; }
  #detail-panel .close-btn {
    position: absolute; top: 20px; right: 20px; cursor: pointer;
    font-size: 22px; color: var(--text3); background: none; border: none;
  }
  .detail-row { display: flex; margin-bottom: 10px; gap: 12px; align-items: flex-start; }
  .detail-label { width: 140px; min-width: 140px; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: var(--text3); padding-top: 2px; }
  .detail-value { font-size: 13px; color: var(--text); word-break: break-all; }
  .error-box { background: #fff1f2; border: 1px solid #fca5a5; border-radius: 8px; padding: 12px 14px; font-size: 12px; color: #7f1d1d; font-family: monospace; margin-top: 8px; }
  .dq-rule-card { background: #fef9c3; border: 1px solid #fde68a; border-radius: 8px; padding: 12px; margin-bottom: 8px; }
  .dq-rule-name { font-weight: 700; font-size: 13px; margin-bottom: 4px; }
  .dq-rule-detail { font-size: 12px; color: var(--text3); }

  /* ── Loading ── */
  .loading { text-align: center; padding: 40px; color: var(--text3); font-size: 14px; }
  .spinner { display: inline-block; width: 20px; height: 20px; border: 3px solid #bfdbfe; border-top-color: var(--blue); border-radius: 50%; animation: spin 0.8s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes spin { to { transform: rotate(360deg); } }

  .mono { font-family: 'JetBrains Mono', 'Fira Code', monospace; font-size: 12px; }
  .text-right { text-align: right; }
  .text-muted { color: var(--text3); }
  .last-refresh { font-size: 12px; color: rgba(255,255,255,0.7); text-align: right; }
</style>
</head>
<body>

<header>
  <div>
    <h1>BQ Delta Ingestion Framework</h1>
    <div class="meta">Pipeline Health Dashboard &nbsp;·&nbsp; {project} &nbsp;·&nbsp; {dataset}</div>
  </div>
  <div style="text-align:right">
    <button class="refresh-btn" onclick="loadAll()">↻ Refresh</button>
    <div class="last-refresh" id="last-refresh">Loading…</div>
  </div>
</header>

<main>

  <!-- KPI row -->
  <div class="kpi-row" id="kpi-row">
    <div class="loading"><span class="spinner"></span>Loading KPIs…</div>
  </div>

  <!-- Runs table -->
  <div class="section-title">Recent Pipeline Runs (last 24 h)</div>
  <div class="table-wrap">
    <table id="runs-table">
      <thead><tr>
        <th>Registry ID</th><th>Status</th><th>Started At</th>
        <th class="text-right">Source Rows</th><th class="text-right">Delta</th>
        <th class="text-right">Ingested</th><th class="text-right">Duration (s)</th>
        <th>Error</th>
      </tr></thead>
      <tbody id="runs-body"><tr><td colspan="8" class="loading"><span class="spinner"></span>Loading runs…</td></tr></tbody>
    </table>
  </div>

  <!-- Tables registry -->
  <div class="section-title">Registered Tables — Current Watermarks</div>
  <div class="table-wrap">
    <table id="tables-table">
      <thead><tr>
        <th>Registry ID</th><th>Source</th><th>Target</th>
        <th>Watermark Col</th><th>Last Successful Run</th>
        <th>Lock Status</th><th>Active</th>
      </tr></thead>
      <tbody id="tables-body"><tr><td colspan="7" class="loading"><span class="spinner"></span>Loading tables…</td></tr></tbody>
    </table>
  </div>

  <!-- Quarantine summary -->
  <div class="section-title">Quarantine — Pending Rows by Table</div>
  <div class="table-wrap">
    <table id="quarantine-table">
      <thead><tr><th>Registry ID</th><th class="text-right">Pending Count</th><th>Oldest Pending</th></tr></thead>
      <tbody id="quarantine-body"><tr><td colspan="3" class="loading"><span class="spinner"></span>Loading quarantine…</td></tr></tbody>
    </table>
  </div>

</main>

<!-- Detail side panel -->
<div id="detail-panel">
  <button class="close-btn" onclick="closeDetail()">✕</button>
  <div id="detail-content"></div>
</div>

<script>
const API = '';  // same-origin

function fmt(val, fallback = '—') {
  if (val === null || val === undefined) return fallback;
  return val;
}

function fmtNum(val) {
  if (val === null || val === undefined) return '—';
  return Number(val).toLocaleString();
}

function fmtTs(val) {
  if (!val) return '—';
  return new Date(val).toLocaleString('en-GB', { hour12: false });
}

function statusBadge(status) {
  const map = {
    'SUCCESS':      'badge-success',
    'FAILED':       'badge-failed',
    'DQ_BLOCKED':   'badge-dqblock',
    'RUNNING':      'badge-running',
    'LOCK_CONFLICT':'badge-lock',
    'PENDING':      'badge-pending',
  };
  return `<span class="badge ${map[status] || ''}">${status || '—'}</span>`;
}

// ── KPIs ─────────────────────────────────────────────────────────────────────

async function loadSummary() {
  try {
    const r = await fetch(API + '/api/summary');
    const d = await r.json();
    document.getElementById('kpi-row').innerHTML = `
      <div class="kpi blue">
        <div class="kpi-label">Total Runs Today</div>
        <div class="kpi-value">${fmtNum(d.total_runs)}</div>
        <div class="kpi-sub">${fmtNum(d.tables_processed)} tables</div>
      </div>
      <div class="kpi success">
        <div class="kpi-label">Successful</div>
        <div class="kpi-value">${fmtNum(d.successful)}</div>
        <div class="kpi-sub">${d.total_runs ? Math.round(d.successful/d.total_runs*100) : 0}% success rate</div>
      </div>
      <div class="kpi danger">
        <div class="kpi-label">Failed</div>
        <div class="kpi-value">${fmtNum(d.failed)}</div>
        <div class="kpi-sub">${fmtNum(d.dq_blocked)} DQ blocked</div>
      </div>
      <div class="kpi warn">
        <div class="kpi-label">Lock Conflicts</div>
        <div class="kpi-value">${fmtNum(d.lock_conflicts)}</div>
        <div class="kpi-sub">concurrency conflicts</div>
      </div>
      <div class="kpi blue">
        <div class="kpi-label">Rows Ingested</div>
        <div class="kpi-value">${fmtNum(d.total_ingested)}</div>
        <div class="kpi-sub">${fmtNum(d.total_delta_rows)} delta detected</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Avg Duration</div>
        <div class="kpi-value" style="font-size:28px">${fmt(d.avg_duration_seconds, '—')}s</div>
        <div class="kpi-sub">max ${fmt(d.max_duration_seconds, '—')}s</div>
      </div>
    `;
  } catch (e) {
    document.getElementById('kpi-row').innerHTML = `<div class="loading" style="color:#b91c1c">Failed to load KPIs: ${e}</div>`;
  }
}

// ── Runs table ────────────────────────────────────────────────────────────────

async function loadRuns() {
  try {
    const r = await fetch(API + '/api/runs?hours=24&limit=50');
    const rows = await r.json();
    const tbody = document.getElementById('runs-body');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="loading text-muted">No runs in last 24 h</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(row => `
      <tr onclick="loadRunDetail('${row.run_id}')" title="Click for detail">
        <td class="mono">${fmt(row.registry_id)}</td>
        <td>${statusBadge(row.status)}</td>
        <td>${fmtTs(row.started_at)}</td>
        <td class="text-right">${fmtNum(row.source_row_count)}</td>
        <td class="text-right">${fmtNum(row.delta_count)}</td>
        <td class="text-right">${fmtNum(row.ingested_count)}</td>
        <td class="text-right">${fmt(row.duration_seconds, '—')}</td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;color:#b91c1c">
          ${row.error_message ? row.error_message.substring(0,80) + (row.error_message.length > 80 ? '…' : '') : ''}
        </td>
      </tr>
    `).join('');
  } catch (e) {
    document.getElementById('runs-body').innerHTML = `<tr><td colspan="8" style="color:#b91c1c;padding:16px">Error: ${e}</td></tr>`;
  }
}

// ── Tables ───────────────────────────────────────────────────────────────────

async function loadTables() {
  try {
    const r = await fetch(API + '/api/tables');
    const rows = await r.json();
    const tbody = document.getElementById('tables-body');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="loading text-muted">No active tables registered</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(row => `
      <tr>
        <td class="mono">${fmt(row.registry_id)}</td>
        <td class="mono" style="font-size:11px">${fmt(row.source_project)}.${fmt(row.source_dataset)}.${fmt(row.source_table)}</td>
        <td class="mono" style="font-size:11px">${fmt(row.target_project)}.${fmt(row.target_dataset)}.${fmt(row.target_table)}</td>
        <td class="mono">${fmt(row.watermark_col)}</td>
        <td>${fmtTs(row.last_successful_run)}</td>
        <td>${statusBadge(row.lock_status)}</td>
        <td>${row.is_active ? '<span class="badge badge-active">ACTIVE</span>' : '<span class="badge badge-inactive">INACTIVE</span>'}</td>
      </tr>
    `).join('');
  } catch (e) {
    document.getElementById('tables-body').innerHTML = `<tr><td colspan="7" style="color:#b91c1c;padding:16px">Error: ${e}</td></tr>`;
  }
}

// ── Quarantine ────────────────────────────────────────────────────────────────

async function loadQuarantine() {
  try {
    const r = await fetch(API + '/api/quarantine/summary');
    const rows = await r.json();
    const tbody = document.getElementById('quarantine-body');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="3" class="loading text-muted" style="color:#15803d">✓ No pending quarantine rows</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(row => `
      <tr>
        <td class="mono">${fmt(row.registry_id)}</td>
        <td class="text-right"><span class="badge badge-pending">${fmtNum(row.pending_count)}</span></td>
        <td>${fmtTs(row.oldest_pending)}</td>
      </tr>
    `).join('');
  } catch (e) {
    document.getElementById('quarantine-body').innerHTML = `<tr><td colspan="3" style="color:#b91c1c;padding:16px">Error: ${e}</td></tr>`;
  }
}

// ── Run detail panel ──────────────────────────────────────────────────────────

async function loadRunDetail(runId) {
  const panel = document.getElementById('detail-panel');
  const content = document.getElementById('detail-content');
  content.innerHTML = '<div class="loading"><span class="spinner"></span>Loading detail…</div>';
  panel.classList.add('open');

  try {
    const [runResp, dqResp] = await Promise.all([
      fetch(API + '/api/runs/' + runId),
      fetch(API + '/api/dq/' + runId)
    ]);
    const run = await runResp.json();
    const dqErrors = await dqResp.json();

    const dqHtml = dqErrors.length ? dqErrors.map(e => `
      <div class="dq-rule-card">
        <div class="dq-rule-name">${statusBadge(e.action_on_fail)} &nbsp; ${fmt(e.rule_id)}</div>
        <div class="dq-rule-detail">Failed rows: <strong>${fmtNum(e.failed_row_count)}</strong>
          &nbsp;·&nbsp; Rate: <strong>${fmt(e.failure_rate_pct, '—')}%</strong>
          &nbsp;·&nbsp; Logged: ${fmtTs(e.logged_at)}
        </div>
        ${e.rule_sql_used ? `<div class="mono" style="margin-top:6px;font-size:11px;background:#fef9c3;padding:6px;border-radius:4px;overflow-x:auto">${e.rule_sql_used}</div>` : ''}
      </div>
    `).join('') : '<div style="color:#15803d;font-size:13px">✓ No DQ rule failures for this run</div>';

    content.innerHTML = `
      <h2 class="mono" style="font-size:14px;margin-bottom:4px">${fmt(run.registry_id)}</h2>
      <div style="margin-bottom:20px">${statusBadge(run.status)}</div>

      <div class="detail-row"><div class="detail-label">Run ID</div><div class="detail-value mono">${fmt(run.run_id)}</div></div>
      <div class="detail-row"><div class="detail-label">DAG Run ID</div><div class="detail-value mono" style="font-size:11px">${fmt(run.dag_run_id)}</div></div>
      <div class="detail-row"><div class="detail-label">Started</div><div class="detail-value">${fmtTs(run.started_at)}</div></div>
      <div class="detail-row"><div class="detail-label">Ended</div><div class="detail-value">${fmtTs(run.ended_at)}</div></div>
      <div class="detail-row"><div class="detail-label">Duration</div><div class="detail-value">${fmt(run.duration_seconds, '—')} s</div></div>
      <div class="detail-row"><div class="detail-label">Watermark From</div><div class="detail-value">${fmtTs(run.watermark_from)}</div></div>
      <div class="detail-row"><div class="detail-label">Watermark To</div><div class="detail-value">${fmtTs(run.watermark_to)}</div></div>
      <div class="detail-row"><div class="detail-label">Source Rows</div><div class="detail-value">${fmtNum(run.source_row_count)}</div></div>
      <div class="detail-row"><div class="detail-label">Delta Count</div><div class="detail-value">${fmtNum(run.delta_count)}</div></div>
      <div class="detail-row"><div class="detail-label">Ingested</div><div class="detail-value">${fmtNum(run.ingested_count)}</div></div>
      <div class="detail-row"><div class="detail-label">DQ Pass / Warn / Fail</div>
        <div class="detail-value">
          <span class="badge badge-success">${fmt(run.dq_rules_passed, 0)} pass</span>
          <span class="badge badge-lock" style="margin-left:4px">${fmt(run.dq_rules_warned, 0)} warn</span>
          <span class="badge badge-failed" style="margin-left:4px">${fmt(run.dq_rules_failed, 0)} fail</span>
        </div>
      </div>
      ${run.error_message ? `
        <div style="margin-top:8px"><div class="detail-label">Error</div>
        <div class="error-box">${run.error_message}</div></div>` : ''}

      <div class="section-title" style="margin-top:24px;font-size:14px">DQ Rule Failures</div>
      ${dqHtml}
    `;
  } catch (e) {
    content.innerHTML = `<div style="color:#b91c1c">Failed to load detail: ${e}</div>`;
  }
}

function closeDetail() {
  document.getElementById('detail-panel').classList.remove('open');
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────

async function loadAll() {
  document.getElementById('last-refresh').textContent = 'Refreshing…';
  await Promise.all([loadSummary(), loadRuns(), loadTables(), loadQuarantine()]);
  document.getElementById('last-refresh').textContent =
    'Last refreshed: ' + new Date().toLocaleTimeString('en-GB', { hour12: false });
}

loadAll();
// Auto-refresh every 60 seconds
setInterval(loadAll, 60_000);

// Close detail panel on Escape
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDetail(); });
</script>
</body>
</html>
""".replace(
    "{project}", GCP_PROJECT
).replace(
    "{dataset}", FRAMEWORK_DATASET
)


@app.get("/", response_class=HTMLResponse, tags=["dashboard"])
def dashboard_html() -> HTMLResponse:
    """
    Main HTML dashboard page.
    Served as a single-page app — all data is fetched via the /api/* endpoints.
    Auto-refreshes every 60 seconds.
    """
    return HTMLResponse(content=_HTML_DASHBOARD)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point — for local development
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    log.info("Starting dashboard on port %d", port)
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=os.environ.get("LOG_LEVEL", "INFO") == "DEBUG",
        log_level=LOG_LEVEL.lower(),
    )
