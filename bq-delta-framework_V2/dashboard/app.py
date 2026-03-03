"""
dashboard/app.py
=================
FastAPI dashboard for the BQ Delta Ingestion Framework.
Serves a single-page web UI and 10 REST API endpoints.

All endpoints are read-only (SELECT queries on framework_config tables).
Results are cached in-process with a configurable TTL to reduce BQ costs.

Environment variables
---------------------
GCP_PROJECT         (required)  GCP project ID
FRAMEWORK_DATASET   (optional)  BQ dataset, default: framework_config
BQ_LOCATION         (optional)  BQ location, default: US
CACHE_TTL_SECONDS   (optional)  Cache TTL per endpoint, default: 60
LOG_FORMAT          (optional)  json | text, default: json
LOG_LEVEL           (optional)  DEBUG | INFO | WARNING, default: INFO
PORT                (optional)  HTTP port, default: 8080

Endpoints
---------
GET /                        → Embedded SPA dashboard HTML
GET /health                  → Liveness probe (no BQ call)
GET /api/summary             → Today's KPI aggregates
GET /api/runs                → Last 50 runs in last 24h
GET /api/runs/{run_id}       → Single run detail
GET /api/dq/{run_id}         → DQ failures for a run
GET /api/quarantine          → Quarantine records (filterable)
GET /api/quarantine/summary  → PENDING quarantine counts by table
GET /api/tables              → All active tables with watermark + lock status
GET /api/trend               → Daily run trend data (N days)
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

# ── Logging setup ─────────────────────────────────────────────────────────────

LOG_FORMAT = os.environ.get("LOG_FORMAT", "json")
LOG_LEVEL  = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
    if LOG_FORMAT == "text"
    else '{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

GCP_PROJECT       = os.environ.get("GCP_PROJECT", "")
FRAMEWORK_DATASET = os.environ.get("FRAMEWORK_DATASET", "framework_config")
BQ_LOCATION       = os.environ.get("BQ_LOCATION", "US")
CACHE_TTL         = int(os.environ.get("CACHE_TTL_SECONDS", "60"))

# ── BigQuery client (singleton) ───────────────────────────────────────────────

_bq_client = None
_bq_lock = Lock()


def get_bq_client():
    global _bq_client
    if _bq_client is None:
        with _bq_lock:
            if _bq_client is None:
                from google.cloud import bigquery
                _bq_client = bigquery.Client(
                    project=GCP_PROJECT,
                    location=BQ_LOCATION,
                )
                logger.info("BigQuery client initialised: project=%s", GCP_PROJECT)
    return _bq_client


# ── Simple TTL cache ──────────────────────────────────────────────────────────

_cache: dict[str, tuple[Any, float]] = {}
_cache_lock = Lock()


def _cached(key: str, ttl: int, fn):
    with _cache_lock:
        if key in _cache:
            value, expires = _cache[key]
            if time.time() < expires:
                return value
    result = fn()
    with _cache_lock:
        _cache[key] = (result, time.time() + ttl)
    return result


def _query(sql: str, params=None) -> list[dict]:
    """Execute a BQ query and return list of dicts."""
    from google.cloud import bigquery
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    rows = client.query(sql, job_config=job_config).result()
    return [dict(row) for row in rows]


def _fmt(value) -> str | None:
    """Serialize a value to a JSON-safe string (handles datetime, etc.)."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _serialize(rows: list[dict]) -> list[dict]:
    return [{k: _fmt(v) for k, v in row.items()} for row in rows]


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="BQ Delta Ingestion Dashboard",
    description="Observability API for the BQ Delta Ingestion Framework",
    version="1.0.0",
)


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Liveness probe — no BQ call."""
    return {"status": "ok", "ts": datetime.now(tz=timezone.utc).isoformat()}


# ── Summary KPIs ──────────────────────────────────────────────────────────────

@app.get("/api/summary")
def get_summary():
    """Today's KPI aggregates from run_audit."""

    def fetch():
        sql = f"""
        SELECT
            COUNTIF(status IN ('SUCCESS','NO_DELTA'))                  AS success_count,
            COUNTIF(status = 'FAILED')                                 AS failed_count,
            COUNTIF(status = 'DQ_BLOCKED')                             AS dq_blocked_count,
            COUNTIF(status = 'LOCK_CONFLICT')                          AS lock_conflict_count,
            COUNT(*)                                                   AS total_runs,
            COALESCE(SUM(rows_affected), 0)                            AS total_rows_affected,
            COALESCE(SUM(rows_quarantined), 0)                         AS total_quarantined,
            ROUND(AVG(duration_secs), 1)                               AS avg_duration_secs,
            ROUND(COUNTIF(status IN ('SUCCESS','NO_DELTA'))/COUNT(*)*100, 1) AS success_rate_pct
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.run_audit`
        WHERE DATE(started_at) = CURRENT_DATE()
        """
        rows = _query(sql)
        return _serialize(rows)[0] if rows else {}

    return _cached("summary:today", CACHE_TTL, fetch)


# ── Recent runs ───────────────────────────────────────────────────────────────

@app.get("/api/runs")
def get_runs(
    limit: int = Query(50, ge=1, le=500),
    hours: int = Query(24, ge=1, le=168),
    status: str | None = Query(None),
    registry_id: str | None = Query(None),
):
    """Last N runs in the last H hours, with optional filters."""

    cache_key = f"runs:{limit}:{hours}:{status}:{registry_id}"

    def fetch():
        filters = [f"started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)"]
        if status:
            filters.append(f"status = '{status}'")
        if registry_id:
            filters.append(f"registry_id = '{registry_id}'")
        where = " AND ".join(filters)
        sql = f"""
        SELECT
            run_id, registry_id, dag_run_id, status, load_mode,
            watermark_from, watermark_to,
            rows_extracted, rows_affected, rows_quarantined,
            dq_rules_passed, dq_rules_failed,
            duration_secs, error_message, error_type,
            started_at, ended_at
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.run_audit`
        WHERE {where}
        ORDER BY started_at DESC
        LIMIT {limit}
        """
        return _serialize(_query(sql))

    return _cached(cache_key, CACHE_TTL, fetch)


# ── Single run detail ─────────────────────────────────────────────────────────

@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    """Full detail for a single run."""

    def fetch():
        from google.cloud import bigquery
        sql = f"""
        SELECT *
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.run_audit`
        WHERE run_id = @run_id
        LIMIT 1
        """
        from google.cloud import bigquery
        params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
        rows = _query(sql, params)
        return _serialize(rows)[0] if rows else None

    result = _cached(f"run:{run_id}", 300, fetch)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")
    return result


# ── DQ error log for a run ────────────────────────────────────────────────────

@app.get("/api/dq/{run_id}")
def get_dq_errors(run_id: str):
    """All DQ rule failures for a specific run."""

    def fetch():
        from google.cloud import bigquery
        sql = f"""
        SELECT
            error_id, run_id, registry_id,
            rule_id, rule_name, rule_type, action_on_fail,
            failed_row_count, error_message,
            logged_at
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.dq_error_log`
        WHERE run_id = @run_id
        ORDER BY logged_at ASC
        """
        params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
        return _serialize(_query(sql, params))

    return _cached(f"dq:{run_id}", 120, fetch)


# ── Quarantine records ────────────────────────────────────────────────────────

@app.get("/api/quarantine")
def get_quarantine(
    registry_id: str | None = Query(None),
    status: str = Query("PENDING"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Quarantine records with optional filters."""

    cache_key = f"quarantine:{registry_id}:{status}:{limit}"

    def fetch():
        filters = [f"status = '{status}'"]
        if registry_id:
            filters.append(f"registry_id = '{registry_id}'")
        where = " AND ".join(filters)
        sql = f"""
        SELECT
            quarantine_id, registry_id, run_id, rule_id, rule_name,
            status, quarantined_at, resolved_at, resolved_by, resolution_note,
            JSON_VALUE(payload) AS payload_preview
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.quarantine_records`
        WHERE {where}
        ORDER BY quarantined_at DESC
        LIMIT {limit}
        """
        return _serialize(_query(sql))

    return _cached(cache_key, 30, fetch)


@app.get("/api/quarantine/summary")
def get_quarantine_summary():
    """PENDING quarantine counts grouped by table."""

    def fetch():
        sql = f"""
        SELECT
            registry_id,
            COUNT(*) AS pending_count,
            MIN(quarantined_at) AS oldest_pending,
            MAX(quarantined_at) AS newest_pending
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.quarantine_records`
        WHERE status = 'PENDING'
        GROUP BY registry_id
        ORDER BY pending_count DESC
        """
        return _serialize(_query(sql))

    return _cached("quarantine:summary", CACHE_TTL, fetch)


# ── Table registry + watermarks ───────────────────────────────────────────────

@app.get("/api/tables")
def get_tables(active_only: bool = Query(True)):
    """All tables with current watermark position and lock status."""

    def fetch():
        active_filter = "AND tr.is_active = TRUE" if active_only else ""
        sql = f"""
        SELECT
            tr.registry_id,
            tr.source_project, tr.source_dataset, tr.source_table,
            tr.target_project, tr.target_dataset, tr.target_table,
            tr.watermark_col,
            tr.priority,
            tr.owner_team,
            tr.is_active,
            pc.last_successful_run,
            pc.last_row_count,
            pc.status          AS lock_status,
            pc.locked_at,
            pc.locked_by,
            pc.updated_at      AS pc_updated_at
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.table_registry` tr
        LEFT JOIN `{GCP_PROJECT}.{FRAMEWORK_DATASET}.process_control` pc
          ON tr.registry_id = pc.registry_id
        WHERE TRUE {active_filter}
        ORDER BY tr.priority ASC, tr.registry_id ASC
        """
        return _serialize(_query(sql))

    return _cached(f"tables:{active_only}", 120, fetch)


# ── Run trend ─────────────────────────────────────────────────────────────────

@app.get("/api/trend")
def get_trend(days: int = Query(14, ge=1, le=90)):
    """Daily run counts by status for the last N days — trend chart data."""

    def fetch():
        sql = f"""
        SELECT
            DATE(started_at)                              AS run_date,
            COUNTIF(status IN ('SUCCESS','NO_DELTA'))     AS success_count,
            COUNTIF(status = 'FAILED')                    AS failed_count,
            COUNTIF(status = 'DQ_BLOCKED')                AS dq_blocked_count,
            COUNT(*)                                      AS total_runs,
            COALESCE(SUM(rows_affected), 0)               AS total_rows
        FROM `{GCP_PROJECT}.{FRAMEWORK_DATASET}.run_audit`
        WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        GROUP BY run_date
        ORDER BY run_date ASC
        """
        return _serialize(_query(sql))

    return _cached(f"trend:{days}", 300, fetch)


# ── Embedded SPA dashboard ─────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BQ Delta Framework — Dashboard</title>
<style>
  :root {
    --bg: #0f172a; --surface: #1e293b; --border: #334155;
    --text: #e2e8f0; --text2: #94a3b8; --text3: #475569;
    --green: #4ade80; --red: #f87171; --blue: #60a5fa;
    --yellow: #fbbf24; --purple: #a78bfa; --cyan: #22d3ee;
    --orange: #fb923c;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; min-height: 100vh; }
  .topbar { background: var(--surface); border-bottom: 2px solid var(--border); padding: 14px 28px; display: flex; align-items: center; justify-content: space-between; }
  .topbar h1 { font-size: 17px; font-weight: 700; color: var(--blue); letter-spacing: 0.5px; }
  .topbar .meta { font-size: 12px; color: var(--text3); font-family: monospace; }
  .main { padding: 28px; max-width: 1400px; margin: 0 auto; }
  /* KPI Cards */
  .kpis { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 16px; margin-bottom: 28px; }
  .kpi { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px 18px; }
  .kpi-label { font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: var(--text3); margin-bottom: 8px; }
  .kpi-value { font-size: 28px; font-weight: 800; }
  .kpi.success .kpi-value { color: var(--green); }
  .kpi.fail    .kpi-value { color: var(--red); }
  .kpi.info    .kpi-value { color: var(--blue); }
  .kpi.warn    .kpi-value { color: var(--yellow); }
  .kpi.purple  .kpi-value { color: var(--purple); }
  .kpi.cyan    .kpi-value { color: var(--cyan); }
  /* Section */
  .section-title { font-size: 13px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--text2); margin: 28px 0 12px; border-left: 3px solid var(--blue); padding-left: 12px; }
  /* Table */
  .data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .data-table th { background: var(--surface); color: var(--text3); font-size: 10px; letter-spacing: 1px; text-transform: uppercase; padding: 10px 14px; text-align: left; border-bottom: 2px solid var(--border); }
  .data-table td { padding: 10px 14px; border-bottom: 1px solid var(--border); color: var(--text); vertical-align: middle; }
  .data-table tr:hover td { background: rgba(96,165,250,0.04); }
  /* Badges */
  .badge { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; font-family: monospace; }
  .badge.success  { background: rgba(74,222,128,0.12); color: var(--green); border: 1px solid rgba(74,222,128,0.3); }
  .badge.failed   { background: rgba(248,113,113,0.12); color: var(--red);   border: 1px solid rgba(248,113,113,0.3); }
  .badge.blocked  { background: rgba(251,191,36,0.12); color: var(--yellow); border: 1px solid rgba(251,191,36,0.3); }
  .badge.running  { background: rgba(96,165,250,0.12); color: var(--blue);   border: 1px solid rgba(96,165,250,0.3); }
  .badge.nodelta  { background: rgba(34,211,238,0.12); color: var(--cyan);   border: 1px solid rgba(34,211,238,0.3); }
  /* Loading */
  .loading { color: var(--text3); font-size: 13px; padding: 24px 0; text-align: center; }
  .error   { color: var(--red);   font-size: 13px; padding: 12px; background: rgba(248,113,113,0.08); border-radius: 8px; }
  /* Tabs */
  .tabs { display: flex; gap: 4px; margin-bottom: 20px; border-bottom: 2px solid var(--border); }
  .tab { padding: 8px 18px; cursor: pointer; font-size: 13px; color: var(--text3); border-bottom: 2px solid transparent; margin-bottom: -2px; transition: all 0.15s; }
  .tab.active { color: var(--blue); border-bottom-color: var(--blue); }
  .tab:hover { color: var(--text); }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }
  /* Refresh */
  .toolbar { display: flex; align-items: center; gap: 12px; margin-bottom: 20px; }
  .btn { padding: 6px 14px; border-radius: 6px; border: 1px solid var(--border); background: var(--surface); color: var(--text); font-size: 12px; cursor: pointer; }
  .btn:hover { border-color: var(--blue); color: var(--blue); }
  .auto-refresh { font-size: 12px; color: var(--text3); margin-left: auto; }
  code { font-family: 'JetBrains Mono', monospace; font-size: 12px; background: rgba(255,255,255,0.06); padding: 1px 6px; border-radius: 4px; }
</style>
</head>
<body>
<div class="topbar">
  <h1>⚡ BQ Delta Ingestion Framework</h1>
  <span class="meta" id="last-refresh">Loading...</span>
</div>
<div class="main">
  <div class="toolbar">
    <button class="btn" onclick="loadAll()">⟳ Refresh</button>
    <span class="auto-refresh" id="next-refresh">Auto-refresh in 60s</span>
  </div>

  <!-- KPI strip -->
  <div class="kpis" id="kpi-strip">
    <div class="kpi info"><div class="kpi-label">Total Runs Today</div><div class="kpi-value" id="k-total">—</div></div>
    <div class="kpi success"><div class="kpi-label">Success</div><div class="kpi-value" id="k-success">—</div></div>
    <div class="kpi fail"><div class="kpi-label">Failed</div><div class="kpi-value" id="k-failed">—</div></div>
    <div class="kpi warn"><div class="kpi-label">DQ Blocked</div><div class="kpi-value" id="k-blocked">—</div></div>
    <div class="kpi cyan"><div class="kpi-label">Rows Ingested</div><div class="kpi-value" id="k-rows">—</div></div>
    <div class="kpi purple"><div class="kpi-label">Quarantined</div><div class="kpi-value" id="k-quarantine">—</div></div>
    <div class="kpi info"><div class="kpi-label">Success Rate</div><div class="kpi-value" id="k-rate">—</div></div>
    <div class="kpi info"><div class="kpi-label">Avg Duration</div><div class="kpi-value" id="k-duration">—</div></div>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <div class="tab active" onclick="switchTab('runs')">Recent Runs</div>
    <div class="tab" onclick="switchTab('tables')">Tables</div>
    <div class="tab" onclick="switchTab('quarantine')">Quarantine</div>
  </div>

  <div id="tab-runs" class="tab-panel active">
    <div class="section-title">Recent Pipeline Runs (last 24h)</div>
    <div id="runs-body"><div class="loading">Loading runs...</div></div>
  </div>

  <div id="tab-tables" class="tab-panel">
    <div class="section-title">Registered Tables — Watermark &amp; Lock Status</div>
    <div id="tables-body"><div class="loading">Loading tables...</div></div>
  </div>

  <div id="tab-quarantine" class="tab-panel">
    <div class="section-title">Quarantine Records — PENDING</div>
    <div id="quarantine-body"><div class="loading">Loading quarantine...</div></div>
  </div>
</div>

<script>
function badge(status) {
  const map = {
    SUCCESS:'success', NO_DELTA:'nodelta', FAILED:'failed',
    DQ_BLOCKED:'blocked', LOCK_CONFLICT:'blocked', RUNNING:'running'
  };
  const cls = map[status] || 'running';
  return `<span class="badge ${cls}">${status}</span>`;
}

function num(v) {
  if (v == null) return '—';
  if (v >= 1e6) return (v/1e6).toFixed(1)+'M';
  if (v >= 1e3) return (v/1e3).toFixed(1)+'K';
  return String(v);
}

function fmtTime(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleTimeString(); } catch { return iso; }
}

function fmtDt(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleString(); } catch { return iso; }
}

async function apiFetch(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
  return r.json();
}

async function loadSummary() {
  try {
    const d = await apiFetch('/api/summary');
    document.getElementById('k-total').textContent    = num(d.total_runs) || '0';
    document.getElementById('k-success').textContent  = num(d.success_count) || '0';
    document.getElementById('k-failed').textContent   = num(d.failed_count) || '0';
    document.getElementById('k-blocked').textContent  = num(d.dq_blocked_count) || '0';
    document.getElementById('k-rows').textContent     = num(d.total_rows_affected) || '0';
    document.getElementById('k-quarantine').textContent = num(d.total_quarantined) || '0';
    document.getElementById('k-rate').textContent     = (d.success_rate_pct ?? '—') + '%';
    document.getElementById('k-duration').textContent = d.avg_duration_secs ? d.avg_duration_secs + 's' : '—';
  } catch(e) { console.error('summary error', e); }
}

async function loadRuns() {
  const el = document.getElementById('runs-body');
  try {
    const rows = await apiFetch('/api/runs?limit=50&hours=24');
    if (!rows.length) { el.innerHTML = '<div class="loading">No runs in last 24h</div>'; return; }
    const html = `<table class="data-table">
      <thead><tr>
        <th>Registry ID</th><th>Status</th><th>Mode</th>
        <th>Rows Extracted</th><th>Rows Affected</th><th>Quarantined</th>
        <th>Duration</th><th>Started At</th>
      </tr></thead>
      <tbody>` +
      rows.map(r => `<tr>
        <td><code>${r.registry_id}</code></td>
        <td>${badge(r.status)}</td>
        <td><code>${r.load_mode || '—'}</code></td>
        <td>${num(r.rows_extracted)}</td>
        <td>${num(r.rows_affected)}</td>
        <td>${num(r.rows_quarantined)}</td>
        <td>${r.duration_secs ? r.duration_secs + 's' : '—'}</td>
        <td>${fmtTime(r.started_at)}</td>
      </tr>`).join('') +
      `</tbody></table>`;
    el.innerHTML = html;
  } catch(e) {
    el.innerHTML = `<div class="error">Error loading runs: ${e.message}</div>`;
  }
}

async function loadTables() {
  const el = document.getElementById('tables-body');
  try {
    const rows = await apiFetch('/api/tables');
    if (!rows.length) { el.innerHTML = '<div class="loading">No tables registered</div>'; return; }
    const html = `<table class="data-table">
      <thead><tr>
        <th>Registry ID</th><th>Source Table</th><th>Target Table</th>
        <th>Watermark Col</th><th>Last Watermark</th><th>Last Rows</th>
        <th>Lock Status</th><th>Priority</th>
      </tr></thead>
      <tbody>` +
      rows.map(r => `<tr>
        <td><code>${r.registry_id}</code></td>
        <td><code>${r.source_dataset}.${r.source_table}</code></td>
        <td><code>${r.target_dataset}.${r.target_table}</code></td>
        <td><code>${r.watermark_col || '—'}</code></td>
        <td>${fmtDt(r.last_successful_run)}</td>
        <td>${num(r.last_row_count)}</td>
        <td>${badge(r.lock_status || 'SUCCESS')}</td>
        <td>${r.priority ?? '—'}</td>
      </tr>`).join('') +
      `</tbody></table>`;
    el.innerHTML = html;
  } catch(e) {
    el.innerHTML = `<div class="error">Error loading tables: ${e.message}</div>`;
  }
}

async function loadQuarantine() {
  const el = document.getElementById('quarantine-body');
  try {
    const [records, summary] = await Promise.all([
      apiFetch('/api/quarantine?status=PENDING&limit=100'),
      apiFetch('/api/quarantine/summary'),
    ]);
    if (!records.length) { el.innerHTML = '<div class="loading">No PENDING quarantine records</div>'; return; }
    const html = `<table class="data-table">
      <thead><tr>
        <th>Registry ID</th><th>Rule</th><th>Status</th>
        <th>Quarantined At</th><th>Run ID</th>
      </tr></thead>
      <tbody>` +
      records.map(r => `<tr>
        <td><code>${r.registry_id}</code></td>
        <td>${r.rule_name || r.rule_id}</td>
        <td><span class="badge blocked">PENDING</span></td>
        <td>${fmtDt(r.quarantined_at)}</td>
        <td><code>${(r.run_id || '').slice(0,8)}…</code></td>
      </tr>`).join('') +
      `</tbody></table>`;
    el.innerHTML = html;
  } catch(e) {
    el.innerHTML = `<div class="error">Error loading quarantine: ${e.message}</div>`;
  }
}

function switchTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => t.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  const tabs = ['runs','tables','quarantine'];
  const idx = tabs.indexOf(name);
  document.querySelectorAll('.tab')[idx].classList.add('active');
  document.getElementById('tab-' + name).classList.add('active');
}

async function loadAll() {
  document.getElementById('last-refresh').textContent =
    'Refreshed at ' + new Date().toLocaleTimeString();
  await Promise.all([loadSummary(), loadRuns(), loadTables(), loadQuarantine()]);
}

// Auto refresh every 60s
let countdown = 60;
setInterval(() => {
  countdown--;
  if (countdown <= 0) { countdown = 60; loadAll(); }
  document.getElementById('next-refresh').textContent = `Auto-refresh in ${countdown}s`;
}, 1000);

loadAll();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    """Serve the embedded SPA dashboard."""
    return HTMLResponse(content=DASHBOARD_HTML, status_code=200)


# ── Entry point (for local dev) ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("dashboard.app:app", host="0.0.0.0", port=port, reload=True)
