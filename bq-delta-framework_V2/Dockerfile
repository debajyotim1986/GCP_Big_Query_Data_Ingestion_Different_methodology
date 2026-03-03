# ── Stage 1: Builder — install all production deps via UV ────────────────────
FROM python:3.11-slim AS builder

ENV UV_NO_CACHE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install uv package manager
RUN pip install uv --no-cache-dir

# Copy lockfile + pyproject FIRST — Docker layer cache skips reinstall if unchanged
COPY pyproject.toml uv.lock ./

# Install framework core + dashboard group, skip dev tools
RUN uv sync --frozen --no-group dev --no-install-project


# ── Stage 2: Runtime — minimal final image ───────────────────────────────────
FROM python:3.11-slim AS runtime

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

# Copy venv from builder — no pip or uv binary in final image
COPY --from=builder /app/.venv /app/.venv

# Copy application source (framework engine + dashboard)
COPY framework/  ./framework/
COPY dashboard/  ./dashboard/

# Run as non-root for Cloud Run security best practice
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# Cloud Run injects PORT env var — uvicorn reads it
EXPOSE 8080

CMD ["uvicorn", "dashboard.app:app",
     "--host",      "0.0.0.0",
     "--port",      "8080",
     "--workers",   "2",
     "--log-level", "info"]
