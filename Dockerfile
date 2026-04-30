# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS build

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc \
 && rm -rf /var/lib/apt/lists/*

# Install only the proxy's runtime deps (slim — no notebook libs).
COPY requirements-server.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-server.txt


# ── Runtime stage ────────────────────────────────────────────────────────────
FROM python:3.12-slim

# Non-root user with a stable uid that matches Dokku's storage perms.
RUN useradd -m -u 1000 appuser

# Copy installed packages and console scripts from the build stage.
COPY --from=build /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=build /usr/local/bin /usr/local/bin

WORKDIR /app
COPY zarr_proxy/   ./zarr_proxy/
COPY run_proxy.py  ./

# Storage paths — mounted by Dokku as a single volume at /data.
ENV ZARR_STORE_DIR=/data \
    PASSPORT_DIR=/data/passports \
    SESSION_TIMEOUT_SEC=300 \
    PYTHONUNBUFFERED=1

USER appuser
EXPOSE 8080

# Honour Dokku-supplied $PORT if present (Dokku injects it on web procs);
# fall back to 8080 for `docker run` and docker-compose.
CMD ["sh", "-c", "python -u run_proxy.py --host 0.0.0.0 --port ${PORT:-8080} --store-dir ${ZARR_STORE_DIR}"]
