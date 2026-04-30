# Plan — Dokku deployment of zarr_proxy + shiny_fluxnet_netcdf_browser

## Goal

Run two services together on a Dokku host:

1. **`zarr-proxy`** — `run_proxy.py` (FastAPI + uvicorn, port 8080) serving
   `icos-fluxnet.zarr` and `icos-obspack.zarr`, plus `combine_to_dim.py`'s
   sibling combined-view groups, and minting data passports.
2. **`fluxnet-browser`** — the Shiny app from
   `https://github.com/ICOS-Carbon-Portal/shiny_fluxnet_netcdf_browser`
   (`app_zarr.py`, port 8000) reading the proxy over HTTP.

Public traffic hits the Shiny app; the proxy is internal and only the
Shiny app talks to it (plus optional public passport endpoints).

## Dokku reality check

Dokku is built around **one container per app**. There's no `docker
compose up` — each Dokku app is its own deploy. The two services share
nothing except the Docker network if you put them in the same Dokku
network.

So the deployment is **two Dokku apps**, not a single compose file:

```
dokku apps:create zarr-proxy
dokku apps:create fluxnet-browser
dokku network:create icos-internal
dokku network:set zarr-proxy attach-post-create icos-internal
dokku network:set fluxnet-browser attach-post-create icos-internal
```

Inside `icos-internal`, the proxy is reachable as
`http://zarr-proxy.web:8080`. The Shiny app sets
`ICOS_ZARR_STORE=http://zarr-proxy.web:8080/icos-fluxnet.zarr`.

A *local* `docker-compose.yml` is still useful for development and CI:
the same two images, the same env vars, but bound to localhost. We'll
ship both: a compose file for dev + Dokku setup commands for prod.

## Two repos, two images

The Shiny app already has a Dockerfile in its repo
(`https://github.com/ICOS-Carbon-Portal/shiny_fluxnet_netcdf_browser`).
The default CMD runs `app.py` (the legacy NetCDF mode); for our use
we override to `app_zarr.py`.

The proxy doesn't have one yet — needs a new `Dockerfile` next to
`run_proxy.py`.

### `Dockerfile` for zarr_proxy (new, in this repo)

```dockerfile
FROM python:3.12-slim AS build
RUN pip install --no-cache-dir uv
COPY requirements.txt /tmp/
RUN uv pip install --system --no-cache-dir -r /tmp/requirements.txt

FROM python:3.12-slim
RUN useradd -m -u 1000 appuser
COPY --from=build /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=build /usr/local/bin /usr/local/bin
WORKDIR /app
COPY zarr_proxy/ ./zarr_proxy/
COPY run_proxy.py ./
USER appuser
EXPOSE 8080
ENV ZARR_STORE_DIR=/data \
    PASSPORT_DIR=/data/passports \
    SESSION_TIMEOUT_SEC=300
CMD ["python", "-u", "run_proxy.py", "--host", "0.0.0.0", "--port", "8080", "--store-dir", "/data"]
```

Note: the requirements.txt in this repo is heavy (cartopy, ipywidgets,
matplotlib, ipympl) because notebooks pull them in. The proxy itself
only needs `fastapi uvicorn[standard] fsspec aiohttp xarray zarr<3
numpy pandas`. Either:

- **Slim Dockerfile**: copy only the proxy-needed lines from
  requirements into a `requirements-server.txt`. Smaller image,
  faster build. Recommended.
- Reuse the full requirements.txt — bigger image but one source of
  truth.

→ **Add `requirements-server.txt`** with:
```
fastapi
uvicorn[standard]
fsspec
aiohttp
xarray
zarr<3
numpy
pandas
```

### Shiny Dockerfile override

Don't edit the upstream repo. Either fork or layer on top:

```dockerfile
FROM ghcr.io/icos-carbon-portal/shiny_fluxnet_netcdf_browser:latest
ENV ICOS_ZARR_STORE=http://zarr-proxy.web:8080/icos-fluxnet.zarr \
    ICOS_OBSPACK_STORE=http://zarr-proxy.web:8080/icos-obspack.zarr \
    PORT=8000
CMD ["sh", "-c", "shiny run app_zarr.py --host 0.0.0.0 --port ${PORT}"]
```

(Confirms that the upstream image is published. If not, add a `git
submodule` of the shiny repo here and Dockerize it from this repo's
build.)

## `docker-compose.yml` for development

```yaml
services:
  zarr-proxy:
    build: .
    image: zarr-proxy:dev
    environment:
      ZARR_STORE_DIR: /data
      PASSPORT_DIR:   /data/passports
      SESSION_TIMEOUT_SEC: "300"
    volumes:
      - ./icos-fluxnet.zarr:/data/icos-fluxnet.zarr:ro
      - ./icos-obspack.zarr:/data/icos-obspack.zarr:ro
      - ./passports:/data/passports
    ports:
      - "8080:8080"

  fluxnet-browser:
    build:
      context: ../shiny_fluxnet_netcdf_browser
      dockerfile: Dockerfile
    image: fluxnet-browser:dev
    environment:
      ICOS_ZARR_STORE:    http://zarr-proxy:8080/icos-fluxnet.zarr
      ICOS_OBSPACK_STORE: http://zarr-proxy:8080/icos-obspack.zarr
      PORT: "8000"
    command: sh -c "shiny run app_zarr.py --host 0.0.0.0 --port 8000"
    ports:
      - "8000:8000"
    depends_on: [zarr-proxy]
```

`./icos-fluxnet.zarr:/data/icos-fluxnet.zarr:ro` — readonly because the
proxy never writes to the zarr store, only to `passports/`.

## Dokku deployment steps

### 1. Set up the proxy app

```bash
# Host preparation: symlink (or bind-mount) the canonical zarr dir.
sudo mkdir -p /srv/icos
sudo ln -s /tank/flexextract/zarr /srv/icos/zarr

dokku apps:create zarr-proxy
dokku ports:set zarr-proxy http:8080:8080
dokku storage:mount zarr-proxy /srv/icos/zarr:/data
dokku config:set zarr-proxy \
    ZARR_STORE_DIR=/data \
    PASSPORT_DIR=/data/passports \
    SESSION_TIMEOUT_SEC=300
# For PID minting / CP upload — leave unset for the dev deployment
# dokku config:set zarr-proxy HANDLE_TOKEN=... HANDLE_PREFIX=11676 ...

# Public domain (decision: open access for now)
dokku domains:add zarr-proxy zarr.icos-cp.eu       # adjust if needed
dokku letsencrypt:enable zarr-proxy

# Disable Nginx response buffering so zarr-chunk fetches stream cleanly
dokku nginx:set zarr-proxy proxy-buffering off

# Push:
git remote add dokku-proxy dokku@<host>:zarr-proxy
git push dokku-proxy main:master
```

The zarr stores are already on `fsicos4:/tank/flexextract/zarr/` — the
symlink at `/srv/icos/zarr` points there, so no rsync is needed unless
the Dokku host is different from `fsicos4`.

### 2. Set up the Shiny app

```bash
dokku apps:create fluxnet-browser
dokku ports:set fluxnet-browser http:80:8000      # public on 80
dokku config:set fluxnet-browser \
    ICOS_ZARR_STORE=http://zarr-proxy.web:8080/icos-fluxnet.zarr \
    ICOS_OBSPACK_STORE=http://zarr-proxy.web:8080/icos-obspack.zarr \
    PORT=8000

dokku domains:add fluxnet-browser icos-fluxnet-browser.icos-ri.eu
dokku letsencrypt:enable fluxnet-browser

# In the fork of ICOS-Carbon-Portal/shiny_fluxnet_netcdf_browser:
git remote add dokku dokku@<host>:fluxnet-browser
git push dokku main:master
```

The fork must override the upstream `CMD` to run `app_zarr.py` instead
of `app.py`. Cleanest way: a `Procfile` at repo root:

```
web: shiny run app_zarr.py --host 0.0.0.0 --port ${PORT}
```

(Dokku honours the Procfile even when a Dockerfile is present, by
appending it as the container `CMD`.) Otherwise add a one-line
`Dockerfile.dokku` that just sets the new CMD.

### 3. Connect the two apps over an internal network

```bash
dokku network:create icos-internal
dokku network:set zarr-proxy attach-post-create icos-internal
dokku network:set fluxnet-browser attach-post-create icos-internal
dokku ps:rebuild zarr-proxy
dokku ps:rebuild fluxnet-browser
```

Inside `icos-internal`, each app is reachable as
`http://<app-name>.web:<port>` (Dokku's standard naming). The
`ICOS_ZARR_STORE` URL above uses that.

### 4. Public proxy

Per decision 2, we keep the proxy public, no auth.
Already enabled in step 1 (`dokku domains:add zarr-proxy …`).

If abuse becomes an issue later, switch to internal-only with:

```bash
dokku domains:disable zarr-proxy
```

…and have the Shiny app reach it only via `http://zarr-proxy.web:8080/`
on the internal network.

## Storage strategy

One mount at `/srv/icos/zarr` (host) → `/data` (container, read-write
since the proxy writes passports under it).

| Path on host | Purpose |
|---|---|
| `/srv/icos/zarr/icos-fluxnet.zarr/` | Read by proxy (Fluxnet) |
| `/srv/icos/zarr/icos-obspack.zarr/` | Read by proxy (Obspack) |
| `/srv/icos/zarr/passports/` | Written by proxy on every session close |

`/srv/icos/zarr` is a symlink (or bind mount) to
`fsicos4:/tank/flexextract/zarr/`, so existing data is reused as-is.

When new combined views land (`combine_to_dim.py` rebuild) or
metadata patches (`patch_fluxnet_zarr_attrs.py`):

```bash
# Push only the metadata files + new sibling groups (~ a few MB)
rsync -av --include='*/' --include='.zattrs' --include='.zmetadata' \
          --exclude='*' \
      icos-fluxnet.zarr/  fsicos4:/tank/flexextract/zarr/icos-fluxnet.zarr/

rsync -av icos-fluxnet.zarr/_combined/  \
      fsicos4:/tank/flexextract/zarr/icos-fluxnet.zarr/_combined/
```

No proxy restart needed — it re-reads from disk on every request.

## Risks / gotchas

1. **Dockerfile location**: The `Dockerfile` for the proxy lives in this
   repo's root. Dokku's buildpack auto-detects `Dockerfile` and uses
   it. If a `Procfile` exists, it overrides; we don't have one.

2. **`zarr<3`** in the proxy image. This repo's runtime requires zarr
   v2; if the base Debian/Python image has zarr 3 cached somehow, our
   pin must explicitly say `zarr<3`. Already in `requirements.txt`.

3. **Image size**: Cartopy + matplotlib + ipympl etc. are unnecessary
   for the proxy. Slim `requirements-server.txt` saves ~400 MB.

4. **Internal network discovery**: Dokku's network aliases
   (`<app>.web`) require all apps to be on the same network. Single
   `network:set ... attach-post-create` per app, then
   `dokku ps:rebuild` to pick up the change.

5. **PASSPORT_DIR persistence**: Make sure the storage mount is
   writable by uid 1000 (the `appuser`). Dokku usually handles this if
   the dir is created via `dokku storage:mount` rather than direct
   `mkdir`.

6. **Reverse-proxy buffering**: Dokku-Nginx's default `proxy_buffering on`
   chunks responses, which can slow down zarr-chunk fetches. For the
   proxy app, set `dokku nginx:set zarr-proxy proxy-buffering off`.

7. **Shiny app upstream**: assumes the upstream Dockerfile is good.
   Right now it runs `app.py`, not `app_zarr.py` — we override CMD.
   If upstream renames things, the override will break; pin to a
   commit/tag.

## Decisions (locked 2026-04-29)

- [x] **Host data path**: `/srv/icos/zarr/` — symlink target of
      `fsicos4:/tank/flexextract/zarr/` so the existing on-server stores
      are reachable directly. Mount this into the proxy container as
      `/data` (read-only).
      ```
      ln -s /tank/flexextract/zarr /srv/icos/zarr
      ```
      Or a bind mount if a real symlink isn't desirable on the Dokku
      host.

- [x] **Public proxy domain**: **Yes, public, no auth** for now.
      Passports + chunk fetches are open. Add auth only if abuse
      becomes an issue. `domains:add zarr.icos-cp.eu` (or a
      subdomain you pick — confirm before deploy).

- [x] **Image size**: **slim** — new `requirements-server.txt` with
      only `fastapi uvicorn[standard] fsspec aiohttp xarray zarr<3
      numpy pandas`. Cartopy/ipywidgets/matplotlib stay client-side
      only.

- [x] **Shiny image source**: **A — build from a fork in the
      ICOS-Carbon-Portal org**. Push directly to Dokku from the fork:
      ```
      git remote add dokku dokku@<host>:fluxnet-browser
      git push dokku main:master
      ```
      Override the upstream Dockerfile's `CMD` to `app_zarr.py` either
      via an `app.json` / `Procfile` in the fork, or by adding a
      one-line `Dockerfile` extension layer in the fork.

- [x] **Public hostname**: `icos-fluxnet-browser.icos-ri.eu`
      → `dokku domains:add fluxnet-browser icos-fluxnet-browser.icos-ri.eu`
      → `dokku letsencrypt:enable fluxnet-browser`

## Estimated effort

- New `Dockerfile` + `requirements-server.txt` + dev `docker-compose.yml`:
  half a day (test locally before pushing to Dokku)
- Dokku setup commands + first deploy: ~2 hours per app
- DNS / TLS / passport-landing-page wiring: ~2 hours
- README / runbook section: 1 hour

Total: ~1–1.5 days.

## Future work (not in scope here)

- **CI**: GitHub Actions to build and push the proxy image to GHCR on
  every commit, then `dokku git:from-image zarr-proxy
  ghcr.io/.../zarr-proxy:latest` instead of buildpack-from-source. Faster
  redeploys.
- **Passport landing page**: serve `passports/*.jsonld` and the
  HTML-rendered version (`zarr_proxy/render_passport.py`) under a public
  path so a Handle PID resolves to a human-readable summary.
- **Auth**: if the proxy goes public, add a Bearer-token check on the
  `POST /{store}/session/close` endpoint so passport mints are
  rate-limited / authenticated.
- **Logs / Matomo**: route container logs to ICOS' central
  observability; populate `MATOMO_*` env vars to track usage events.
