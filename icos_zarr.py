"""
icos_zarr — thin xarray/zarr wrapper for the ICOS zarr data passport proxy.

Usage
-----
    from icos_zarr import open_zarr

    with open_zarr("http://localhost:8000/", group="SE-Svb") as ds:
        nee = ds["NEE"].isel(time=slice(0, 100)).values
    # On exit:
    #   Passport minted : hdl:11676/3f2a1b9c-...
    #   Landing page    : https://meta.icos-cp.eu/objects/...
    #   Saved to        : .passport/20260416T210000_SE-Svb.jsonld  (if save_passport=True)

    # Or without context manager — call close() explicitly:
    ds = open_zarr("http://localhost:8000/", group="SE-Svb")
    nee = ds["NEE"].values
    passport = ds.close()
    print(passport["passport_pid"])
"""
import json
import pathlib
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

import xarray as xr


class ICOSDataset:
    """
    Wraps an xr.Dataset opened from an ICOS zarr proxy server.
    Delegates all attribute access to the underlying dataset, so it behaves
    exactly like xr.Dataset in user code.  Calls POST /session/close on exit.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        proxy_url: str,
        group: str = "",
        save_passport: bool = True,
        passport_dir: str = ".passport",
        verbose: bool = True,
    ) -> None:
        self._ds            = ds
        self._proxy_url     = proxy_url.rstrip("/")
        self._group         = group
        self._save_passport = save_passport
        self._passport_dir  = pathlib.Path(passport_dir)
        self._verbose       = verbose
        self._passport: dict | None = None   # filled by close()

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "ICOSDataset":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def __del__(self) -> None:
        # Safety net: mint passport even if context manager wasn't used and
        # the user forgot to call close() — fires when the object is GC'd.
        if self._passport is None:
            try:
                self.close(verbose=False)
            except Exception:
                pass

    # ── Transparent delegation to xr.Dataset ─────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ds, name)

    def __getitem__(self, key: str) -> Any:
        return self._ds[key]

    def __repr__(self) -> str:
        return repr(self._ds)

    def __contains__(self, key: str) -> bool:
        return key in self._ds

    def __iter__(self):
        return iter(self._ds)

    # ── Session close + passport ──────────────────────────────────────────────

    def close(self, verbose: bool | None = None) -> dict:
        """
        Close the proxy session and mint a data passport.
        Returns the passport info dict from POST /session/close.
        Idempotent — subsequent calls return the cached result.
        """
        if self._passport is not None:
            return self._passport

        self._passport = {}   # mark as attempted even if request fails

        try:
            req  = urllib.request.Request(
                f"{self._proxy_url}/session/close", method="POST"
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                info = json.loads(resp.read())
        except urllib.error.URLError as exc:
            _print(f"[icos_zarr] Could not reach proxy to close session: {exc}")
            return {}
        except Exception as exc:
            _print(f"[icos_zarr] Session close failed: {exc}")
            return {}

        self._passport = info
        be_verbose = self._verbose if verbose is None else verbose

        if be_verbose:
            pid = info.get("passport_pid", "")
            url = info.get("passport_url", "")
            if pid:
                print(f"Passport minted : {pid}")
            if url:
                print(f"Landing page    : {url}")
            if not pid and not url:
                print(
                    f"[icos_zarr] Session closed ({info.get('chunks', 0)} chunks) "
                    f"— Handle/CP not configured, no PID minted."
                )

        if self._save_passport and info.get("passport_pid"):
            self._write_passport(info)

        return info

    def _write_passport(self, info: dict) -> None:
        """Save a small JSON summary next to the working directory."""
        try:
            self._passport_dir.mkdir(parents=True, exist_ok=True)
            ts    = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            label = self._group.replace("/", "_") or "root"
            path  = self._passport_dir / f"{ts}_{label}.json"
            path.write_text(json.dumps(info, indent=2), encoding="utf-8")
            if self._verbose:
                print(f"Saved to        : {path}")
        except Exception as exc:
            _print(f"[icos_zarr] Could not save passport: {exc}")


def _print(msg: str) -> None:
    print(msg)


# ── Public API ────────────────────────────────────────────────────────────────

def open_zarr(
    proxy_url: str,
    group: str = "",
    *,
    save_passport: bool = True,
    passport_dir: str = ".passport",
    verbose: bool = True,
    **xr_kwargs,
) -> ICOSDataset:
    """
    Open a zarr group from an ICOS zarr proxy server.

    Parameters
    ----------
    proxy_url : str
        Base URL of the zarr proxy, e.g. "http://localhost:8000/"
    group : str
        Zarr group path, e.g. "SE-Svb" or "SE-Svb/fluxnet_dd"
    save_passport : bool
        If True (default), save a JSON passport summary to *passport_dir*
        when the session is closed.
    passport_dir : str
        Directory for saved passport files (default: ".passport/")
    verbose : bool
        Print passport PID and landing page on session close (default: True)
    **xr_kwargs
        Extra keyword arguments forwarded to xr.open_zarr()
        (e.g. chunks="auto", decode_timedelta=False).
        consolidated=True is set by default (required for the HTTP store).

    Returns
    -------
    ICOSDataset
        Wraps xr.Dataset; use as a context manager or call .close() manually.

    Examples
    --------
    # Context manager — passport minted automatically on exit
    with open_zarr("http://localhost:8000/", group="SE-Svb") as ds:
        nee = ds["NEE"].isel(time=slice(0, 100)).values

    # Explicit close — useful in scripts / notebooks
    ds = open_zarr("http://localhost:8000/", group="SE-Svb")
    nee = ds["NEE"].values
    passport = ds.close()
    print(passport["passport_pid"])
    """
    xr_kwargs.setdefault("consolidated", True)
    url = proxy_url.rstrip("/")
    ds  = xr.open_zarr(url + "/", group=group or None, **xr_kwargs)
    return ICOSDataset(
        ds,
        proxy_url=url,
        group=group,
        save_passport=save_passport,
        passport_dir=passport_dir,
        verbose=verbose,
    )
