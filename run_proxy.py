#!/usr/bin/env python3
"""
Launch the zarr data passport proxy.

    python run_proxy.py [--host HOST] [--port PORT] [--store-dir DIR]

Each *.zarr directory inside STORE_DIR is served under its own name:
    xr.open_zarr("http://localhost:8000/icos-fluxnet.zarr/", group="SE-Svb")
    xr.open_zarr("http://localhost:8000/icos-atmosphere.zarr/", group="BE-Bra")
"""
import argparse
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="zarr data passport proxy")
    parser.add_argument("--host",      default="0.0.0.0", help="Bind host")
    parser.add_argument("--port",      default=8000, type=int, help="Bind port")
    parser.add_argument("--store-dir", default=".",
                        help="Directory containing one or more *.zarr stores (default: .)")
    args = parser.parse_args()

    os.environ.setdefault("ZARR_STORE_DIR", args.store_dir)

    uvicorn.run(
        "zarr_proxy.main:app",
        host=args.host,
        port=args.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
