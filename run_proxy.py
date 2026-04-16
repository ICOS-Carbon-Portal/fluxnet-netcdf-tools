#!/usr/bin/env python3
"""
Launch the zarr data passport proxy.

    python run_proxy.py [--host HOST] [--port PORT] [--store PATH]

Clients connect with:
    xr.open_zarr("http://localhost:8000/")
    ds = xr.open_zarr("http://localhost:8000/", group="SE-Svb")
"""
import argparse
import os
import sys

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="zarr data passport proxy")
    parser.add_argument("--host",  default="0.0.0.0",          help="Bind host")
    parser.add_argument("--port",  default=8000, type=int,      help="Bind port")
    parser.add_argument("--store", default="icos-fluxnet.zarr", help="zarr store path")
    args = parser.parse_args()

    os.environ.setdefault("ZARR_STORE_PATH", args.store)

    uvicorn.run(
        "zarr_proxy.main:app",
        host=args.host,
        port=args.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
