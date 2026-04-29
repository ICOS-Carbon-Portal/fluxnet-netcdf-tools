# Plan — restore station metadata in fluxnet zarr store

## Bug

Per-station group `.zattrs` in `icos-fluxnet.zarr` only contains `_provenance`.
The geospatial / station metadata (`geospatial_lat`, `geospatial_lon`,
`station_elevation`, `country`, `ecosystem`, `climate_zone`, `site_id`, etc.)
that the NetCDF path attaches via `fetch_icos_station_meta()` is missing.

Effect: the new `nl_2024_minimal.ipynb` cannot filter fluxnet sites by
lat/lon. The shiny `app_zarr.py` had to fall back to constructing a station
landing-page URL from `site_id` because the lat/lon attrs aren't there
either.

## Root cause

In `fluxnet_restructure.py::_write_group_to_zarr` (lines 1756–1766), `_flush()`
does:

```python
ds.attrs.update(global_attrs if first else {})
ds.to_zarr(store_path, group=group_path, mode="w" if first else "a", …)
```

- First call: `mode="w"`, `ds.attrs` carries `global_attrs` → group `.zattrs`
  written correctly.
- Subsequent calls (LE, soil, profile, 1-D): `mode="a"`, `ds.attrs={}`. xarray's
  `to_zarr(mode="a")` **rewrites** the group `.zattrs` from the new dataset's
  attrs (empty) — wiping everything except keys it doesn't touch. We confirmed
  this exact behaviour in the obspack work and worked around it there with an
  in-memory accumulator.

After all the `_flush` calls, only the **last** `to_zarr` write determines
the group `.zattrs`, which is empty.

Then `fluxnet2zarr.py::_update_provenance` later sets the `_provenance` key
— that's the only thing that survives.

## Fix

Mirror the obspack pattern: keep `global_attrs` in memory, write them to the
group `.zattrs` **after** all `to_zarr` calls finish (in `_write_group_to_zarr`,
or once at the end of `restructure_to_zarr`). Two equivalent options:

### Option A (preferred) — re-apply attrs after flush in `_write_group_to_zarr`

In `_write_group_to_zarr` after the final `_flush()` call:

```python
import zarr as _zarr
grp = _zarr.open_group(store_path, mode="a")[group_path]
for k, v in global_attrs.items():
    grp.attrs[k] = v
```

Pros: localised to the function that owns `global_attrs`. Each sub-group
(`fluxnet_dd`, `fluxnet_mm`, …) independently gets its station metadata
written, matching the NetCDF behaviour.

Cons: opens the zarr group twice per write (once via xarray, once to fix
attrs).

### Option B — accumulate in `restructure_to_zarr`, write at the end

In `restructure_to_zarr`, after the loop that writes the root + all
sub-groups, open the station group and re-apply `global_attrs` to it (and
optionally also to each sub-group).

Pros: one place for the fix.

Cons: the function currently doesn't have a single "after all writes done"
hook — would need a small refactor.

→ **Go with A.** It's the smallest change.

## Implementation

1. Add 5 lines at the end of `_write_group_to_zarr` (just before it returns):

   ```python
   # to_zarr(mode="a") wipes group .zattrs — re-apply global_attrs after
   # all flushes are done, so every sub-group keeps its station metadata.
   import zarr as _zarr
   _grp = _zarr.open_group(store_path, mode="a")[group_path]
   for k, v in global_attrs.items():
       _grp.attrs[k] = v
   ```

2. Add a comment to `_flush` explaining that the `global_attrs if first else {}`
   trick is no longer load-bearing — the post-write re-apply is what matters.

3. **Re-ingest one station** to validate (`fluxnet2zarr.py populate
   <DOI> --station NL-Loo`) and confirm `geospatial_lat` etc. appear in
   `.zattrs`.

4. Once verified, **re-ingest all 35 stations** to refresh the on-disk store.
   No data files change — only the `.zattrs` per group. Could shortcut by
   writing a small one-off script that walks every station + sub-group, fetches
   `station_meta` via `fetch_icos_station_meta(site_id)`, and writes the attrs.
   But a clean re-ingest is simpler and we already proved it's reliable.

5. After re-ingest, also run `zarr.consolidate_metadata` per station + at the
   store root (`fluxnet2zarr.py` already does this). The consolidation captures
   the freshly written attrs.

6. Update `nl_2024_minimal.ipynb` to query the now-present `geospatial_lat` /
   `geospatial_lon` attrs.

## Verification

```python
import zarr
g = zarr.open_group("icos-fluxnet.zarr", mode="r")["NL-Loo"]
attrs = dict(g.attrs)
assert "geospatial_lat" in attrs
assert "geospatial_lon" in attrs
assert "station_elevation" in attrs
assert "_provenance" in attrs   # still there
print(attrs.keys())
```

## Open question

The bug also affects `obspack` if there's any `to_zarr(mode="a")` call after
the initial group-attrs write. Already worked around there via the
`station_attrs[sid]` accumulator, so no change needed for obspack.

## Estimated effort

- 5-line code change: 5 minutes
- Re-ingest 35 stations: ~15 minutes (faster than original because data
  already on disk if `--keep-csv` was used; otherwise full re-download
  ~30 minutes)
- Verification + nl_2024_minimal.ipynb fix: 5 minutes

Total: ~25 minutes worst case.
