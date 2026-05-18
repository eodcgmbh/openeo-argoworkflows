import base64
import functools
import logging
import numpy as np
import os
import pyproj
import pystac_client
import re
import requests
import rasterio
import rioxarray
import tempfile
import xarray as xr

from odc.stac import stac_load
from pathlib import Path
from pystac.extensions import raster
from typing import Optional, Union
from openeo_processes_dask_slim.process_implementations.data_model import RasterCube
from openeo_processes_dask_slim.process_implementations.cubes._filter import filter_bbox
from openeo_pg_parser_networkx.pg_schema import BoundingBox, GeoJson, TemporalInterval

__all__ = ["load_collection", "save_result"]

_log = logging.getLogger(__name__)

# Sentinel so we only patch once per process.
_rasterio_open_patched = False


class _AuthDataset:
    """Thin wrapper around a rasterio dataset that keeps a rasterio.Env alive
    for the full duration of the ``with rasterio.open(...) as ds:`` block.

    Without this, the Env exits immediately after open() returns, so subsequent
    range-read requests inside the block run without the auth header.
    """

    def __init__(self, ds, env):
        self._ds = ds
        self._env = env

    def __enter__(self):
        return self._ds.__enter__()

    def __exit__(self, *args):
        try:
            return self._ds.__exit__(*args)
        finally:
            self._env.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self._ds, name)


def _patch_rasterio_open_with_auth(hdr_path: str) -> None:
    """Replace rasterio.open with a version that injects GDAL HTTP auth env for
    HDA download URLs.  Called once per load_collection invocation; subsequent
    calls update the header file in-place so the token stays fresh."""
    global _rasterio_open_patched

    _log.info("GDAL auth header file: %s", hdr_path)

    if _rasterio_open_patched:
        return  # already patched; header file was refreshed above

    _orig_open = rasterio.open

    @functools.wraps(_orig_open)
    def _auth_open(fp, mode="r", *args, **kwargs):
        if isinstance(fp, str) and "hda-download." in fp:
            env = rasterio.Env(GDAL_HTTP_HEADER_FILE=hdr_path, GDAL_HTTP_NO_USE_HEAD="YES")
            env.__enter__()
            try:
                ds = _orig_open(fp, mode, *args, **kwargs)
            except Exception:
                env.__exit__(None, None, None)
                raise
            return _AuthDataset(ds, env)
        return _orig_open(fp, mode, *args, **kwargs)

    rasterio.open = _auth_open
    _rasterio_open_patched = True
    _log.info("rasterio.open patched for HDA Bearer auth")


def _setup_hda_auth() -> None:
    log = _log
    client_id = os.environ.get("STAC_API_USERNAME")
    client_secret = os.environ.get("STAC_API_PASSWORD")
    if not (client_id and client_secret):
        log.warning("STAC_API_USERNAME/STAC_API_PASSWORD not set — HDA downloads will not be authenticated")
        return
    token_url = os.environ.get(
        "STAC_IDENTITY_URL",
        "https://identity.data.destination-earth.eu/auth/realms/DEDL/protocol/openid-connect/token",
    )
    try:
        log.info("Fetching Bearer token from %s", token_url)
        resp = requests.post(
            token_url,
            data={"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"},
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        log.info("Bearer token obtained (length %d)", len(token))
        hdr = tempfile.NamedTemporaryFile(mode="w", suffix=".hdr", prefix="gdal_auth_", delete=False)
        hdr.write(f"Authorization: Bearer {token}\n")
        hdr.close()
        _patch_rasterio_open_with_auth(hdr.name)
    except Exception as e:
        log.warning("Bearer token fetch failed (%s) — HDA downloads may fail", e)


def _stac_auth_headers() -> dict:
    username = os.environ.get("STAC_API_USERNAME")
    password = os.environ.get("STAC_API_PASSWORD")
    if username and password:
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {credentials}"}
    return {}


def load_collection(
    id: str,
    spatial_extent: Optional[Union[BoundingBox, dict, str, GeoJson]] = None,
    temporal_extent: Optional[TemporalInterval] = None,
    bands: Optional[list[str]] = None,
    properties: Optional[dict] = None,
    **kwargs,
):
    query_dict = {}

    query_dict["collections"] = [id]

    if spatial_extent is None:
        raise Exception(
            "No spatial extent was provided, will not load the entire x and y axis of the datacube."
        )
    elif temporal_extent is None:
        raise Exception(
            "No temporal extent was provided, will not load the entire temporal axis of the datacube."
        )

    if isinstance(spatial_extent, BoundingBox):
        query_dict["bbox"] = (
            spatial_extent.west,
            spatial_extent.south,
            spatial_extent.east,
            spatial_extent.north,
        )
    else:
        raise ValueError("Provided spatial extent could not be interpreted.")

    query_dict["datetime"] = tuple(
        [time.root.isoformat() for time in temporal_extent if time != "None"]
    )

    if "STAC_API_URL" not in os.environ:
        raise Exception("STAC URL Not available in executor config.")

    catalog = pystac_client.Client.open(
        os.environ["STAC_API_URL"],
        headers=_stac_auth_headers(),
    )
    results = catalog.search(**query_dict, limit=10)

    result_items = list(results.items())

    example_item = result_items[0]

    crs = None
    resolution = None
    nodata = None
    dtype = None

    if "proj:wkt2" in example_item.properties.keys():
        crs = pyproj.CRS.from_wkt(example_item.properties["proj:wkt2"])
    elif "proj:epsg" in example_item.properties.keys():
        crs = pyproj.CRS.from_epsg(example_item.properties["proj:epsg"])

    if raster.RasterExtension.has_extension(example_item):
        for asset in example_item.get_assets().values():
            if "raster:bands" in asset.extra_fields.keys():
                for band in asset.extra_fields["raster:bands"]:
                    if "spatial_resolution" in band:
                        resolution = band["spatial_resolution"]
                    if "nodata" in band:
                        nodata = band["nodata"]
                    if "data_type" in band:
                        dtype = band["data_type"]
            if resolution and nodata and dtype:
                break
    elif crs is not None:
        crs_measurement = crs.axis_info[0].unit_name

        if crs_measurement == "metre":
            resolution = 10
        elif crs_measurement == "degree":
            resolution = 0.0009

    # Fallback for catalogues that publish no proj/raster extensions (e.g. HDA).
    # Try grid:code MGRS → UTM EPSG, then parse resolution from asset href suffix.
    if crs is None:
        grid_code = example_item.properties.get("grid:code", "")
        mgrs_match = re.match(r"MGRS-(\d{1,2})([C-X])", grid_code, re.IGNORECASE)
        if mgrs_match:
            zone = int(mgrs_match.group(1))
            band_letter = mgrs_match.group(2).upper()
            epsg = 32600 + zone if band_letter >= "N" else 32700 + zone
            crs = pyproj.CRS.from_epsg(epsg)
        else:
            crs = pyproj.CRS.from_epsg(4326)

    if resolution is None:
        for asset in example_item.get_assets().values():
            href = asset.href or ""
            m = re.search(r"_(\d+)m\.", href)
            if m:
                resolution = int(m.group(1))
                break
        if resolution is None:
            # Check alternate origin hrefs
            for asset in example_item.get_assets().values():
                alt_href = (
                    asset.extra_fields.get("alternate", {})
                    .get("origin", {})
                    .get("href", "")
                )
                m = re.search(r"_(\d+)m\.", alt_href)
                if m:
                    resolution = int(m.group(1))
                    break
        if resolution is None:
            crs_unit = crs.axis_info[0].unit_name if crs else "degree"
            resolution = 10 if crs_unit == "metre" else 0.0001

    # TODO Need to tidy up the logic above.
    kwargs = {}
    # TODO Need to ensure nodata belongs to the dtype
    if dtype:
        kwargs["dtype"] = dtype

        if "int" in dtype and isinstance(nodata, float):
            nodata = int(nodata)

    if nodata:
        kwargs["nodata"] = nodata

    # Patch rasterio.open so every Dask worker thread gets an explicit
    # rasterio.Env with the Bearer token when opening HDA download URLs.
    # Global GDAL config (set_gdal_config) is not reliably applied inside
    # per-thread rasterio.Env contexts created by odc.stac's task runner.
    _setup_hda_auth()

    lazy_xarray = stac_load(
        result_items,
        crs=crs,
        resolution=resolution,
        bands=bands,
        # TODO Add some way to decide chunks
        chunks={"x": 2048, "y": 2048},
        # Some STAC items have stale/wrong collection IDs causing genuine 404s;
        # skip those rather than aborting the entire job.
        fail_on_error=False,
        **kwargs,
    ).to_array(dim="bands")

    lazy_xarray.rio.write_crs(crs)

    # Add some sort of clipping here to the original bounding box that was requested.
    return filter_bbox(lazy_xarray, extent=spatial_extent)


def save_result(
    data: RasterCube,
    format: str = "netcdf",
    options: Optional[dict] = None,
):
    """ """

    def clean_unused_coordinates(ds):
        """
        Remove all coordinates that are not used in the DataArray dimensions.
        """
        # Gather all dimensions used by DataArray variables
        used_dims = set()
        for var in ds.dims:
            used_dims.update(ds[var].dims)

        # Drop unused coordinates
        for coord in list(ds.coords):
            if coord not in used_dims:
                ds = ds.drop_vars(coord)
        return ds

    import uuid

    logging.info("DATA %s", data)
    logging.info("DATA ATTRS %s", data.attrs)

    _id = str(uuid.uuid4())
    # TODO A nice abstraction to split the xarray into the respective output datasets
    # TODO Some nicer way to handle the user workspace
    destination = Path(os.environ["OPENEO_RESULTS_PATH"]) / f"{_id}.nc"
    dim = data.openeo.band_dims[0] if data.openeo.band_dims else None
    crs = data.rio.crs

    data.attrs = {}
    data.attrs["crs"] = str(crs)

    out_data: xr.Dataset = data.to_dataset(
        dim=dim, name="name" if not dim else None, promote_attrs=True
    )

    dtype = None
    if not dtype:
        dtype = "float32"

    comp = dict(zlib=True, complevel=5, dtype=dtype)

    encoding = {var: comp for var in out_data.data_vars}
    out_data = clean_unused_coordinates(out_data)

    out_data.to_netcdf(path=destination, encoding=encoding)
