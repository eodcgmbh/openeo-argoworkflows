import base64
import json
import logging
import numpy as np
import os
import pyproj
import pystac_client
import re
import rioxarray
import xarray as xr

from odc.stac import stac_load
from pathlib import Path
from pystac.extensions import raster
from typing import Optional, Union
from openeo_processes_dask_slim.process_implementations.data_model import RasterCube
from openeo_processes_dask_slim.process_implementations.cubes._filter import filter_bbox
from openeo_pg_parser_networkx.pg_schema import BoundingBox, GeoJson, TemporalInterval

__all__ = ["load_collection", "save_result"]


def _stac_auth_headers() -> dict:
    username = os.environ.get("STAC_API_USERNAME")
    password = os.environ.get("STAC_API_PASSWORD")
    if username and password:
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {credentials}"}
    return {}


def _subset_to_bands(cube, bands: Optional[list[str]]):
    """Restrict a loaded cube to the requested bands.

    Handles both a band-dimension DataArray and a per-variable Dataset. No-op
    when no bands are requested or none of them are present.
    """
    if not bands:
        return cube
    if isinstance(cube, xr.Dataset):
        present = [b for b in bands if b in cube.data_vars]
        return cube[present] if present else cube
    for band_dim in ("bands", "band"):
        if band_dim in cube.dims:
            available = cube[band_dim].values.tolist()
            present = [b for b in bands if b in available]
            if present:
                return cube.sel({band_dim: present})
            break
    return cube


def load_collection(
    id: str,
    spatial_extent: Optional[Union[BoundingBox, dict, str, GeoJson]] = None,
    temporal_extent: Optional[TemporalInterval] = None,
    bands: Optional[list[str]] = None,
    properties: Optional[dict] = None,
    **kwargs,
):
    if spatial_extent is None:
        raise Exception(
            "No spatial extent was provided, will not load the entire x and y axis of the datacube."
        )
    elif temporal_extent is None:
        raise Exception(
            "No temporal extent was provided, will not load the entire temporal axis of the datacube."
        )

    if "STAC_API_URL" not in os.environ:
        raise Exception("STAC URL Not available in executor config.")

    # Prefer the dedl load_stac for DEDL-native / icechunk collections: it reads
    # native assets (icechunk/defair) and applies band selection itself, instead
    # of the odc.stac_load fallback below that materialises every asset.
    try:
        from openeo_processes_dedl_cube_load import load_stac
    except ImportError:
        load_stac = None

    if load_stac is not None:
        stac_url = os.environ["STAC_API_URL"].rstrip("/") + f"/collections/{id}"
        cube = load_stac(
            url=stac_url,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            bands=bands,
            properties=properties,
        )
        return _subset_to_bands(cube, bands)

    # Fallback: odc.stac_load (e.g. base executor image without the dedl package).
    query_dict = {}

    query_dict["collections"] = [id]

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

    # Only load the requested bands instead of every asset in the items.
    if bands:
        kwargs["bands"] = bands

    lazy_xarray = stac_load(
        result_items,
        crs=crs,
        resolution=resolution,
        # TODO Add some way to decide chunks
        chunks={"x": 2048, "y": 2048},
        **kwargs,
    ).to_array(dim="bands")

    lazy_xarray.rio.write_crs(crs)

    # Add some sort of clipping here to the original bounding box that was requested.
    return filter_bbox(lazy_xarray, extent=spatial_extent)


def _derive_crs(data) -> Optional[pyproj.CRS]:
    """Resolve the cube CRS without tripping over rioxarray.

    For dedl native (geostationary) cubes, ``.rio.crs`` raises: band selection
    drops the ``geostationary`` grid-mapping variable, leaving a dangling
    ``grid_mapping`` reference and an invalid ``crs="None"`` attr. Derive the CRS
    from the geostationary ``projection`` attr (a JSON PROJ dict) instead, and
    only fall back to rioxarray when that attr is absent.
    """
    projection = data.attrs.get("projection")
    if projection:
        try:
            proj = (
                json.loads(projection)
                if isinstance(projection, str)
                else dict(projection)
            )
            # `type`/`no_defs` are metadata, not PROJ params, and break from_dict.
            proj = {k: v for k, v in proj.items() if k not in ("type", "no_defs")}
            return pyproj.CRS.from_dict(proj)
        except Exception:
            logging.warning(
                "save_result: could not parse projection attr %r", projection
            )
    try:
        return data.rio.crs
    except Exception:
        logging.warning("save_result: could not resolve CRS via rioxarray")
        return None


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
    crs = _derive_crs(data)

    # The dedl load_stac path yields an xarray.Dataset (one variable per band),
    # which is already in the target shape and has no `.openeo` DataArray accessor.
    # The odc fallback path yields a RasterCube (DataArray), which we split into a
    # Dataset along its band dimension.
    if isinstance(data, xr.Dataset):
        out_data: xr.Dataset = data
    else:
        dim = data.openeo.band_dims[0] if data.openeo.band_dims else None
        out_data = data.to_dataset(
            dim=dim, name="name" if not dim else None, promote_attrs=True
        )

    # Band selection drops the `geostationary` grid-mapping variable but leaves a
    # dangling `grid_mapping` reference on the data variables; strip it so the
    # output doesn't point at a missing variable.
    for var in out_data.data_vars:
        out_data[var].attrs.pop("grid_mapping", None)
        out_data[var].encoding.pop("grid_mapping", None)

    out_data.attrs = {"crs": crs.to_string() if crs is not None else "None"}

    dtype = None
    if not dtype:
        dtype = "float32"

    comp = dict(zlib=True, complevel=5, dtype=dtype)

    encoding = {var: comp for var in out_data.data_vars}
    out_data = clean_unused_coordinates(out_data)

    out_data.to_netcdf(path=destination, encoding=encoding)
