import os
import pyproj
import pystac_client

from odc.stac import stac_load
from pathlib import Path
from pystac.extensions import raster
from typing import Optional, Union
from openeo_processes_dask.process_implementations.cubes import load_stac
from openeo_processes_dask.process_implementations.data_model import (
    RasterCube
)
from openeo_pg_parser_networkx.pg_schema import BoundingBox, GeoJson, TemporalInterval

__all__ = ["load_collection", "save_result"]

def load_collection(
    id: str,
    spatial_extent: Optional[Union[BoundingBox, dict, str, GeoJson]] = None,
    temporal_extent: Optional[TemporalInterval] = None,
    bands: Optional[list[str]] = None,
    properties: Optional[dict] = None,
    **kwargs,
):
    print("In load collection")

    stac_url = f"https://stac.eodc.eu/api/v1/"

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
        [str(time.root) for time in temporal_extent if time != 'None']
    )

    catalog = pystac_client.Client.open(stac_url)
    results = catalog.search(**query_dict, limit=10)

    result_items = list(results.items())

    example_item = result_items[0]

    if "proj:wkt2" in example_item.properties.keys():
        crs = pyproj.CRS.from_wkt(example_item.properties["proj:wkt2"])
    elif "proj:epsg" in example_item.properties.keys():
        crs = pyproj.CRS.from_epsg(example_item.properties["proj:epsg"])

    if raster.RasterExtension.has_extension(example_item):
        for asset in example_item.get_assets().values():
            if 'raster:bands' in asset.extra_fields.keys():
                for band in asset.extra_fields['raster:bands']:
                    if 'spatial_resolution' in band:
                        resolution = band['spatial_resolution']
                    if 'nodata' in band:
                        nodata = band['nodata']
                    if 'data_type' in band:
                        dtype = band['data_type']
            if resolution and nodata and dtype:
                break
    else:
        crs_measurement = pyproj.CRS.from_wkt(crs).axis_info[0].unit_name

        if crs_measurement == 'metre':
            resolution = 10
        elif crs_measurement == 'degree':
            resolution = 0.0009
        
    # TODO Need to tidy up the logic above.
    kwargs = {}
    # TODO Need to ensure nodata belongs to the dtype
    if dtype:
        kwargs["dtype"] = dtype

        if "int" in dtype and isinstance(nodata, float):
            nodata = int(nodata)

    if nodata:
        kwargs["nodata"] = nodata       

    lazy_xarray = stac_load(
        result_items,
        crs=crs,
        resolution=resolution,
        # TODO Add some way to decide chunks
        chunks={"x": 2048, "y": 2048},
        **kwargs
    ).to_array(dim='bands')

    # Add some sort of clipping here to the original bounding box that was requested.
    return lazy_xarray

def save_result(
    data: RasterCube,
    format: str = 'netcdf',
    options: Optional[dict] = None,
):
    """ """
    import uuid

    _id = str(uuid.uuid4())
    # TODO A nice abstraction to split the xarray into the respective output datasets
    # TODO Some nicer way to handle the user workspace
    destination = Path(os.environ["OPENEO_RESULTS_PATH"]) / f"{_id}.nc"

    data.to_netcdf(path=destination)

