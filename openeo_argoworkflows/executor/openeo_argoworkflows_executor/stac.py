import datetime
import os

import geopandas as gpd
import shapely
import stactools.core.projection
import xarray as xr

from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension


# This is stolen directly from stactools, because in the original function
# rasterio is used to open the file, so netcdf doesn't work.
# We use xarray to open it, and use the .rio accessor to get rasterio-like
# access to metadata!
def create_stac_item(href: str) -> Item:
    """Creates a STAC Item from the asset at the provided href.
    The ``read_href_modifer`` argument can be used to modify the href for the
    rasterio read, e.g. if you need to sign a url.
    This function is intentionally minimal in its signature and capabilities. If
    you need to customize your Item, do so after creation.
    Args:
        href (str): The href of the asset that will be used to create the item.
        read_href_modifier (Optional[ReadHrefModifier]):
            An optional callable that will be used to modify the href before reading.
    Returns:
        pystac.Item: A PySTAC Item.
    """

    id = os.path.splitext(os.path.basename(href))[0]

    with xr.open_dataset(href) as dataset:
        crs = dataset.rio.crs
        proj_bbox = dataset.rio.bounds()
        proj_transform = list(dataset.rio.transform())[0:6]
        proj_shape = dataset.rio.shape

    proj_geometry = shapely.geometry.mapping(shapely.geometry.box(*proj_bbox))
    geometry = stactools.core.projection.reproject_geom(
        crs, "EPSG:4326", proj_geometry, precision=6
    )
    bbox = list(shapely.geometry.shape(geometry).bounds)
    item = Item(
        id=id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime.datetime.now(),
        assets={"raster-result": Asset(href=href, title="raster-data", roles=["data"])},
        properties={},
    )

    projection = ProjectionExtension.ext(item, add_if_missing=True)
    epsg = crs.to_epsg()
    if epsg:
        projection.epsg = epsg
    else:
        projection.wkt2 = crs.to_wkt("WKT2_2015")

    projection.transform = proj_transform
    projection.shape = proj_shape

    return item

