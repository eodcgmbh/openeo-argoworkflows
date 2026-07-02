import datetime
import os

import numpy as np
import shapely
import stactools.core.projection
import xarray as xr

from pydantic import BaseModel
from pyproj import Geod, CRS
from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension
from shapely import geometry, Polygon, box
from typing import Optional, Tuple, Union

class GridCorners(BaseModel):

    lower_left: Tuple[Union[int, float], Union[int, float]]
    lower_right: Tuple[Union[int, float], Union[int, float]]
    upper_left: Tuple[Union[int, float], Union[int, float]]
    upper_right: Tuple[Union[int, float], Union[int, float]]


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

    crs = None
    proj_bbox = proj_transform = proj_shape = None
    bbox = None

    with xr.open_dataset(href) as dataset:
        try:
            crs = dataset.rio.crs
        except Exception:
            # Cubes without a PROJ-recognised CRS (e.g. healpix, whose `crs`
            # attr is `healpix:1024` / `None`) make rioxarray raise. Treat them
            # as CRS-less and fall back to a lat/lon bbox below.
            crs = None

        if crs is not None:
            proj_bbox = dataset.rio.bounds()
            proj_transform = list(dataset.rio.transform())[0:6]
            proj_shape = dataset.rio.shape
        else:
            bbox = _wgs84_bbox_from_dataset(dataset)

    if crs is not None:
        proj_geometry = shapely.geometry.mapping(shapely.geometry.box(*proj_bbox))
        geometry = stactools.core.projection.reproject_geom(
            crs, "EPSG:4326", proj_geometry, precision=6
        )
        bbox = list(shapely.geometry.shape(geometry).bounds)
    elif bbox is not None:
        geometry = shapely.geometry.mapping(shapely.geometry.box(*bbox))
    else:
        geometry = None

    item = Item(
        id=id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime.datetime.now(),
        assets={"raster-result": Asset(href=href, title="raster-data", roles=["data"])},
        properties={},
    )

    # Only attach the projection extension when there is a real CRS; CRS-less
    # cubes (healpix) get a plain geometry-only item.
    if crs is not None:
        projection = ProjectionExtension.ext(item, add_if_missing=True)
        epsg = crs.to_epsg()
        if epsg:
            projection.epsg = epsg
        else:
            projection.wkt2 = crs.to_wkt("WKT2_2015")

        projection.transform = proj_transform
        projection.shape = proj_shape

    return item


def _wgs84_bbox_from_dataset(dataset) -> Optional[list]:
    """Best-effort WGS84 bbox from ``lat``/``lon`` (or ``y``/``x``) coordinates.

    Used for cubes without a projected CRS (e.g. healpix), where rioxarray
    cannot derive bounds. Returns ``None`` when no such coordinates are present.
    """
    lat = dataset.coords.get("lat", dataset.coords.get("y"))
    lon = dataset.coords.get("lon", dataset.coords.get("x"))
    if lat is None or lon is None:
        return None
    try:
        return [
            float(lon.min()),
            float(lat.min()),
            float(lon.max()),
            float(lat.max()),
        ]
    except Exception:
        return None


class StacGrid:

    def __init__(self, bbox, tilesize, crs) -> None:
        self.bbox = geometry.box(*bbox)
        self.edges = self.derive_points(self.bbox)

        self.tilesize = tilesize

        self.crs = CRS(crs)

        self._cells = None

    @property
    def get_cells(self):
        """ """
        if self._cells:
            return self._cells
        self._cells = self.derive_cells()
        return self._cells
    
    @classmethod
    def derive_points(cls, bbox: Polygon):
        """ """
        minx, miny, maxx, maxy = bbox.bounds
        
        lower_left = (minx, miny)
        lower_right = (maxx, miny)
        upper_left = (minx, maxy)
        upper_right = (maxx, maxy)
        return GridCorners(
            lower_left=lower_left,
            lower_right=lower_right,
            upper_left=upper_left,
            upper_right=upper_right
        )
    
    @staticmethod
    def derive_distance(crs, point1, point2):
        """ Returning distance in metres divided by the resolution in metres. """
        geod = Geod(a=crs.ellipsoid.semi_major_metre, rf=crs.ellipsoid.inverse_flattening)
        az12, az21, distance = geod.inv(point1[0], point1[1], point2[0], point2[1])
        return  distance
    

    @classmethod
    def find_cell_bounds(cls, crs, cell, starting_position):

        lon, lat = starting_position

        geod = Geod(a=crs.ellipsoid.semi_major_metre, rf=crs.ellipsoid.inverse_flattening)
        
        y_range, x_range = cell

        yN, yM = y_range
        min_lon, tmp_lat, _= geod.fwd(lon, lat, 90, yN )
        max_lon, _, _ = geod.fwd(lon, lat, 90, yM )

        xN, xM = x_range
        _, min_lat , _= geod.fwd(min_lon, tmp_lat, 180, xN )
        _, max_lat, _= geod.fwd(min_lon, tmp_lat, 180, xM )

        return box(min_lon, min_lat, max_lon, max_lat)

    def set_grid_cells(self):
        """ """

        lon_distance = self.derive_distance(self.crs, self.edges.upper_left, self.edges.upper_right)
        lat_distance = self.derive_distance(self.crs, self.edges.upper_left, self.edges.lower_left)

        cells = [
        
        ]
        n_lon_tiles = int(np.ceil(lon_distance / self.tilesize))
        n_lat_tiles = int(np.ceil(lat_distance / self.tilesize))


        for long_cell in range(n_lon_tiles):

            if ((long_cell + 1) * self.tilesize) > lon_distance:
                long_cell_pos = (long_cell * self.tilesize, lon_distance)
            else:
                long_cell_pos = (long_cell * self.tilesize, ((long_cell + 1) * self.tilesize) - 1)
            
            for lat_cell in range(n_lat_tiles):

                if ((lat_cell + 1) * self.tilesize) > lat_distance:
                    lat_cell_pos = (lat_cell * self.tilesize, lat_distance)

                else:
                    lat_cell_pos = (lat_cell * self.tilesize, ((lat_cell + 1) * self.tilesize) - 1)

                bounds = self.find_cell_bounds(self.crs, [long_cell_pos, lat_cell_pos], self.edges.upper_left)

                cells.append([
                    long_cell_pos, lat_cell_pos, bounds
                ])

        self.cells = cells
