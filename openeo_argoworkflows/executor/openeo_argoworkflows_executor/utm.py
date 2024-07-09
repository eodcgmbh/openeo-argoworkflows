from datetime import datetime
from pathlib import Path

import dask
import geopandas as gpd
import numpy as np
import xarray as xr
from numpy import datetime64
from odc.geo.geobox import resolution_from_affine
from pyproj import CRS, Transformer
from pyproj.aoi import AreaOfInterest
from pyproj.database import query_utm_crs_info
from shapely.geometry import Polygon
from shapely.ops import transform

import openeo_argoworkflows_executor 

MODULE_FILEPATH = openeo_argoworkflows_executor.__path__[0]

utm_shapefile = gpd.read_file(
    MODULE_FILEPATH + '/utm_grid/sentinel_2_index_shapefile.shp'
)

class UTMGrid:
    def __init__(self, sampling) -> None:
        self.utm_shapefile = utm_shapefile
        self.sampling = sampling
        self.resolution = 10

    def get_resolution(self, data):
        if 'x' in data.dims:
            return resolution_from_affine(data.odc.geobox.affine).x
        if "longitude" in data.dims:
            resolution_m = resolution_from_affine(data.odc.geobox.affine).x
            return resolution_m / 0.00009 * self.resolution

    def get_xarray_polygon(self, data, x='x', y='y'):
        x_min, x_max = float(data[x].min().values), float(data[x].max().values)
        y_min, y_max = float(data[y].min().values), float(data[y].max().values)

        return Polygon([[x_min, y_min], [x_min, y_max], [x_max, y_max], [x_max, y_min]])

    def polygon_proj(self, polygon, output_crs, input_crs="EPSG:4326"):
        input_crs = CRS(input_crs)
        if not str(output_crs).upper().startswith("EPSG:"):
            output_crs = "EPSG:" + str(output_crs)
        output_crs = CRS(output_crs)

        project = Transformer.from_crs(input_crs, output_crs, always_xy=True).transform
        proj_polygon = transform(project, polygon)
        return proj_polygon

    def _check_utm(self, intersection, utms):
        if isinstance(utms, list):
            if len(utms) == 1:
                return utms[0].code
            for crs in utms:
                if not isinstance(crs, str):
                    crs = crs.code
                if not crs.upper().startswith("EPSG:"):
                    crs = "EPSG:" + crs
                coords = np.array(self.polygon_proj(intersection, crs).exterior.coords)
                x = coords[:, 0]
                y = coords[:, 1]
                bounds = self.polygon_proj(intersection, crs).bounds
                x_check = np.isclose(x, bounds[0], atol=0.01) + np.isclose(
                    x, bounds[2], atol=0.01
                )
                y_check = np.isclose(y, bounds[1], atol=0.01) + np.isclose(
                    y, bounds[3], atol=0.01
                )
                if x_check.all() and y_check.all():
                    return crs
            return False

    def get_code(self, intersection):
        bounds = intersection.bounds
        utm_crs_list = query_utm_crs_info(
            datum_name="WGS 84",
            area_of_interest=AreaOfInterest(
                west_lon_degree=bounds[0],
                south_lat_degree=bounds[1],
                east_lon_degree=bounds[2],
                north_lat_degree=bounds[3],
            ),
        )
        return self._check_utm(intersection, utm_crs_list)

    def get_utm_tiles(self, data):
        intersection = {}
        for i in range(len(self.utm_shapefile.geometry)):
            if self.utm_shapefile.geometry[i].intersects(data):
                intersection[self.utm_shapefile.Name[i]] = self.utm_shapefile.geometry[i]
        return intersection

    def utm_tiles(self, data, input_crs, x='x', y='y'):
        polygon = self.get_xarray_polygon(data, x, y)
        if input_crs not in ["4326", "EPSG:4326", "epsg:4326"]:
            polygon = self.polygon_proj(polygon, "EPSG:4326", input_crs)
        intersection = self.get_utm_tiles(polygon)
        EPSG = {}
        crs = {}
        for key, value in intersection.items():
            epsg = self.get_code(value)
            EPSG[key] = self.polygon_proj(value, epsg).bounds
            if str(epsg)[-5:] not in crs.keys():
                crs[(str(epsg)[-5:])] = [key]
            else:
                crs[(str(epsg)[-5:])].append(key)
        return EPSG, crs

    def numpy_datetime_to_formatted_string(self, input_time: datetime64) -> str:
        """Take a numpy datetime, and format it as expected."""
        ts = input_time.astype(datetime)
        date = datetime.utcfromtimestamp(ts / 1e9)
        return datetime.strftime(date, "%Y%m%dT%H%M%S")

    def split_xarray_by_tiles(
        self,
        data: xr.DataArray,
        filepath: Path,
        extension: str,
        time_dimension: str = 'time',
    ) -> tuple[list[xr.Dataset], list[Path]]:
        """"""
        final_datasets = []
        dataset_filenames = []

        if isinstance(filepath, str):
            filepath = Path(filepath)

        try:
            crs = data.crs
        except AttributeError:
            crs = None

        if crs is None:
            try:
                crs = data.rio.crs
            except AttributeError:
                raise AttributeError(
                    "No CRS could be determined for gridding processing output."
                )

        tile_bounds, crs = self.utm_tiles(
            data, str(crs), x=data.openeo.x_dim, y=data.openeo.y_dim
        )
        sampling_str = str(self.sampling)
        sampling_str = sampling_str if len(sampling_str) == 3 else "0" + sampling_str
        resolution = self.get_resolution(data)
        
        dim = data.openeo.band_dims[0] if data.openeo.band_dims else None
        for crs_str, tiles in crs.items():
            reproj = data.odc.reproject(
                how=CRS.from_epsg(crs_str),
                resolution=resolution,
                resampling="nearest",
            )
            for tile in tiles:
                temp_bbox = tile_bounds[tile]

                with dask.config.set(**{'array.slicing.split_large_chunks': False}):
                    temp_data = (
                        reproj.where(reproj["x"] > temp_bbox[0], drop=True)
                        .where(reproj["x"] <= temp_bbox[2], drop=True)
                        .where(reproj["y"] > temp_bbox[1], drop=True)
                        .where(reproj["y"] <= temp_bbox[3], drop=True)
                    )

                times, dataarrays = zip(*temp_data.groupby(time_dimension))
                for idx, time in enumerate(times):
                    final_data = dataarrays[idx]
                    try:
                        file_time = self.numpy_datetime_to_formatted_string(time)
                    except Exception:
                        file_time = "NO_TIME"

                    final_data = final_data.to_dataset(
                        dim=dim, name="name" if not dim else None, promote_attrs=True
                    )
                    final_datasets.append(final_data)
                    dataset_filenames.append(
                        filepath / f'UTM{sampling_str}_T{tile}_{file_time}{extension}'
                    )
        return (final_datasets, dataset_filenames)
