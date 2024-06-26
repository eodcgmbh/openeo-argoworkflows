import importlib
import inspect
import logging
from typing import Optional

from openeo_pg_parser_networkx import Process, ProcessRegistry, OpenEOProcessGraph
from openeo_processes_dask.process_implementations.core import process

from openeo_argoworkflows_executor.stac import StacGrid
from openeo_argoworkflows_executor.utils import (
    derive_sub_graph, get_pg_bounding_box
)

logger = logging.getLogger(__name__)

def _register_processes_from_module(
    process_registry,
    source,
    implementations_dir="process_implementations",
    specs_dir="specs",
):
    """Grab all the process implementations from openeo_executor
    and register to the process registry."""

    processes_from_module = [
        func
        for _, func in inspect.getmembers(
            importlib.import_module(f"{source}.{implementations_dir}"),
            inspect.isfunction,
        )
    ]

    specs_module = importlib.import_module(f"{source}.{specs_dir}")
    specs = {
        func.__name__: getattr(specs_module, func.__name__)
        for func in processes_from_module
    }

    for func in processes_from_module:
        process_registry[func.__name__] = Process(
            spec=specs[func.__name__], implementation=func
        )

    return process_registry


def prepare_graphs(
    process_graph: OpenEOProcessGraph
):
    # We get the total bounding box from the process graph
    _box = get_pg_bounding_box(process_graph.pg_data)


    bbox = [ _box.west, _box.south, _box.east, _box.north ]

    tilesize = 100000
    crs = 4326

    grid = StacGrid(
        bbox, tilesize, crs
    )

    # We get the cells for this given process graph
    grid.set_grid_cells()

    sub_graphs = []
    # Derive a list of "sub_graphs"
    for cell in grid.cells:
        sub_graphs.append(
            OpenEOProcessGraph(pg_data=derive_sub_graph(cell, process_graph.pg_data))
        )

    return sub_graphs    

def execute(
    parsed_graph: OpenEOProcessGraph
):
    process_registry = ProcessRegistry(wrap_funcs=[process])
    
    _register_processes_from_module(process_registry, "openeo_processes_dask")
    _register_processes_from_module(process_registry, "openeo_argoworkflows_executor.extra_processes")

    sub_graphs = prepare_graphs(parsed_graph)

    for graph in sub_graphs:
        pg_callable = graph.to_callable(
            process_registry=process_registry,
            results_cache={}
        )
        
        pg_callable()