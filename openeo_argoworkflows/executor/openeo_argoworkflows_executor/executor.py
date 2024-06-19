import importlib
import inspect
import logging

from pathlib import Path
from pydantic import BaseModel
from typing import Optional

from openeo_pg_parser_networkx import Process, ProcessRegistry, OpenEOProcessGraph
from openeo_processes_dask.process_implementations.core import process


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


def execute(
    parsed_graph: OpenEOProcessGraph,
    parameters: Optional[dict] = None,
):
    process_registry = ProcessRegistry(wrap_funcs=[process])
    
    _register_processes_from_module(process_registry, "openeo_processes_dask")
    _register_processes_from_module(process_registry, "openeo_argoworkflows_executor.extra_processes")


    print("Wants to call")
    parsed_graph = parsed_graph
    pg_callable = parsed_graph.to_callable(
        process_registry=process_registry,
        results_cache={},
        parameters=parameters,
    )
    
    print("Started Processing. ", pg_callable, type(pg_callable))
    pg_callable()