import click
import fsspec
import logging

logger = logging.getLogger(__name__)

@click.group()
def cli():
    """Defining group for executor CLI."""
    pass

@click.command()
@click.option(
    '--process_graph',
    type=str,
    required=True,
    help='OpenEO Process Graph as a JSON string.',
)
@click.option(
    '--user_profile',
    type=str,
    required=True,
    help='Profile of the Dask Cluster to initialise.',
)
@click.option(
    '--dask_profile',
    type=str,
    required=True,
    help='Profile of the Dask Cluster to initialise.',
)
def execute(process_graph, user_profile, dask_profile):
    """CLI for running the OpenEOExecutor on an OpenEO process graph."""
    
    import os
    import json

    from dask_gateway import Gateway
    import openeo_processes_dask
    from openeo_pg_parser_networkx.graph import OpenEOProcessGraph

    from openeo_argoworkflows_executor.executor import execute
    from openeo_argoworkflows_executor.models import ExecutorParameters
    from openeo_argoworkflows_executor.stac import create_stac_item

    logger.info(
        f"Using processes from openeo-processes-dask v{openeo_processes_dask.__version__}"
    )

    openeo_parameters = ExecutorParameters(
        process_graph=json.loads(process_graph),
        user_profile=json.loads(user_profile),
        dask_profile=json.loads(dask_profile)
    )

    if not openeo_parameters.user_profile.OPENEO_USER_WORKSPACE.exists():
        openeo_parameters.user_profile.OPENEO_USER_WORKSPACE.mkdir(parents=True, exist_ok=True)

    if not openeo_parameters.user_profile.results_path.exists():
        openeo_parameters.user_profile.results_path.mkdir(parents=True, exist_ok=True)

    if not openeo_parameters.user_profile.stac_path.exists():
        openeo_parameters.user_profile.stac_path.mkdir(parents=True, exist_ok=True)

    os.environ["OPENEO_USER_WORKSPACE"] = str(openeo_parameters.user_profile.OPENEO_USER_WORKSPACE)
    os.environ["OPENEO_STAC_PATH"] = str(openeo_parameters.user_profile.stac_path)
    os.environ["OPENEO_RESULTS_PATH"] = str(openeo_parameters.user_profile.results_path)

    if openeo_parameters.dask_profile.LOCAL:
        from dask.distributed import worker_client

        dask_cluster = None
        client = worker_client()
        pass
    else:
        gateway = Gateway(openeo_parameters.dask_profile.GATEWAY_URL)
        options = gateway.cluster_options()

        options.OPENEO_JOB_ID = openeo_parameters.user_profile.OPENEO_JOB_ID
        options.OPENEO_USER_ID = openeo_parameters.user_profile.OPENEO_USER_ID

        options.IMAGE = openeo_parameters.dask_profile.OPENEO_EXECUTOR_IMAGE

        options.WORKER_CORES = int(openeo_parameters.dask_profile.WORKER_CORES)
        options.WORKER_MEMORY = int(openeo_parameters.dask_profile.WORKER_MEMORY)
        options.CLUSTER_IDLE_TIMEOUT = int(openeo_parameters.dask_profile.CLUSTER_IDLE_TIMEOUT)

        dask_cluster = gateway.new_cluster(options, shutdown_on_close=True)

        # We need to initiate a cluster with at least one worker, otherwise .scatter that's used in xgboost will timeout waiting for workers
        # See https://github.com/dask/distributed/issues/2941
        dask_cluster.adapt(minimum=1, maximum=int(openeo_parameters.dask_profile.WORKER_LIMIT))
        client = dask_cluster.get_client()

    parsed_graph = OpenEOProcessGraph(pg_data=openeo_parameters.process_graph)

    execute(parsed_graph=parsed_graph)

    # Can't assume the same cluster is running post process graph execution due to sub workflows processing.
    # If the previous cluster was closed, check for a new one!
    if dask_cluster:
        if dask_cluster.status == 'closed':
            cluster_list = gateway.list_clusters()
            if cluster_list:
                dask_cluster = gateway.connect(cluster_list[0].name)

        # Can call shutdown on previously closed clusters.
        dask_cluster.shutdown()

    # TODO Time to generate STAC
    from pystac import Asset, Collection, Extent, SpatialExtent, TemporalExtent, layout

    fs = fsspec.filesystem(protocol="file")

    output_collection = Collection(
        id=openeo_parameters.user_profile.OPENEO_JOB_ID,
        description=f"The STAC Collection representing the output of job {openeo_parameters.user_profile.OPENEO_JOB_ID}",
        extent=Extent(
            SpatialExtent([None, None, None, None]), TemporalExtent([None, None])
        ),
    )

    collection_href = str(
            openeo_parameters.user_profile.stac_path / f"{output_collection.id}_collection.json"
        )
    output_collection.set_self_href(collection_href)

    from openeo_argoworkflows_executor.stac import create_stac_item

    for file in fs.listdir(str(openeo_parameters.user_profile.results_path)):
        filepath = file["name"]

        item = create_stac_item(filepath)

        item.set_parent(output_collection)
        item_href = str(
            openeo_parameters.user_profile.stac_path / f"{item.id}.json"
        )
        item.set_self_href(item_href)

        tmp_asset = Asset(
                title=item.id,
                href=str(filepath),
                roles=["data"]
            )
    
        output_collection.add_asset(item.id, tmp_asset)

        output_collection.add_item(item, strategy=layout.AsIsLayoutStrategy())

        item.save_object()

    output_collection.update_extent_from_items()
    output_collection.save_object()


cli.add_command(execute)

if __name__ == '__main__':
    cli()
