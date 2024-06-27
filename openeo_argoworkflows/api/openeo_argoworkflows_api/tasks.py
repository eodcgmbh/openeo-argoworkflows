from datetime import timedelta
from hera.workflows import  WorkflowsService
from openeo_fastapi.api.types import Status
from redis import Redis
from rq import Queue
from typing import Any

from openeo_fastapi.client.psql.engine import modify
from openeo_argoworkflows_api.psql.models import ArgoJob
from openeo_argoworkflows_api.workflows import executor_workflow
from openeo_argoworkflows_api.settings import ExtendedAppSettings

settings = ExtendedAppSettings()
q = Queue(
    connection=Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT
))

def queue_to_submit(job: ArgoJob):
    """  Function to see if there is space in the pool for another Job. """
    argo = WorkflowsService(
        host=settings.ARGO_WORKFLOWS_SERVER,
        verify_ssl=False,
        namespace=settings.ARGO_WORKFLOWS_NAMESPACE,
        token=settings.ARGO_WORKFLOWS_TOKEN.get_secret_value(),
    )

    workflows = argo.list_workflows().items

    if not workflows:
        return q.enqueue(submit_job, job)

    check_statuses = ("Running", "Pending")
    filtered_workflows = [
        workflow
        for workflow in workflows
        if workflow.status.phase in check_statuses
    ]

    if len(filtered_workflows) >= settings.ARGO_WORKFLOWS_LIMIT:
        return q.enqueue_in(timedelta(minutes=5), queue_to_submit, job)
    else:
        return q.enqueue(submit_job, job)


def submit_job(job: ArgoJob):
    """ Submit the job to argo. """
    argo = WorkflowsService(
        host=settings.ARGO_WORKFLOWS_SERVER,
        verify_ssl=False,
        namespace=settings.ARGO_WORKFLOWS_NAMESPACE,
        token=settings.ARGO_WORKFLOWS_TOKEN.get_secret_value(),
    )    

    dask_profile = {
        "LOCAL": True
    }

    user_profile = {
        "OPENEO_JOB_ID": job.job_id,
        "OPENEO_USER_ID": job.user_id,
        "OPENEO_USER_WORKSPACE": settings.OPENEO_WORKSPACE_ROOT / job.user_id / job.job_id
    }
    workflow = executor_workflow(argo, job.process.process_graph, dask_profile, user_profile)

    response = workflow.create()

    job.status = Status.running
    modify(job)

    return q.enqueue(poll_job_status, job, response.metadata)


def poll_job_status(job: ArgoJob, metadata: Any):
    """ Submit the job to argo. """
    argo = WorkflowsService(
        host=settings.ARGO_WORKFLOWS_SERVER,
        verify_ssl=False,
        namespace=settings.ARGO_WORKFLOWS_NAMESPACE,
        token=settings.ARGO_WORKFLOWS_TOKEN.get_secret_value(),
    )

    workflow = argo.get_workflow(
        name=metadata.name,
        namespace=metadata.namespace
    )

    if workflow.status.phase == "Succeeded":
        job.status = Status.finished
        modify(job)
    elif workflow.status.phase in ("Failed", "Error"):
        job.status = Status.error
        modify(job)
    elif workflow.status.phase in ("Running", "Pending"):
        return q.enqueue(poll_job_status, job, metadata)