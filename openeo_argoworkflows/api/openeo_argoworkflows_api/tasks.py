import json

from datetime import timedelta
from hera.workflows import  WorkflowsService
from openeo_fastapi.api.types import Status
from redis import Redis
from rq import Queue
from typing import Any

from openeo_fastapi.client.psql import engine
from openeo_fastapi.client.psql.engine import modify
from openeo_argoworkflows_api.psql.models import ArgoJob, ExtendedUser
from openeo_argoworkflows_api.workflows import executor_workflow
from openeo_argoworkflows_api.settings import ExtendedAppSettings

settings = ExtendedAppSettings()


def _select_dask_profile(
    user_roles: list,
    role_mapping: dict,
    profiles: dict,
    base_profile: dict,
) -> dict:
    """Return a dask profile dict for the given user roles.

    Walks user_roles in order, finds the first hit in role_mapping, then looks
    up that profile name in profiles and merges it over base_profile.
    Falls back to role_mapping["default"] when no role matches, then to
    base_profile unchanged when neither match nor default exists.
    """
    def _resolve(profile_name: str) -> dict:
        if profile_name in profiles:
            return {**base_profile, **profiles[profile_name]}
        return base_profile

    for role in user_roles:
        if role in role_mapping:
            return _resolve(role_mapping[role])

    if "default" in role_mapping:
        return _resolve(role_mapping["default"])

    return base_profile


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

    if settings.DASK_GATEWAY_SERVER and settings.OPENEO_EXECUTOR_IMAGE:
        base_profile = {
            "GATEWAY_URL": settings.DASK_GATEWAY_SERVER,
            "OPENEO_EXECUTOR_IMAGE": settings.OPENEO_EXECUTOR_IMAGE,
            "WORKER_CORES": settings.DASK_WORKER_CORES,
            "WORKER_MEMORY": settings.DASK_WORKER_MEMORY,
            "WORKER_LIMIT": settings.DASK_WORKER_LIMIT,
            "CLUSTER_IDLE_TIMEOUT": settings.DASK_CLUSTER_IDLE_TIMEOUT
        }
        if settings.DASK_PROFILES and settings.DASK_ROLE_PROFILE_MAPPING:
            user = engine.get(get_model=ExtendedUser, primary_key=job.user_id)
            user_roles = (user.roles or []) if user else []
            dask_profile = _select_dask_profile(
                user_roles,
                json.loads(settings.DASK_ROLE_PROFILE_MAPPING),
                json.loads(settings.DASK_PROFILES),
                base_profile,
            )
        else:
            dask_profile = base_profile
    else:
        dask_profile = {"LOCAL": True}

    user_profile = {
        "OPENEO_JOB_ID": str(job.job_id),
        "OPENEO_USER_ID": str(job.user_id),
        "OPENEO_USER_WORKSPACE": str(settings.OPENEO_WORKSPACE_ROOT / str(job.user_id) / str(job.job_id))
    }
    workflow = executor_workflow(argo, job.process.process_graph, dask_profile, user_profile)

    response = workflow.create()

    job.status = Status.running
    job.workflowname = response.metadata.name
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