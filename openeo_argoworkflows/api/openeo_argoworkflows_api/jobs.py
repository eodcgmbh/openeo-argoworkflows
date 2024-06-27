import datetime
import fsspec
import json
import logging
import requests
import uuid

from hera.exceptions import NotFound
from hera.workflows import WorkflowsService
from hera.workflows.models import WorkflowStopRequest
from hera.exceptions import NotFound
from fastapi import Depends, Response, HTTPException
from typing import Optional
from pydantic import conint
from sqlalchemy.exc import IntegrityError
from urllib.parse import urljoin

from openeo_fastapi.api.types import Status, Error
from openeo_fastapi.api.models import JobsGetLogsResponse, JobsRequest
from openeo_fastapi.client.psql import engine
from openeo_fastapi.client.jobs import JobsRegister
from openeo_fastapi.client.auth import Authenticator, User

from openeo_argoworkflows_api.psql.models import ArgoJob
from openeo_argoworkflows_api.tasks import queue_to_submit

fs = fsspec.filesystem(protocol="file")

logger = logging.getLogger(__name__)


class ArgoJobsRegister(JobsRegister):

    def __init__(self, settings, links) -> None:
        super().__init__(settings, links)

        self.workflows_service = WorkflowsService(
            host=settings.ARGO_WORKFLOWS_SERVER, verify_ssl=False, namespace=settings.ARGO_WORKFLOWS_NAMESPACE
        )

    def create_job(
        self, body: JobsRequest, user: User = Depends(Authenticator.validate)
    ):
        """Create a new BatchJob.

        Args:
            body (JobsRequest): The Job Request that should be used to create the new BatchJob.
            user (User): The User returned from the Authenticator.

        Returns:
            Response: A general FastApi response to signify the changes where made as expected. Specific response
            headers need to be set in this response to ensure certain behaviours when being used by OpenEO client modules.
        """
        job_id = uuid.uuid4()

        if not body.process.id:
            auto_name_size = 16
            body.process.id = uuid.uuid4().hex[:auto_name_size].upper()

        # Create the job
        job = ArgoJob(
            job_id=job_id,
            process=body.process,
            status=Status.created,
            title=body.title,
            description=body.description,
            user_id=user.user_id,
            created=datetime.datetime.now(),
        )

        try:
            engine.create(create_object=job)
        except IntegrityError:
            raise HTTPException(
                status_code=500,
                detail=Error(code="Internal", message=f"The job {job.job_id} already exists."),
            )

        return Response(
            status_code=201,
            headers={
                "Location": f"{self.settings.API_DNS}{self.settings.OPENEO_PREFIX}/jobs/{job_id.__str__()}",
                "OpenEO-Identifier": job_id.__str__(),
                "access-control-allow-headers": "Accept-Ranges, Content-Encoding, Content-Range, Link, Location, OpenEO-Costs, OpenEO-Identifier",
                "access-control-expose-headers": "Accept-Ranges, Content-Encoding, Content-Range, Link, Location, OpenEO-Costs, OpenEO-Identifier",
            },
        )
    
    def start_job(
        self, job_id: uuid.UUID, user: User = Depends(Authenticator.validate)
    ):
        
        job = engine.get(get_model=ArgoJob, primary_key=job_id)

        if not job:
            raise HTTPException(
                    status_code=404,
                    detail="Job was not found for this ID",
                )
        
        if (job.status == Status.queued) or (job.status == Status.running):
                raise HTTPException(
                    status_code=400,
                    detail="Job is already in queue or running, and cannot be started again.",
                )

        job_workspace = (
            self.settings.OPENEO_WORKSPACE_ROOT
            / user.user_id.__str__()
            / job.job_id.__str__()
        )
        try:
            fs.mkdir(job_workspace)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Could not create workspace for the current job."
            )

        job.status = "queued"
        queued = engine.modify(modify_object=job)

        # TODO Enqueue call to submit queued job
        queue_to_submit(job)

        if queued:
            return Response(
                status_code=202,
                content="The creation of the resource has been queued successfully.",
            )
        raise HTTPException(
            status_code=500,
            detail="The workflow server could not run the job at this time. Please try again later.",
        )
        
    def delete_job(
        self, job_id: uuid.UUID, user: User = Depends(Authenticator.validate)
    ):
        
        return Response(
            status_code=202,
            content="The resource has been deleted successfully.",
        )
    
    def stop_job(
        self, job_id: uuid.UUID, user: User = Depends(Authenticator.validate)
    ):
        job = engine.get(get_model=ArgoJob, primary_key=job_id)

        if (job.status != Status.queued) and (job.status != Status.running):
            raise HTTPException(
                status_code=400,
                detail="The job isn't running or queued and therefore could not be canceled",
            )
        
        req = WorkflowStopRequest(
            name=job.workflow_name,
            namespace=self.settings.ARGO_WORKFLOWS_NAMESPACE,
        )

        try:
            self.workflows_service.stop_workflow(
                name = job.workflow_name,
                req=req,
                namespace=req.namespace
            )
        except NotFound:
            logger.warning(f"Could not stop workflow {job.workflow_name} for job {job.job_id}.")
        
        job.status = "created"
        engine.modify(modify_object=job)
        return Response(
            status_code=204, content="Process the job has been successfully canceled."
        )


    def debug_job(
        self,
        job_id: uuid.UUID,
        offset: Optional[str] = None,
        limit: Optional[conint(ge=1)] = None,
        user: User = Depends(Authenticator.validate),
    ):

        job = engine.get(get_model=ArgoJob, primary_key=job_id)

        if not job.workflow_name:
            raise HTTPException(404, "No Job run found for this Job.")
        
        try:
            workflow = self.workflows_service.get_workflow(
                name=job.workflow_name
            )
        except NotFound as exc:
            raise HTTPException(404, "Job run not longer available for this Job.")

        resp = requests.get(
            url=urljoin(
                self.workflows_service.host, "api/v1/workflows/{namespace}/{name}/log"
            ).format(name=workflow.metadata.name, namespace=workflow.metadata.namespace),
            params={
                "podName": None,
                "logOptions.container": "main",
                "logOptions.follow": None,
                "logOptions.previous": None,
                "logOptions.sinceSeconds": None,
                "logOptions.sinceTime.seconds": None,
                "logOptions.sinceTime.nanos": None,
                "logOptions.timestamps": None,
                "logOptions.tailLines": None,
                "logOptions.limitBytes": None,
                "logOptions.insecureSkipTLSVerifyBackend": None,
                "grep": None,
                "selector": None,
            },
            headers={"Authorization": f"Bearer {self.workflows_service.token}"},
            data=None,
            verify=self.workflows_service.verify_ssl,
        )

        logs = []
        if resp.ok:
            raw_logs = resp.content.decode("utf8").split("\n")
            logs = [
                json.loads(log)["result"]["content"]
                for log in raw_logs
                if log != "" and "content" in json.loads(log)["result"].keys()
            ]

        return JobsGetLogsResponse(
                logs=logs,
                links=[],
            ).dict(exclude_none=True)
