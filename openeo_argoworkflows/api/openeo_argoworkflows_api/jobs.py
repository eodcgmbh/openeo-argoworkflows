import datetime
import fsspec
import json
import logging
import os
import io
import requests
import tarfile
import time
import uuid

from hera.exceptions import NotFound
from hera.workflows import WorkflowsService
from hera.workflows.models import WorkflowStopRequest
from hera.exceptions import NotFound
from fastapi import Depends, Response, HTTPException, responses
from typing import Optional
from pathlib import Path
from pydantic import conint, BaseModel
from pystac import Collection, Link as StacLink
from redis import Redis
from rq import Queue
from sqlalchemy.exc import IntegrityError
from typing import Union
from urllib.parse import urljoin

from openeo_fastapi.api.types import Status, Error
from openeo_fastapi.api.models import JobsGetLogsResponse, JobsRequest
from openeo_fastapi.client.psql import engine
from openeo_fastapi.client.jobs import JobsRegister
from openeo_fastapi.client.auth import Authenticator, User

from openeo_argoworkflows_api.auth import ExtendedAuthenticator
from openeo_argoworkflows_api.psql.models import ArgoJob
from openeo_argoworkflows_api.tasks import queue_to_submit, submit_job


fs = fsspec.filesystem(protocol="file")

logger = logging.getLogger(__name__)


class UserWorkspace(BaseModel):

    root_dir: Path
    user_id: Union[str, uuid.UUID]
    job_id: Union[str, uuid.UUID]

    @property
    def user_directory(self):
        return self.root_dir / str(self.user_id)
    
    @property
    def job_directory(self):
        return self.user_directory / str(self.job_id)
    
    @property
    def stac_directory(self):
        return self.job_directory / "STAC"

    @property
    def results_directory(self):
        return self.job_directory / "RESULTS"
    
    @property
    def results_collection_json(self):
        return self.stac_directory / f"{self.job_id}_collection.json"


class ArgoJobsRegister(JobsRegister):

    def __init__(self, settings, links) -> None:
        super().__init__(settings, links)

        self.workflows_service = WorkflowsService(
            host=settings.ARGO_WORKFLOWS_SERVER, verify_ssl=False, namespace=settings.ARGO_WORKFLOWS_NAMESPACE
        )

        self.q = Queue(
            connection=Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT
        ))

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


    def get_results(
        self, job_id: uuid.UUID, user: User = Depends(ExtendedAuthenticator.signed_url_or_validate)
    ):
        """Get the results for the BatchJob.

        Args:
            job_id (JobId): A UUID job id.
            body (JobsRequest): The Job Request that should be used to create the new BatchJob.
            user (User): The User returned from the Authenticator.

        Raises:
            HTTPException: Raises an exception with relevant status code and descriptive message of failure.

        """

        job = engine.get(get_model=ArgoJob, primary_key=job_id)

        wspace = UserWorkspace(
            root_dir=self.settings.OPENEO_WORKSPACE_ROOT, user_id=str(user.user_id), job_id=str(job.job_id)
        )

        stac_collection = Collection.from_file(str(wspace.results_collection_json))

        new_links = [link for link in stac_collection.links if link.rel != "item"]

        if self.settings.API_TLS:
            API_SELF_URL = f"https://{self.settings.API_DNS}"
        else:
            API_SELF_URL= f"http://{self.settings.API_DNS}"

        self_url = f"{self.settings.OPENEO_PREFIX}/jobs/{str(job.job_id)}/results"

        for link in new_links:
            link._target_href = API_SELF_URL.__add__(self_url)

        # Sign urls
        now = datetime.datetime.now().replace(microsecond=0)
        week = datetime.timedelta(days=7)
        expiry = now + week

        canonical_url = API_SELF_URL.__add__(
            ExtendedAuthenticator.sign_url(
                url=self_url,
                key_name="OPENEO_SIGN_KEY",
                user_id=user.user_id,
                expiration_time=expiry
            )
        )
        new_links.append(StacLink(rel="canonical", target=canonical_url))

        stac_collection.links = new_links

        for value in stac_collection.assets.values():
            file_name = value.href.split("/")[-1]
            relative_path = "/{job_id}/RESULTS/{file}".format(
                user_id=user, job_id=job_id, file=file_name
            )
            path ="{prefix}/files{path}".format(prefix=self.settings.OPENEO_PREFIX, path=relative_path)

            value.href = API_SELF_URL.__add__(
                ExtendedAuthenticator.sign_url(
                    url=path,
                    key_name="OPENEO_SIGN_KEY",
                    user_id=user.user_id,
                    expiration_time=expiry
                )
            )

        stac_collection.summaries.add(
            "datetime", {
                "minimum": str(stac_collection.extent.temporal.intervals[0][0]),
                "maximum": str(stac_collection.extent.temporal.intervals[0][1])
            }
        )

        stac_collection.extra_fields.update({"openeo:status": "finished"})

        return stac_collection.to_dict()
    

    def process_sync_job(self, body: JobsRequest = JobsRequest(), user: User = Depends(Authenticator.validate)):
        """Start the processing of a synchronous Job.

        Args:
            body (JobsRequest): The Job Request that should be used to create the new BatchJob.
            user (User): The User returned from the Authenticator.

        Raises:
            HTTPException: Raises an exception with relevant status code and descriptive message of failure.
                        
        """

        # Ensure there is a record of this sync job run
        job_id = uuid.uuid4()

        if not body.process.id:
            auto_name_size = 16
            body.process.id = uuid.uuid4().hex[:auto_name_size].upper()

        # Create the job
        job = ArgoJob(
            job_id=job_id,
            process=body.process,
            status=Status.queued,
            title=body.title,
            description=f"Synchronous execution of process graph {body.process.id}.",
            user_id=user.user_id,
            created=datetime.datetime.now(),
            synchronous=True
        )

        engine.create(create_object=job)

        self.q.enqueue(submit_job, job)

        job_finished = False

        # Needs to wait for completion
        while not job_finished:
            job = engine.get(ArgoJob, job.job_id)
            if job.status == Status.finished:
                job_finished = True
            elif job.status == Status.error:
                raise HTTPException(
                    status_code=500,
                    detail=Error(code="InternalServerError", message="Failed to process. Submit as batch job to view logs."),
                )
            elif job.status == Status.running:
                time.sleep(15)

        wspace = UserWorkspace(
            root_dir=self.settings.OPENEO_WORKSPACE_ROOT, user_id=str(user.user_id), job_id=str(job.job_id)
        )

        files = [file for file in wspace.results_directory.glob(f"*") if file.is_file()]

        if len(files) == 0:
            raise HTTPException(
                status_code=500,
                detail=f"No files to return for request.",
            )
        elif len(files) == 1:
            def single_file_iterator(file_path):
                with open(file_path, "rb") as file:
                    yield from file
            
            file = files[0]

            # TODO Improve, maybe move general functionality of mimetypes to openeo-fastapi
            extention = os.path.splitext(file)[1]
            mime_types = {
                ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
                ".nc": "application/netcdf",
                ".json": "application/json"
            }
            mime_type = mime_types[extention]

            response = responses.StreamingResponse(
                single_file_iterator(file),
                200,
                headers={
                    "Content-Disposition": f'attachment; filename="{os.path.basename(file)}"',
                    "Content-Type": mime_type,
                },
            )

        else:
            tar_buffer = io.BytesIO()
            with tarfile.open(mode="w", fileobj=tar_buffer) as tar:
                for file_path in files:
                    tar.add(file_path, arcname=os.path.basename(file_path))

            def tar_file_iterator(tar_buffer):
                tar_buffer.seek(0)
                yield from tar_buffer

            response = responses.StreamingResponse(
                tar_file_iterator(tar_buffer),
                200,
                headers={
                    "Content-Disposition": 'attachment; filename="archive.tar"',
                    "Content-Type": "application/x-tar",
                },
            )
        return response

    

