from typing import Optional
from openeo_fastapi.client.jobs import Job
from openeo_fastapi.client.psql.settings import BASE
from openeo_fastapi.client.psql.models import *

class ArgoJobORM(JobORM):

    workflowname = Column(VARCHAR, nullable=True)
    """The name of the argo workflow."""


class ArgoJob(Job):

    workflowname: Optional[str]
    """The name of the argo workflow."""

    @classmethod
    def get_orm(cls):
        return ArgoJobORM


metadata = BASE.metadata
