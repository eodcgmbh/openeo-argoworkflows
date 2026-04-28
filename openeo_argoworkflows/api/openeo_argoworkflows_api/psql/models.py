from typing import List, Optional
from openeo_fastapi.client.auth import User
from openeo_fastapi.client.jobs import Job
from openeo_fastapi.client.psql.settings import BASE
from openeo_fastapi.client.psql.models import *
from sqlalchemy.dialects.postgresql import ARRAY


class ExtendedUserORM(UserORM):

    roles = Column(ARRAY(VARCHAR), nullable=True, default=[])
    """Roles assigned to the user, sourced from the OIDC userinfo response."""


class ExtendedUser(User):

    roles: List[str] = []
    """Roles assigned to the user, sourced from the OIDC userinfo response."""

    @classmethod
    def get_orm(cls):
        return ExtendedUserORM


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
