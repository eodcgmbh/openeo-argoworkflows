from typing import List, Optional
from openeo_fastapi.client.auth import User
from openeo_fastapi.client.jobs import Job
from openeo_fastapi.client.psql.settings import BASE
from openeo_fastapi.client.psql.models import *
from sqlalchemy import Column as _Column
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import column_property as _column_property

# Extend UserORM's table and mapper with a roles column rather than subclassing,
# so there is only one SQLAlchemy mapper for the users table.
_roles_col = _Column('roles', ARRAY(VARCHAR), nullable=True)
UserORM.__table__.append_column(_roles_col)
UserORM.__mapper__.add_property('roles', _column_property(_roles_col))


class ExtendedUser(User):

    roles: List[str] = []
    """Roles assigned to the user, sourced from the OIDC userinfo response."""

    @classmethod
    def get_orm(cls):
        return UserORM


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
