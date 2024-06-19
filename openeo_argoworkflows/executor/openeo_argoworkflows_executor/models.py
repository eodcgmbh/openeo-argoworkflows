from pathlib import Path
from pydantic import BaseModel, model_validator
from typing import Any, Optional
from openeo_pg_parser_networkx.graph import OpenEOProcessGraph


class UserProfile(BaseModel):

    OPENEO_USER_ID: str
    OPENEO_JOB_ID: str
    OPENEO_USER_WORKSPACE: Path

    @property
    def results_path(self) -> Path:
        return self.OPENEO_USER_WORKSPACE / "RESULTS"

    @property
    def stac_path(self) -> Path:
        return self.OPENEO_USER_WORKSPACE / "STAC"


class ClusterProfile(BaseModel):


    GATEWAY_URL: Optional[str] = None
    OPENEO_EXECUTOR_IMAGE: Optional[str] = None
    
    LOCAL: bool = False

    CLUSTER_IDLE_TIMEOUT: int = 3600

    WORKER_CORES: int = 4
    WORKER_MEMORY: int = 8
    WORKER_LIMIT: int = 4

    @model_validator(mode='before')
    @classmethod
    def when_local_omit_all(cls, data: Any) -> Any:
    
        assert not (
            "LOCAL" in data.keys() and ( "GATEWAY_URL" in data.keys() or "OPENEO_EXECUTOR_IMAGE" in data.keys() )
        ), "Cannot initialise a local cluster and also a remote cluster"
        return data



class ExecutorParameters(BaseModel):

    process_graph: dict
    user_profile: UserProfile
    dask_profile: ClusterProfile
