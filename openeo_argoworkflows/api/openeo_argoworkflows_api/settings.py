from pathlib import Path
from pydantic import AnyUrl, SecretStr
from typing import Optional

from openeo_fastapi.client.settings import AppSettings

class ExtendedAppSettings(AppSettings):
        
    OPENEO_WORKSPACE_ROOT: Optional[Path]
    OPENEO_EXECUTOR_IMAGE: Optional[str]
    OPENEO_SIGN_KEY: Optional[str]

    ARGO_WORKFLOWS_SERVER: Optional[AnyUrl]
    ARGO_WORKFLOWS_NAMESPACE: Optional[str]
    ARGO_WORKFLOWS_TOKEN: Optional[SecretStr]
    ARGO_WORKFLOWS_LIMIT: int = 10
    
    DASK_GATEWAY_SERVER: Optional[str]
    DASK_WORKER_CORES: str = "4"
    DASK_WORKER_MEMORY: str = "8"
    DASK_WORKER_LIMIT: str = "6"
    DASK_CLUSTER_IDLE_TIMEOUT: str = "3600"

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379