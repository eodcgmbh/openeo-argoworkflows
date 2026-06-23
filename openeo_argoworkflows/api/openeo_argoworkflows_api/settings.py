from pathlib import Path
from pydantic import AnyUrl, Field, SecretStr
from typing import Optional

from openeo_fastapi.client.settings import AppSettings


class ExtendedAppSettings(AppSettings):

    OPENEO_WORKSPACE_ROOT: Optional[Path]
    OPENEO_MOUNT_PATH: Optional[str] = "/eodc"
    OPENEO_WORKSPACE_CLAIMNAME: Optional[str]
    OPENEO_WORKSPACE_SECURITY_GROUP: Optional[int]
    OPENEO_EXECUTOR_IMAGE: Optional[str]
    OPENEO_SIGN_KEY: Optional[str]

    STAC_API_USERNAME: Optional[SecretStr] = None
    STAC_API_PASSWORD: Optional[SecretStr] = None

    # Credentials/config passed through to the executor for icechunk-store (S3)
    # access and EODAG/DEDL data loading. The EODAG variables keep their exact
    # `EODAG__DEDL__...` env names. NOTE: this runs on pydantic v1, where the
    # binding kwarg is `env=`; `validation_alias=` (pydantic v2) is silently
    # ignored and the field falls back to its name (`EODAG_DEDL_*`), so the
    # `EODAG__DEDL__*` env vars never bind and forward as None.
    AWS_DEFAULT_REGION: Optional[str] = None
    AWS_ENDPOINT_URL: Optional[str] = None
    AWS_ACCESS_KEY_ID: Optional[SecretStr] = None
    AWS_SECRET_ACCESS_KEY: Optional[SecretStr] = None

    EODAG_DEDL_USERNAME: Optional[SecretStr] = Field(
        default=None, env="EODAG__DEDL__AUTH__CREDENTIALS__USERNAME"
    )
    EODAG_DEDL_PASSWORD: Optional[SecretStr] = Field(
        default=None, env="EODAG__DEDL__AUTH__CREDENTIALS__PASSWORD"
    )
    EODAG_DEDL_PRIORITY: Optional[str] = Field(
        default=None, env="EODAG__DEDL__PRIORITY"
    )

    ICECHUNK_S3_CONNECT_TIMEOUT_MS: Optional[str] = None
    ICECHUNK_S3_OPERATION_ATTEMPT_TIMEOUT_MS: Optional[str] = None
    ICECHUNK_S3_OPERATION_TIMEOUT_MS: Optional[str] = None

    ARGO_WORKFLOWS_SERVER: Optional[AnyUrl]
    ARGO_WORKFLOWS_NAMESPACE: Optional[str]
    ARGO_WORKFLOWS_TOKEN: Optional[SecretStr]
    ARGO_WORKFLOWS_LIMIT: int = 10

    DASK_GATEWAY_SERVER: Optional[str]
    DASK_WORKER_CORES: str = "4"
    DASK_WORKER_MEMORY: str = "8"
    DASK_WORKER_LIMIT: str = "6"
    DASK_CLUSTER_IDLE_TIMEOUT: str = "3600"

    DASK_PROFILES: Optional[str] = None
    DASK_ROLE_PROFILE_MAPPING: Optional[str] = None

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    OIDC_ROLES_CLAIM: Optional[str] = None

    LOG_LEVEL: str = "INFO"
