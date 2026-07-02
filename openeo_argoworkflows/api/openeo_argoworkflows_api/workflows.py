import json

from pydantic import SecretStr

from hera.workflows import Steps, Workflow, WorkflowsService, Step, Env
from hera.workflows.models import (
    Template,
    Container,
    Metadata,
    PersistentVolumeClaimVolumeSource,
    ResourceRequirements,
    Volume,
    VolumeMount,
    SecurityContext,
    PodSecurityContext,
)

from openeo_argoworkflows_api.settings import ExtendedAppSettings


def executor_workflow(
    service: WorkflowsService,
    process_graph: dict,
    dask_profile: dict,
    user_profile: dict,
):
    user_profile_as_json = json.dumps(user_profile)
    dask_profile_as_json = json.dumps(dask_profile)
    process_graph_as_json = json.dumps(process_graph)

    settings = ExtendedAppSettings()

    executor_env = [Env(name="STAC_API_URL", value=str(settings.STAC_API_URL))]
    if settings.STAC_API_USERNAME and settings.STAC_API_PASSWORD:
        executor_env.extend(
            [
                Env(
                    name="STAC_API_USERNAME",
                    value=settings.STAC_API_USERNAME.get_secret_value(),
                ),
                Env(
                    name="STAC_API_PASSWORD",
                    value=settings.STAC_API_PASSWORD.get_secret_value(),
                ),
            ]
        )

    # Icechunk-store (S3) access + EODAG/DEDL credentials, passed through to the
    # executor with the exact env var names that boto3, EODAG and icechunk read.
    executor_passthrough_env = {
        "AWS_DEFAULT_REGION": settings.AWS_DEFAULT_REGION,
        "AWS_ENDPOINT_URL": settings.AWS_ENDPOINT_URL,
        "AWS_ACCESS_KEY_ID": settings.AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": settings.AWS_SECRET_ACCESS_KEY,
        "EODAG__DEDL__AUTH__CREDENTIALS__USERNAME": settings.EODAG_DEDL_USERNAME,
        "EODAG__DEDL__AUTH__CREDENTIALS__PASSWORD": settings.EODAG_DEDL_PASSWORD,
        "EODAG__DEDL__PRIORITY": settings.EODAG_DEDL_PRIORITY,
        "ICECHUNK_S3_CONNECT_TIMEOUT_MS": settings.ICECHUNK_S3_CONNECT_TIMEOUT_MS,
        "ICECHUNK_S3_OPERATION_ATTEMPT_TIMEOUT_MS": settings.ICECHUNK_S3_OPERATION_ATTEMPT_TIMEOUT_MS,
        "ICECHUNK_S3_OPERATION_TIMEOUT_MS": settings.ICECHUNK_S3_OPERATION_TIMEOUT_MS,
    }
    for env_name, env_value in executor_passthrough_env.items():
        if env_value is None:
            continue
        if isinstance(env_value, SecretStr):
            env_value = env_value.get_secret_value()
        executor_env.append(Env(name=env_name, value=str(env_value)))

    with Workflow(
        generate_name="openeo-executor-",
        entrypoint="process",
        namespace=service.namespace,
        workflows_service=service,
        pod_metadata=Metadata(
            labels={
                "OPENEO_JOB_ID": user_profile["OPENEO_JOB_ID"],
                "OPENEO_USER_ID": user_profile["OPENEO_USER_ID"],
            }
        ),
        volumes=Volume(
            name="workspaces-volume",
            persistent_volume_claim=PersistentVolumeClaimVolumeSource(
                claim_name=settings.OPENEO_WORKSPACE_CLAIMNAME
            ),
        ),
        security_context=PodSecurityContext(
            fsGroup=settings.OPENEO_WORKSPACE_SECURITY_GROUP
        ),
        deletion_grace_period_seconds=1800,
    ) as w:
        with Steps(name="process"):
            Step(
                name="process-graph",
                template=Template(
                    name="executor",
                    container=Container(
                        env=executor_env,
                        image=settings.OPENEO_EXECUTOR_IMAGE,
                        image_pull_policy=settings.OPENEO_EXECUTOR_IMAGE_PULL_POLICY,
                        resources=ResourceRequirements(
                            requests={
                                "cpu": settings.OPENEO_EXECUTOR_CPU_REQUEST,
                                "memory": settings.OPENEO_EXECUTOR_MEMORY_REQUEST,
                            },
                            limits={
                                "cpu": settings.OPENEO_EXECUTOR_CPU_LIMIT,
                                "memory": settings.OPENEO_EXECUTOR_MEMORY_LIMIT,
                            },
                        ),
                        command=["openeo_executor"],
                        args=[
                            "execute",
                            "--process_graph",
                            process_graph_as_json,
                            "--user_profile",
                            user_profile_as_json,
                            "--dask_profile",
                            dask_profile_as_json,
                        ],
                        volume_mounts=[
                            VolumeMount(
                                name="workspaces-volume",
                                mount_path=settings.OPENEO_MOUNT_PATH,
                            )
                        ],
                        security_context=SecurityContext(
                            runAsUser=1000,
                            runAsGroup=settings.OPENEO_WORKSPACE_SECURITY_GROUP,
                        ),
                    ),
                ),
            )

    return w
