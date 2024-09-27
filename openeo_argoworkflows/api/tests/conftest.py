import datetime
import fsspec
import importlib.metadata
import pytest
import os
import uuid

from fakeredis import FakeStrictRedis
from pathlib import Path
from openeo_fastapi.client.auth import User
from openeo_fastapi.api.types import Link
from unittest.mock import patch

fs = fsspec.filesystem(protocol="file")

__version__ = importlib.metadata.version("openeo_fastapi")

OPENEO_WORKSPACE_ROOT = "/openeo-argoworkflows-api/tests/data/out"
ALEMBIC_DIR = Path(__file__).parent.parent / "openeo_argoworkflows_api/psql/"

SETTINGS_DICT = {
        "API_DNS": "test.api.org",
        "API_TLS": "False",
        "API_TITLE": "OpenEO Argo Api",
        "API_DESCRIPTION": "Testing the OpenEO Argo Api",
        "STAC_API_URL": "http://test-stac-api.mock.com/api/",
        "OIDC_URL": "http://test-oidc-api.mock.com/api/",
        "OIDC_ORGANISATION": "issuer",
        "OPENEO_WORKSPACE_ROOT": OPENEO_WORKSPACE_ROOT,
        "ARGO_WORKFLOWS_SERVER": "http://not.real.argo.com/api/",
        "ARGO_WORKFLOWS_NAMESPACE": "testing",
        "ARGO_WORKFLOWS_TOKEN": "atoken",
        "OPENEO_EXECUTOR_IMAGE": "testimage:2024.6.1",
        "OPENEO_SIGN_KEY": "xx9Yp6whivS0wrC2CmIhxlJAMbfDugZw"
    }

for k, v in SETTINGS_DICT.items():
    os.environ[k] = str(v)

from openeo_argoworkflows_api.jobs import ArgoJob
from openeo_argoworkflows_api.settings import ExtendedAppSettings

def mock_user():
    return User(
        user_id = uuid.uuid4(), oidc_sub="testuser@testing.eu"
    )
    
def mock_job():
    return ArgoJob(
        job_id=uuid.uuid4(),
        process_graph_id="testgraph",
        status="created",
        user_id=uuid.uuid4(),
        created=datetime.datetime.now(),
        description="old description",
        process={"x":{"y": 2}}
    )


@pytest.fixture(scope="function")
def a_mock_user():
    return mock_user()

@pytest.fixture(scope="function")
def a_mock_job():
    return mock_job()

@pytest.fixture(scope="function")
def mock_links(uuid: uuid.UUID = uuid.uuid4()):
    
    return [
        Link(
            href="https://eodc.eu/",
            rel="about",
            type="text/html",
            title="Homepage of the service provider",
        )
    ]

@pytest.fixture(scope="function")
def mock_settings():
    return ExtendedAppSettings(**SETTINGS_DICT)


        
@pytest.fixture(autouse=True)
def mock_engine(postgresql):
    """Postgresql engine for SQLAlchemy."""
    import os
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    from openeo_fastapi.client.psql.engine import get_engine

    os.chdir(Path(ALEMBIC_DIR))

    # Set the env vars that alembic will use for DB connection and run alembic engine from CLI!
    os.environ["POSTGRES_USER"] = postgresql.info.user
    os.environ["POSTGRES_PASSWORD"] = "postgres"
    os.environ["POSTGRESQL_HOST"] = postgresql.info.host
    os.environ["POSTGRESQL_PORT"] = str(postgresql.info.port)
    os.environ["POSTGRES_DB"] = postgresql.info.dbname
    os.environ["ALEMBIC_DIR"] = str(ALEMBIC_DIR)

    alembic_cfg = Config("alembic.ini")

    command.upgrade(alembic_cfg, "head")

    engine = get_engine()

    return engine

@pytest.fixture(scope="module", autouse=True)
def cleanup_out_folder():
    
    if not fs.exists(OPENEO_WORKSPACE_ROOT):
        fs.mkdir(OPENEO_WORKSPACE_ROOT)

    yield  # Yield to the running tests

    # Teardown: Delete the output folder,
    if fs.exists(OPENEO_WORKSPACE_ROOT):
        fs.rm(OPENEO_WORKSPACE_ROOT, recursive=True)
        


@pytest.fixture
def redis_conn():
    return FakeStrictRedis(decode_responses=True)

def mock_auth(status_code=200):
    import json
    from fastapi import Response

    user = mock_user()

    if not status_code:
        status_code = 200
    fake_sub_id = "testuser@eodc.eu"
    test_resp_content_bytes = json.dumps(
        {
            "userinfo_endpoint": "http://a.fake_url_again.cloud/",
            "eduperson_entitlement": [
                "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu",
                "urn:mace:egi.eu:group:vo.openeo.cloud:role=platform_developer#aai.egi.eu",
            ],
            "sub": user.oidc_sub,
        }
    ).encode("utf-8")

    mocked_response = Response()
    mocked_response.status_code = status_code
    mocked_response._content = test_resp_content_bytes

    return {"user": user.user_id, "sub": user.oidc_sub, "resp": mocked_response}
