import datetime
import fsspec

from fastapi.testclient import TestClient
from unittest.mock import patch

from openeo_fastapi.client.psql.engine import create
from openeo_argoworkflows_api.app import client
from openeo_argoworkflows_api.jobs import ArgoJobsRegister, UserWorkspace
from openeo_argoworkflows_api.auth import ExtendedAuthenticator
from openeo_argoworkflows_api.settings import ExtendedAppSettings

from openeo_argoworkflows_api.app import app as app_api

def test_jobs_is_argo():
    """Test the OpenEOApi and OpenEOCore classes interact as intended."""

    assert isinstance(client.jobs, ArgoJobsRegister)

def test_app_settings():

    blank_policies = ExtendedAppSettings(
        OIDC_POLICIES = ""
    )
    assert not blank_policies.OIDC_POLICIES

    single_policy = ExtendedAppSettings(
        OIDC_POLICIES = "groups, /staff"
    )
    assert single_policy.OIDC_POLICIES == ["groups,/staff"]

    multi_policy = ExtendedAppSettings(
        OIDC_POLICIES = "groups, /staff && groups, /users"
    )
    assert multi_policy.OIDC_POLICIES == ["groups,/staff", "groups,/users"]

def test_signed_urls(a_mock_user, a_mock_job, mock_settings):
    """ """

    fs = fsspec.filesystem(protocol="file")
    a_mock_job.status == "finished"

    create(a_mock_user)
    create(a_mock_job)

    workspace = UserWorkspace(
        user_id=str(a_mock_user.user_id),
        job_id=str(a_mock_job.job_id),
        root_dir=mock_settings.OPENEO_WORKSPACE_ROOT.parent / "out"
    )


    # Ensure 'users workspace' exists
    fs.mkdir(workspace.user_directory)
    fs.mkdir(workspace.job_directory)
    fs.mkdir(workspace.results_directory)

    original_file = str(
        mock_settings.OPENEO_WORKSPACE_ROOT.parent / "fake-process-graph.json"
    )

    fs.copy(
        original_file, str(workspace.results_directory)
    )

    test_path = f"{mock_settings.OPENEO_PREFIX}/files/{str(a_mock_job.job_id)}/RESULTS/fake-process-graph.json"

    signed = ExtendedAuthenticator.sign_url(
        test_path, "OPENEO_SIGN_KEY", str(a_mock_user.user_id), datetime.datetime.fromtimestamp(1678692590)
    )

    app = TestClient(app_api)

    resp = app.get(signed)
    assert resp.status_code == 200


def test_get_credentials(mock_settings):
    """ """

    test_path = f"{mock_settings.OPENEO_PREFIX}/credentials/oidc"

    app = TestClient(app_api)

    resp = app.get(test_path)
    assert resp.status_code == 200


def test_get_wellknown(mock_settings):
    """ """

    test_path = f"/.well-known/openeo"

    app = TestClient(app_api)

    resp = app.get(test_path)
    assert resp.status_code == 200

    test_path = f"{mock_settings.OPENEO_VERSION}/.well-known/openeo"

    app = TestClient(app_api)

    resp = app.get(test_path)
    assert resp.status_code == 200