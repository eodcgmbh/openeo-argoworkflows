import fsspec
import json
from unittest.mock import patch

from fastapi.testclient import TestClient
from openeo_fastapi.client.psql.engine import create
from uuid import uuid4

from openeo_argoworkflows_api.app import app as app_api
from openeo_argoworkflows_api.jobs import UserWorkspace

@patch("openeo_fastapi.client.auth.Authenticator.validate")
def test_file_download(user_validate, a_mock_user, mock_settings):
    
    user_validate.return_value = a_mock_user

    fs = fsspec.filesystem(protocol="file")
    user_id = uuid4()

    create(create_object=a_mock_user)

    app = TestClient(app_api)

    # Ensure 'users workspace' exists
    user_dir = mock_settings.OPENEO_WORKSPACE_ROOT / user_id.__str__()
    fs.mkdir(str(user_dir))

    original_file = (
        mock_settings.OPENEO_WORKSPACE_ROOT.parent / "fake-process-graph.json"
    )
    file_in_workspace = user_dir / "process_graph.json"

    assert user_dir.exists()

    fs.copy(str(original_file), str(file_in_workspace))

    assert file_in_workspace.exists()

    headers = {"Authorization": "Bearer /oidc/egi/toetoetoeken"}

    resp = app.get(f"{mock_settings.OPENEO_PREFIX}/files/{file_in_workspace}", headers=headers)

    assert resp.status_code == 200

def test_file_formats(mock_settings):
    
    app = TestClient(app_api)

    resp = app.get(f"{mock_settings.OPENEO_PREFIX}/file_formats")

    json_out = resp.json()

    assert resp.status_code == 200
    assert len(json_out["input"]) == 2
    assert len(json_out["output"]) == 1


@patch("openeo_fastapi.client.auth.Authenticator.validate")
def test_file_list(user_validate, a_mock_user, mock_settings):
    
    user_validate.return_value = a_mock_user

    fs = fsspec.filesystem(protocol="file")

    create(create_object=a_mock_user)

    app = TestClient(app_api)

    # Ensure 'users workspace' exists
    user_workspace = UserWorkspace(
        root_dir=mock_settings.OPENEO_WORKSPACE_ROOT,
        user_id=str(a_mock_user.user_id)
    )

    original_file = (
        mock_settings.OPENEO_WORKSPACE_ROOT.parent / "fake-process-graph.json"
    )
    file_in_workspace = user_workspace.files_directory / "process_graph.json"
    file_in_workspace_2 = user_workspace.files_directory / "process_graph_2.json"

    fs.copy(str(original_file), str(file_in_workspace))
    fs.copy(str(original_file), str(file_in_workspace_2))

    assert file_in_workspace.exists()

    headers = {"Authorization": "Bearer oidc/egi/toetoetoeken"}

    resp = app.get(f"{mock_settings.OPENEO_PREFIX}/files", headers=headers)

    assert resp.status_code == 200
    assert len(resp.json()["files"]) == 2


@patch("openeo_fastapi.client.auth.Authenticator.validate")
def test_file_upload(user_validate, a_mock_user, mock_settings):
    
    fs = fsspec.filesystem(protocol="file")

    user_validate.return_value = a_mock_user

    create(create_object=a_mock_user)

    app = TestClient(app_api)

    original_file = (
        mock_settings.OPENEO_WORKSPACE_ROOT.parent / "fake-process-graph.json"
    )

    headers = {"Authorization": "Bearer oidc/egi/toetoetoeken"}

    with open(original_file, "rb") as f:
        resp = app.put(f"{mock_settings.OPENEO_PREFIX}/files/fake-process-graph.json", headers=headers, files={'file': f})

    assert resp.status_code == 200
    assert resp.json()["path"] == "fake-process-graph.json"

    expected_file = mock_settings.OPENEO_WORKSPACE_ROOT / f"{str(a_mock_user.user_id)}/FILES/fake-process-graph.json"

    assert fs.exists(expected_file)

    with open(original_file, "rb") as f:
        resp = app.put(f"{mock_settings.OPENEO_PREFIX}/files/fake-process-graph-2.json", headers=headers, data=f)

    assert resp.status_code == 200
    assert resp.json()["path"] == "fake-process-graph-2.json"

    expected_file = mock_settings.OPENEO_WORKSPACE_ROOT / f"{str(a_mock_user.user_id)}/FILES/fake-process-graph-2.json"

    assert fs.exists(expected_file)



@patch("openeo_fastapi.client.auth.Authenticator.validate")
def test_file_delete(user_validate, a_mock_user, mock_settings):
    
    user_validate.return_value = a_mock_user

    fs = fsspec.filesystem(protocol="file")

    create(create_object=a_mock_user)

    app = TestClient(app_api)

    # Ensure 'users workspace' exists
    user_workspace = UserWorkspace(
        root_dir=mock_settings.OPENEO_WORKSPACE_ROOT,
        user_id=str(a_mock_user.user_id)
    )

    original_file = (
        mock_settings.OPENEO_WORKSPACE_ROOT.parent / "fake-process-graph.json"
    )
    file_in_workspace = user_workspace.files_directory / "process_graph.json"

    fs.copy(str(original_file), str(file_in_workspace))

    assert file_in_workspace.exists()

    headers = {"Authorization": "Bearer oidc/egi/toetoetoeken"}

    resp = app.delete(f"{mock_settings.OPENEO_PREFIX}/files/process_graph.json", headers=headers)
    assert resp.status_code == 204

    resp = app.delete(f"{mock_settings.OPENEO_PREFIX}/files/process_graph", headers=headers)
    assert resp.status_code == 404