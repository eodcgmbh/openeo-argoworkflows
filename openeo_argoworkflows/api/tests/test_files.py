import fsspec
import json
from unittest.mock import patch

from fastapi.testclient import TestClient
from openeo_fastapi.client.psql.engine import create
from uuid import uuid4

from openeo_argoworkflows_api.app import app as app_api

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

    resp = app.get(f"/1.1.0/files/{file_in_workspace}", headers=headers)

    assert resp.status_code == 200



