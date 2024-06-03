from fastapi.testclient import TestClient

from openeo_argoworkflows.app import client
from openeo_argoworkflows.jobs import ArgoJobsRegister

def test_jobs_is_argo():
    """Test the OpenEOApi and OpenEOCore classes interact as intended."""

    assert isinstance(client.jobs, ArgoJobsRegister)
