import pytest

from openeo_fastapi.api.models import JobsRequest
from openeo_fastapi.client.psql.engine import create, get

from openeo_argoworkflows_api.jobs import ArgoJobsRegister

@pytest.mark.skip("Not ready")
def test_start_job(a_mock_user, a_mock_job, mock_links, mock_settings, mocked_validate_user):
    
    create(a_mock_job)
        
    argo_register = ArgoJobsRegister(
        links = mock_links,
        settings = mock_settings
    )
    
    resp = argo_register.start_job(
        a_mock_job.job_id, a_mock_user
    )

    assert resp.status_code == 202
    
    job = get(a_mock_job, a_mock_job.job_id)
    
    assert job.status.value == "queued"

@pytest.mark.skip("Not ready")
def test_run_sync_job(a_mock_user, a_mock_job, mock_links, mock_settings):
    
    create(a_mock_job)
        
    argo_register = ArgoJobsRegister(
        links = mock_links,
        settings = mock_settings
    )


    import json
    with open(mock_settings.OPENEO_WORKSPACE_ROOT.parent / "fake-simple-pg.json") as f:
        json_data = json.load(f)

    job = JobsRequest(title="test", process=json_data)  
    
    resp = argo_register.process_sync_job(
        job, a_mock_user
    )

    assert resp.status_code == 502
    
