from uuid import uuid4

from fastapi.testclient import TestClient

from openeo_fastapi.client.psql.engine import create, get

def test_start_job(a_mock_user, a_mock_job, mock_links, mock_settings, mocked_validate_user):
    
    create(a_mock_job)
    
    from openeo_argoworkflows.jobs import ArgoJobsRegister
    
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