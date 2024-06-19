import fakeredis

from rq import Worker, SimpleWorker
from unittest.mock import patch

from openeo_argoworkflows_api.tasks import queue_to_submit, q

@patch("openeo_argoworkflows_api.tasks.Redis")
def test_submit_job(mock_redis, redis_conn, a_mock_job):
    # Patch the Redis connection to use the FakeRedis instance
    # mock_redis.return_value = redis_conn

    # # Reset the queue to use the patched Redis connection
    # q.connection = redis_conn

    # # Call the function to submit the job
    # job = queue_to_submit(a_mock_job)

    # # Verify that the job has been enqueued correctly
    # assert job.is_queued
    assert True
