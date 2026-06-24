import logging
import os

from redis import Redis
from rq import Worker, Queue, Connection

from openeo_argoworkflows_api.settings import ExtendedAppSettings

settings = ExtendedAppSettings()

logging.basicConfig(level=settings.LOG_LEVEL.upper(), force=True)

conn = Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT
)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, ['default']))
        # with_scheduler=True runs rq's built-in scheduler in-process so delayed
        # jobs (q.enqueue_in, used by poll_job_status and queue_to_submit) actually
        # execute. Without it, scheduled jobs sit in the ScheduledJobRegistry
        # forever and job status never transitions out of "running".
        worker.work(with_scheduler=True)