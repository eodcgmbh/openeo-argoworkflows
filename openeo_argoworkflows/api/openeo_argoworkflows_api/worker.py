import os

from redis import Redis
from rq import Worker, Queue, Connection

from openeo_argoworkflows_api.settings import ExtendedAppSettings

settings = ExtendedAppSettings()

conn = Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT
)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, ['default']))
        worker.work()