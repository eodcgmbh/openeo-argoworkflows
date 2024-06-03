
from fastapi import FastAPI

from openeo_fastapi.api.app import OpenEOApi
from openeo_fastapi.api.types import Billing, Plan
from openeo_fastapi.client.core import OpenEOCore

from openeo_argoworkflows.jobs import ArgoJobsRegister
from openeo_argoworkflows.settings import ExtendedAppSettings

formats = []

links = []

settings = ExtendedAppSettings()

client = OpenEOCore(
    settings=settings,
    jobs=ArgoJobsRegister(settings=settings, links=links),
    input_formats=formats,
    output_formats=formats,
    links=links,
    billing=Billing(
        currency="credits",
        default_plan="a-cloud",
        plans=[Plan(name="user", description="Subscription plan.", paid=True)],
    )
)

api = OpenEOApi(client=client, app=FastAPI())

app = api.app
