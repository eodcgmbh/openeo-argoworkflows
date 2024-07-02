
from fastapi import FastAPI

from openeo_fastapi.api.app import OpenEOApi
from openeo_fastapi.api.types import Billing, Plan
from openeo_fastapi.client.core import OpenEOCore

from openeo_argoworkflows_api.jobs import ArgoJobsRegister
from openeo_argoworkflows_api.files import ArgoFileRegister
from openeo_argoworkflows_api.settings import ExtendedAppSettings

formats = []

links = []

settings = ExtendedAppSettings()

client = OpenEOCore(
    settings=settings,
    files=ArgoFileRegister(settings=settings, links=links),
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

api.app.router.add_api_route(
    name="file_headers",
    path=f"/{api.client.settings.OPENEO_VERSION}/files" + "/{path:path}",
    response_model=None,
    response_model_exclude_unset=False,
    response_model_exclude_none=True,
    methods=["HEAD"],
    endpoint=api.client.files.file_header,
)

app = api.app