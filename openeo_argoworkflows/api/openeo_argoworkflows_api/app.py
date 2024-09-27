
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from openeo_fastapi.api.app import OpenEOApi
from openeo_fastapi.api.types import Billing, Plan, FileFormat, GisDataType
from openeo_fastapi.client.core import OpenEOCore

from openeo_argoworkflows_api.auth import get_credentials_oidc
from openeo_argoworkflows_api.jobs import ArgoJobsRegister
from openeo_argoworkflows_api.files import ArgoFileRegister
from openeo_argoworkflows_api.settings import ExtendedAppSettings


gtif = FileFormat(
   title="GTiff",
    gis_data_types=[GisDataType("raster")],
    parameters={},
)

netcdf = FileFormat(
   title="netCDF",
    gis_data_types=[GisDataType("raster")],
    parameters={},
)

input_formats = [ gtif, netcdf ]
output_formats = [ netcdf ]

links = []

settings = ExtendedAppSettings()

client = OpenEOCore(
    settings=settings,
    files=ArgoFileRegister(settings=settings, links=links),
    jobs=ArgoJobsRegister(settings=settings, links=links),
    input_formats=input_formats,
    output_formats=output_formats,
    links=links,
    billing=Billing(
        currency="credits",
        default_plan="a-cloud",
        plans=[Plan(name="user", description="Subscription plan.", paid=True)],
    )
)
app = FastAPI()

app.router.add_api_route(
    name="file_headers",
    path=f"/{client.settings.OPENEO_VERSION}/files" + "/{path:path}",
    response_model=None,
    response_model_exclude_unset=False,
    response_model_exclude_none=True,
    methods=["HEAD"],
    endpoint=client.files.file_header,
)

api = OpenEOApi(client=client, app=app)

def redirect_wellknown():
    return RedirectResponse("/.well-known/openeo")
    
api.app.router.add_api_route(
    name="redirect_wellknown",
    path=f"/{client.settings.OPENEO_VERSION}/.well-known/openeo",
    response_model=None,
    response_model_exclude_unset=False,
    response_model_exclude_none=True,
    methods=["GET"],
    endpoint=redirect_wellknown,
)

app = api.app