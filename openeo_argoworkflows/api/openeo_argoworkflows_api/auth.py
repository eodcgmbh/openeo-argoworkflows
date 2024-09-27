import base64
import datetime
import hashlib
import hmac
import logging

from fastapi import HTTPException, Request
from openeo_fastapi.client.auth import Authenticator, User
from openeo_fastapi.client.psql import engine
from pydantic import BaseModel, ValidationError, validator
from urllib import parse
from uuid import UUID

from openeo_argoworkflows_api.settings import ExtendedAppSettings

logger = logging.getLogger("")

# Signed url Classes
class SignedQuery(BaseModel):
    Expires: datetime.datetime
    KeyName: str
    UserId: UUID
    Signature: str

    @validator("Expires", pre=True)
    def expires_as_datetime(cls, v):
        return datetime.datetime.fromtimestamp(int(v))


class SignedUrl(BaseModel):
    path: str
    query: SignedQuery

    @validator("query", pre=True)
    def query_to_dict(cls, v):
        params = v.split("&")
        query_dict = {}
        for x in params:
            parameter = x.split("=")
            query_dict[parameter[0]] = parameter[1]
        return query_dict



class ExtendedAuthenticator(Authenticator):

    @classmethod
    async def signed_url_or_validate(cls, request: Request):
        if "Authorization" not in request.headers:
            # See if it's a signed url
            if cls.verify_signed_url(request.url.__str__()):
                user = await cls.validate_signed_url(request)
                return user
            else:
                raise HTTPException(
                    status_code=401,
                    detail="Can't authorize. Neither Authorization header, or signed url have been provided.",
                )
        else:
           user = super().validate(request.headers.get("Authorization"))
           return user
        
    
    @classmethod
    def sign_url(cls, url, key_name: str, user_id: str, expiration_time: int):
        """Gets the Signed URL string for the specified URL and configuration.

        Args:
            url: URL to sign as a string.
            key_name: name of the signing key as a string.
            base64_key: signing key as a base64 encoded string.
            expiration_time: expiration time as a UTC datetime object.

        Returns:
            Returns the Signed URL appended with the query parameters based on the
            specified configuration.
        """
        settings = ExtendedAppSettings()
        stripped_url = url.strip()
        parsed_url = parse.urlsplit(stripped_url)
        query_params = parse.parse_qs(parsed_url.query, keep_blank_values=True)
        epoch = datetime.datetime(1970, 1, 1, 0, 0)
        expiration_timestamp = int((expiration_time - epoch).total_seconds())
        
        base64_key = settings.__getattribute__(key_name).__str__()
        decoded_key = base64.urlsafe_b64decode(base64_key)

        url_pattern = (
            "{url}{separator}Expires={expires}&KeyName={key_name}&UserId={user_id}"
        )

        url_to_sign = url_pattern.format(
            url=stripped_url,
            separator="&" if query_params else "?",
            expires=expiration_timestamp,
            key_name=key_name,
            user_id=str(user_id),
        )

        digest = hmac.new(decoded_key, url_to_sign.encode("utf-8"), hashlib.sha1).digest()
        signature = base64.urlsafe_b64encode(digest).decode("utf-8")

        signed_url = "{url}&Signature={signature}".format(
            url=url_to_sign, signature=signature
        )

        return signed_url
    
    @classmethod
    def verify_signed_url(cls, url: str):
        """ """
        try:
            SignedUrl(path=url.split("?")[0], query=url.split("?")[1])
        except ValidationError:
            raise HTTPException(status_code=401, detail="Signed URL could not be parsed.")
        except IndexError:
            raise HTTPException(status_code=401, detail="Signed URL could not be parsed.")
        return True

    
    @classmethod
    async def validate_signed_url(cls, request: Request):
        """ """
        original_url = request.url.__str__()
        parsed = parse.urlparse(original_url)
        signed_url = SignedUrl(path=parsed.path, query=parsed.query)

        derived_url = cls.sign_url(
            signed_url.path,
            signed_url.query.KeyName,
            str(signed_url.query.UserId),
            signed_url.query.Expires,
        )
        logger.error(f"PARSED URL : {parsed.query}")
        parsed_derived = parse.urlparse(derived_url)
        logger.error(f"DERIVED URL : {parsed_derived.query}")

        if parsed_derived.query != parsed.query:
            raise HTTPException(status_code=401, detail="Signed URL not valid.")
        else:
            user = engine.get(
                get_model=User,
                primary_key=signed_url.query.UserId,
            )
            return user

# TODO Move upstream to openeo fastapi
from enum import Enum
from fastapi import Response
from pydantic import AnyUrl, BaseModel, Field
from typing import List, Optional
from typing_extensions import Annotated


from openeo_fastapi.api.types import Link


class GrantType(Enum):
    implicit = 'implicit'
    authorization_code_pkce = 'authorization_code+pkce'
    urn_ietf_params_oauth_grant_type_device_code_pkce = (
        'urn:ietf:params:oauth:grant-type:device_code+pkce'
    )
    refresh_token = 'refresh_token'


class DefaultClient(BaseModel):
    id: str = Field(
        ...,
        description='The OpenID Connect Client ID to be used in the authentication procedure.',
    )
    grant_types: List[GrantType] = Field(
        ...,
        description='List of authorization grant types (flows) supported by the OpenID Connect client.\nA grant type descriptor consist of a OAuth 2.0 grant type,\nwith an additional `+pkce` suffix when the grant type should be used with\nthe PKCE extension as defined in [RFC 7636](https://www.rfc-editor.org/rfc/rfc7636.html).\n\nAllowed values:\n- `implicit`: Implicit Grant as specified in [RFC 6749, sec. 1.3.2](https://www.rfc-editor.org/rfc/rfc6749.html#section-1.3.2)\n- `authorization_code+pkce`: Authorization Code Grant as specified in [RFC 6749, sec. 1.3.1](https://www.rfc-editor.org/rfc/rfc6749.html#section-1.3.1), with PKCE extension.\n- `urn:ietf:params:oauth:grant-type:device_code+pkce`: Device Authorization Grant (aka Device Code Flow) as specified in [RFC 8628](https://www.rfc-editor.org/rfc/rfc8628.html), with PKCE extension. Note that the combination of this grant with the PKCE extension is *not standardized* yet.\n- `refresh_token`: Refresh Token as specified in [RFC 6749, sec. 1.5](https://www.rfc-editor.org/rfc/rfc6749.html#section-1.5)',
    )
    redirect_urls: Optional[List[AnyUrl]] = Field(
        None,
        description='List of redirect URLs that are whitelisted by the OpenID Connect client.\nRedirect URLs MUST be provided when the OpenID Connect client supports\nthe `implicit` or `authorization_code+pkce` authorization flows.',
    )


class Provider(BaseModel):
    id: str = Field(
        ...,
        pattern=r'[\d\w]{1,20}',
        description='A **unique** identifier for the OpenID Connect Provider to be as prefix for the Bearer token.',
    )
    issuer: AnyUrl = Field(
        ...,
        description="The [issuer location](https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig) (also referred to as 'authority' in some client libraries) is the URL of the OpenID Connect provider, which conforms to a set of rules:\n1. After appending `/.well-known/openid-configuration` to the URL, a [HTTP/1.1 GET request](https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfigurationRequest) to the concatenated URL MUST return a [OpenID Connect Discovery Configuration Response](https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfigurationResponse). The response provides all information required to authenticate using OpenID Connect.\n2. The URL MUST NOT contain a terminating forward slash `/`.",
        examples=['https://accounts.google.com'],
    )
    scopes: Optional[List[str]] = Field(
        None,
        description='A list of OpenID Connect scopes that the client MUST include when requesting authorization. If scopes are specified, the list MUST at least contain the `openid` scope.',
    )
    title: str = Field(
        ...,
        description='The name that is publicly shown in clients for this OpenID Connect provider.',
    )
    description: Optional[str] = Field(
        None,
        description='A description that explains how the authentication procedure works.\n\nIt should make clear how to register and get credentials. This should\ninclude instruction on setting up `client_id`, `client_secret` and `redirect_uri`.\n\n[CommonMark 0.29](http://commonmark.org/) syntax MAY be used for rich\ntext representation.',
    )
    default_clients: Optional[List[DefaultClient]] = Field(
        None,
        description='List of default OpenID Connect clients that can be used by an openEO client\nfor OpenID Connect based authentication.\n\nA default OpenID Connect client is managed by the backend implementer.\nIt MUST be configured to be usable without a client secret,\nwhich limits its applicability to OpenID Connect grant types like\n"Authorization Code Grant with PKCE" and "Device Authorization Grant with PKCE"\n\nA default OpenID Connect client is provided without availability guarantees.\nThe backend implementer CAN revoke, reset or update it any time.\nAs such, openEO clients SHOULD NOT store or cache default OpenID Connect client information\nfor long term usage.\nA default OpenID Connect client is intended to simplify authentication for novice users.\nFor production use cases, it is RECOMMENDED to set up a dedicated OpenID Connect client.',
        title='Default OpenID Connect Clients',
    )
    links: Optional[List[Link]] = Field(
        None,
        description='Links related to this provider, for example a\nhelp page or a page to register a new user account.\n\nFor relation types see the lists of\n[common relation types in openEO](#section/API-Principles/Web-Linking).',
    )


class CredentialsOidcGetResponse(BaseModel):
    providers: List[Provider] = Field(
        ...,
        description="The first provider in this list is the default provider for authentication. Clients can either pre-select or directly use the default provider for authentication if the user doesn't specify a specific value.",
    )



def get_credentials_oidc() -> Response:
    """
        OpenID Connect authentication.
    """

    settings = ExtendedAppSettings()

    return CredentialsOidcGetResponse(
        providers=[
            Provider(
                id=settings.OIDC_ORGANISATION,
                title="EGI Check-in",
                issuer=settings.OIDC_URL,
                scopes=[
                    "openid",
                    "email",
                    "eduperson_entitlement",
                    "eduperson_scoped_affiliation",
                ],
                default_clients=[
                    DefaultClient(
                        id="openeo-platform-default-client",
                        redirect_urls=[
                            "https://editor.openeo.cloud",
                            "https://editor.openeo.org",
                            "http://localhost:1410/",
                        ],
                        grant_types=[
                            GrantType.authorization_code_pkce,
                            GrantType.urn_ietf_params_oauth_grant_type_device_code_pkce,
                            GrantType.refresh_token,
                        ],
                    )
                ],
            )
        ]
    )
