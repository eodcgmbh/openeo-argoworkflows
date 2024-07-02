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

