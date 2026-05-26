import logging

from fastapi import HTTPException
from pydantic import ValidationError

from openeo_fastapi.api.models import Collection, Collections
from openeo_fastapi.api.types import Error
from openeo_fastapi.client.collections import CollectionRegister

logger = logging.getLogger(__name__)


class ArgoCollectionRegister(CollectionRegister):

    async def get_collections(self):
        path = "collections"
        resp = await self._proxy_request(path)

        if not resp:
            raise HTTPException(
                status_code=404,
                detail=Error(code="NotFound", message="No Collections found."),
            )

        valid_collections = []
        for collection in resp["collections"]:
            if (
                self.settings.STAC_COLLECTIONS_WHITELIST
                and collection.get("id") not in self.settings.STAC_COLLECTIONS_WHITELIST
            ):
                continue
            try:
                valid_collections.append(Collection(**collection))
            except (ValidationError, Exception) as e:
                logger.warning(
                    "Dropping collection %r from response due to validation error: %s",
                    collection.get("id"),
                    e,
                )

        return Collections(collections=valid_collections, links=resp["links"])
