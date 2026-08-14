from google.api_core import retry_async
from aiohttp import ClientSession
import logging
from src.cache import (
    CacheBackend,
    SingleFlight,
    cache as app_cache,
    entities_query_key,
    cache_list,
    relation_key,
    singleflight as app_singleflight,
)
from src.core import settings
from src.exception import BadRequestError, InternalServerError, NotFoundError
from src.models import AttributeFilterRecords, Entity, Relation
from src.utils import http_client

logger = logging.getLogger(__name__)


def custom_retry_predicate(exception: Exception) -> bool:
    """
    Determine if the request should be retried based on the exception type.
    Returns False for BadRequestError to skip retries.
    """
    if isinstance(exception, (BadRequestError, NotFoundError)):
        return False

    if isinstance(exception, (InternalServerError)):
        return True


api_retry_decorator = retry_async.AsyncRetry(
    predicate=custom_retry_predicate,
    initial=1.0,
    maximum=6.0,
    multiplier=2.0,
    timeout=10.0,  # retry for 10 seconds
)


def _entities_cache_key(_self, entity: Entity) -> str:
    if not entity:
        raise BadRequestError("Entity is required")
    return entities_query_key(
        entity.model_dump(mode="json"), prefix=settings.CACHE_KEY_PREFIX
    )


def _relation_cache_key(_self, entityId: str, relation: Relation) -> str:
    if not entityId or not relation:
        raise BadRequestError("Entity ID and relation is required")
    stripped_entity_id = str(entityId).strip()
    if not stripped_entity_id:
        raise BadRequestError("Entity ID can not be empty")
    return relation_key(
        stripped_entity_id,
        relation.model_dump(mode="json"),
        prefix=settings.CACHE_KEY_PREFIX,
    )


class OpenGINService:
    """
    The OpenGINService directly interfaces with the OpenGIN APIs to retrieve data.

    get_entities / fetch_relation are read-through cached (NullCache when disabled).
    """

    def __init__(
        self,
        cache: CacheBackend | None = None,
        singleflight: SingleFlight | None = None,
    ):
        # Default to lifespan singletons so routers can keep doing OpenGINService()
        self._cache = cache if cache is not None else app_cache
        self._sf = singleflight if singleflight is not None else app_singleflight

    @property
    def session(self) -> ClientSession:
        return http_client.session

    @cache_list(key_builder=_entities_cache_key, model=Entity)
    @api_retry_decorator
    async def get_entities(self, entity: Entity):
        """Search OpenGIN entities. Cached; retries apply on cache miss only."""
        if not entity:
            raise BadRequestError("Entity is required")

        url = f"{settings.BASE_URL_QUERY}/v1/entities/search"
        headers = {"Content-Type": "application/json"}
        payload = entity.model_dump(mode="json")

        try:
            async with self.session.post(
                url, json=payload, headers=headers
            ) as response:
                if response.status == 404:
                    raise NotFoundError(
                        f"Read API Error: Entity not found for id {entity.id}"
                    )
                if response.status == 400:
                    raise BadRequestError(
                        f"Read API Error: Bad request for id {entity.id}"
                    )

                response.raise_for_status()
                res_json = await response.json()
                response_list = res_json.get("body", [])

                if not response_list:
                    raise NotFoundError(
                        f"Read API Error: Entity not found for id {entity.id}"
                    )

                result = [Entity.model_validate(response) for response in response_list]
                return result

        except NotFoundError:
            raise
        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Read API Error: {str(e)}")
            raise InternalServerError("An unexpected error occurred") from e

    @cache_list(key_builder=_relation_cache_key, model=Relation)
    @api_retry_decorator
    async def fetch_relation(self, entityId: str, relation: Relation):
        """Fetch OpenGIN relations. Cached; retries apply on cache miss only."""
        if not entityId or not relation:
            raise BadRequestError("Entity ID and relation is required")

        stripped_entity_id = str(entityId).strip()
        if not stripped_entity_id:
            raise BadRequestError("Entity ID can not be empty")

        url = f"{settings.BASE_URL_QUERY}/v1/entities/{stripped_entity_id}/relations"
        headers = {"Content-Type": "application/json"}
        payload = relation.model_dump(mode="json")

        try:
            async with self.session.post(
                url, json=payload, headers=headers
            ) as response:
                if response.status == 404:
                    raise NotFoundError(
                        f"Read API Error: Relation not found for id {stripped_entity_id}"
                    )
                if response.status == 400:
                    raise BadRequestError(
                        f"Read API Error: Bad request for id {stripped_entity_id}"
                    )
                response.raise_for_status()
                data = await response.json()
                if data is None:
                    return []
                result = [Relation.model_validate(item) for item in data]
                return result

        except NotFoundError:
            raise
        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Read API Error: {str(e)}")
            raise InternalServerError("An unexpected error occurred") from e

    @api_retry_decorator
    async def get_metadata(self, entityId: str):

        if not entityId:
            raise BadRequestError("Entity ID is required")

        stripped_entity_id = str(entityId).strip()
        if not stripped_entity_id:
            raise BadRequestError("Entity ID can not be empty")

        url = f"{settings.BASE_URL_QUERY}/v1/entities/{entityId}/metadata"
        headers = {"Content-Type": "application/json"}

        try:
            async with self.session.get(url, headers=headers) as response:
                if response.status == 404:
                    raise NotFoundError(
                        f"Read API Error: Metadata not found for id {entityId}"
                    )
                if response.status == 400:
                    raise BadRequestError(
                        f"Read API Error: Bad request for id {entityId}"
                    )
                response.raise_for_status()
                return await response.json()
        except NotFoundError:
            raise
        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Read API Error: {str(e)}")
            raise InternalServerError("An unexpected error occurred") from e

    @api_retry_decorator
    async def get_attributes(
        self,
        category_id: str,
        dataset_name: str,
        startTime: str | None = None,
        endTime: str | None = None,
        fields: list[str] | None = None,
        filters: AttributeFilterRecords | None = None,
    ):
        if not category_id:
            raise BadRequestError("Category ID is required")

        if not dataset_name:
            raise BadRequestError("Dataset name is required")

        stripped_category_id = str(category_id).strip()
        if not stripped_category_id:
            raise BadRequestError("Category ID can not be empty")

        stripped_dataset_name = str(dataset_name).strip()
        if not stripped_dataset_name:
            raise BadRequestError("Dataset name can not be empty")

        url = f"{settings.BASE_URL_QUERY}/v1/entities/{category_id}/attributes/{dataset_name}"
        headers = {"Content-Type": "application/json"}
        payload = filters.model_dump(mode="json") if filters else {}
        params: dict[str, str | list[str]] = {}
        if startTime is not None:
            params["startTime"] = startTime
        if endTime is not None:
            params["endTime"] = endTime
        if fields is not None:
            params["fields"] = fields

        try:
            async with self.session.post(
                url, json=payload, headers=headers, params=params or None
            ) as response:
                if response.status == 404:
                    raise NotFoundError(
                        f"Read API Error: Attributes not found for category id {category_id} and dataset name {dataset_name}"
                    )
                if response.status == 400:
                    raise BadRequestError(
                        f"Read API Error: Bad request for category id {category_id} and dataset name {dataset_name}"
                    )
                response.raise_for_status()
                return await response.json()
        except NotFoundError:
            raise
        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Read API Error: {str(e)}")
            raise InternalServerError("An unexpected error occurred") from e
