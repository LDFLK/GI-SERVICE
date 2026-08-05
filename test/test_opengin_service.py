import pytest
from src.cache import InMemoryCache, NullCache, SingleFlight
from src.enums import RelationDirectionEnum, RelationNameEnum
from src.exception import BadRequestError, NotFoundError
from src.models import (
    AttributeFilterRecord,
    AttributeFilterRecords,
    Entity,
    Relation,
    Kind,
)
from src.services import OpenGINService
from test.conftest import MockResponse


# Test get entity
@pytest.mark.asyncio
async def test_get_entity_success(mock_service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse(
        {"body": [Entity(id="entity_123", name="Test Entity")]}
    )

    mock_session.post.return_value = mock_response

    result = await mock_service.get_entities(entity)

    assert result == [Entity(id="entity_123", name="Test Entity")]
    mock_session.post.assert_called_once()


@pytest.mark.asyncio
async def test_get_entity_empty_entity_id(mock_service, mock_session):
    entity = Entity(id="")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entities(entity)


@pytest.mark.asyncio
async def test_get_entity_none_empty_id(mock_service, mock_session):
    entity = None
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await mock_service.get_entities(entity)


@pytest.mark.asyncio
async def test_get_entity_by_none_response(mock_service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse(
        {"wrong_body": [Entity(id="entity_123", name="Test Entity")]}
    )

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entities(entity)


@pytest.mark.asyncio
async def test_get_entity_by_id_empty_response(mock_service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entities(entity)


@pytest.mark.asyncio
async def test_get_entity_by_name_empty_response(mock_service, mock_session):
    entity = Entity(name="minister of X")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entities(entity)


@pytest.mark.asyncio
async def test_get_entity_by_kind_empty_response(mock_service, mock_session):
    kind = Kind(major="Org", minor="min")
    entity = Entity(kind=kind)
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entities(entity)


@pytest.mark.asyncio
async def test_get_entity_by_created_empty_response(mock_service, mock_session):
    entity = Entity(created="2022-12-01")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entities(entity)


@pytest.mark.asyncio
async def test_get_entity_by_terminated_empty_response(mock_service, mock_session):
    entity = Entity(terminated="2022-12-01")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entities(entity)


# Test fetch relation
@pytest.mark.asyncio
async def test_fetch_relation_success(mock_service, mock_session):
    entity_id = "entity_123"

    mock_response = MockResponse(
        [
            Relation(
                id="relation_123",
                relationName=RelationNameEnum.AS_MINISTER.value,
                direction=RelationDirectionEnum.OUTGOING.value,
            )
        ]
    )

    mock_session.post.return_value = mock_response

    result = await mock_service.fetch_relation(
        entity_id,
        relation=Relation(
            id="relation_123", direction=RelationDirectionEnum.OUTGOING.value
        ),
    )

    assert result == [
        Relation(
            id="relation_123",
            relationName=RelationNameEnum.AS_MINISTER.value,
            direction=RelationDirectionEnum.OUTGOING.value,
        )
    ]
    mock_session.post.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_relation_none_response_returns_empty_list(
    mock_service, mock_session
):
    entity_id = "entity_123"
    mock_session.post.return_value = MockResponse(None)

    result = await mock_service.fetch_relation(
        entity_id,
        relation=Relation(
            name=RelationNameEnum.AS_MINISTER.value,
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    )

    assert result == []
    mock_session.post.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_relation_empty_entity_id(mock_service, mock_session):
    entity_id = ""
    mock_response = MockResponse(
        [
            Relation(
                id="relation_123",
                relationName=RelationNameEnum.AS_MINISTER.value,
                direction=RelationDirectionEnum.OUTGOING.value,
            )
        ]
    )

    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await mock_service.fetch_relation(
            entity_id, relation=Relation(id="relation_123")
        )


@pytest.mark.asyncio
async def test_fetch_relation_none_entity_id(mock_service, mock_session):
    entity_id = None
    mock_response = MockResponse(
        [
            Relation(
                id="relation_123",
                relationName=RelationNameEnum.AS_MINISTER.value,
                direction=RelationDirectionEnum.OUTGOING.value,
            )
        ]
    )

    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await mock_service.fetch_relation(
            entity_id, relation=Relation(id="relation_123")
        )


# Tests for get_metadata
@pytest.mark.asyncio
async def test_get_metadata_success(mock_service, mock_session):
    """Test get_metadata with successful response"""
    category_id = "category_123"
    metadata_response = {
        "attr1": "value1",
        "attr2": "value2",
        "description": "Test metadata",
    }

    mock_response = MockResponse(metadata_response)
    mock_session.get.return_value = mock_response

    result = await mock_service.get_metadata(category_id)

    assert result == metadata_response
    assert result["attr1"] == "value1"
    assert result["attr2"] == "value2"
    mock_session.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_metadata_empty_response(mock_service, mock_session):
    """Test get_metadata with empty metadata response"""
    category_id = "category_456"
    empty_metadata = {}

    mock_response = MockResponse(empty_metadata)
    mock_session.get.return_value = mock_response

    result = await mock_service.get_metadata(category_id)

    assert result == {}
    mock_session.get.assert_called_once()


# Tests for get_attributes
@pytest.mark.asyncio
async def test_get_attributes_success(mock_service, mock_session):
    """Test get_attributes with successful response and no optional params"""
    category_id = "category_123"
    dataset_name = "dataset_abc"
    attributes_response = [
        {"field1": "value1", "field2": "value2"},
        {"field1": "value3", "field2": "value4"},
    ]

    mock_session.post.return_value = MockResponse(attributes_response)

    result = await mock_service.get_attributes(category_id, dataset_name)

    assert result == attributes_response
    mock_session.post.assert_called_once()
    call_kwargs = mock_session.post.call_args.kwargs
    assert call_kwargs["json"] == {}
    assert call_kwargs["params"] is None
    assert call_kwargs["headers"] == {"Content-Type": "application/json"}
    assert mock_session.post.call_args.args[0].endswith(
        f"/v1/entities/{category_id}/attributes/{dataset_name}"
    )


@pytest.mark.asyncio
async def test_get_attributes_with_query_params(mock_service, mock_session):
    """Test get_attributes passes startTime, endTime, and fields query params"""
    category_id = "category_123"
    dataset_name = "dataset_abc"
    start_time = "2024-01-01T00:00:00Z"
    end_time = "2024-12-31T23:59:59Z"
    fields = ["field1", "field2"]
    attributes_response = [{"field1": "value1", "field2": "value2"}]

    mock_session.post.return_value = MockResponse(attributes_response)

    result = await mock_service.get_attributes(
        category_id,
        dataset_name,
        startTime=start_time,
        endTime=end_time,
        fields=fields,
    )

    assert result == attributes_response
    call_kwargs = mock_session.post.call_args.kwargs
    assert call_kwargs["params"] == {
        "startTime": start_time,
        "endTime": end_time,
        "fields": fields,
    }
    assert call_kwargs["json"] == {}


@pytest.mark.asyncio
async def test_get_attributes_omits_fields_query_param_when_not_provided(
    mock_service, mock_session
):
    """Test get_attributes does not send fields param so the API defaults to ['*']"""
    mock_session.post.return_value = MockResponse([])

    await mock_service.get_attributes("category_123", "dataset_abc")

    call_kwargs = mock_session.post.call_args.kwargs
    assert "fields" not in (call_kwargs["params"] or {})


@pytest.mark.asyncio
async def test_get_attributes_with_filter_payload(mock_service, mock_session):
    """Test get_attributes sends row-level filter payload in request body"""
    category_id = "category_123"
    dataset_name = "dataset_abc"
    filters = AttributeFilterRecords(
        records=[
            AttributeFilterRecord(field_name="status", operator="eq", value="active")
        ]
    )
    attributes_response = [{"status": "active"}]

    mock_session.post.return_value = MockResponse(attributes_response)

    result = await mock_service.get_attributes(
        category_id,
        dataset_name,
        filters=filters,
    )

    assert result == attributes_response
    call_kwargs = mock_session.post.call_args.kwargs
    assert call_kwargs["json"] == {
        "records": [{"field_name": "status", "operator": "eq", "value": "active"}]
    }
    assert call_kwargs["params"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operator",
    ["eq", "neq", "gt", "lt", "gte", "lte", "contains", "notcontains"],
)
async def test_get_attributes_with_filter_operators(
    mock_service, mock_session, operator
):
    """Test get_attributes serializes all accepted filter operators in the payload"""
    filters = AttributeFilterRecords(
        records=[
            AttributeFilterRecord(field_name="amount", operator=operator, value="100")
        ]
    )
    mock_session.post.return_value = MockResponse([])

    await mock_service.get_attributes("category_123", "dataset_abc", filters=filters)

    call_kwargs = mock_session.post.call_args.kwargs
    assert call_kwargs["json"]["records"][0]["operator"] == operator


@pytest.mark.asyncio
async def test_get_attributes_with_query_params_and_filters(mock_service, mock_session):
    """Test get_attributes sends both query params and filter payload together"""
    filters = AttributeFilterRecords(
        records=[
            AttributeFilterRecord(field_name="region", value="western"),
            AttributeFilterRecord(field_name="year", operator="gte", value="2020"),
        ]
    )
    mock_session.post.return_value = MockResponse([])

    await mock_service.get_attributes(
        "category_123",
        "dataset_abc",
        startTime="2024-01-01T00:00:00Z",
        endTime="2024-12-31T23:59:59Z",
        fields=["region", "year"],
        filters=filters,
    )

    call_kwargs = mock_session.post.call_args.kwargs
    assert call_kwargs["params"] == {
        "startTime": "2024-01-01T00:00:00Z",
        "endTime": "2024-12-31T23:59:59Z",
        "fields": ["region", "year"],
    }
    assert call_kwargs["json"] == {
        "records": [
            {"field_name": "region", "operator": "eq", "value": "western"},
            {"field_name": "year", "operator": "gte", "value": "2020"},
        ]
    }


@pytest.mark.asyncio
async def test_get_attributes_missing_category_id(mock_service, mock_session):
    with pytest.raises(BadRequestError, match="Category ID is required"):
        await mock_service.get_attributes(None, "dataset_abc")

    mock_session.post.assert_not_called()


@pytest.mark.asyncio
async def test_get_attributes_missing_dataset_name(mock_service, mock_session):
    with pytest.raises(BadRequestError, match="Dataset name is required"):
        await mock_service.get_attributes("category_123", None)

    mock_session.post.assert_not_called()


@pytest.mark.asyncio
async def test_get_attributes_empty_category_id(mock_service, mock_session):
    with pytest.raises(BadRequestError, match="Category ID can not be empty"):
        await mock_service.get_attributes("   ", "dataset_abc")

    mock_session.post.assert_not_called()


@pytest.mark.asyncio
async def test_get_attributes_empty_dataset_name(mock_service, mock_session):
    with pytest.raises(BadRequestError, match="Dataset name can not be empty"):
        await mock_service.get_attributes("category_123", "   ")

    mock_session.post.assert_not_called()


@pytest.mark.asyncio
async def test_get_attributes_not_found(mock_service, mock_session):
    mock_session.post.return_value = MockResponse({}, status=404)

    with pytest.raises(NotFoundError, match="Attributes not found"):
        await mock_service.get_attributes("category_123", "dataset_abc")


@pytest.mark.asyncio
async def test_get_attributes_bad_request(mock_service, mock_session):
    mock_session.post.return_value = MockResponse({}, status=400)

    with pytest.raises(BadRequestError, match="Bad request"):
        await mock_service.get_attributes("category_123", "dataset_abc")


# --- Read-through cache (InMemoryCache; no Redis required) ---


@pytest.mark.asyncio
async def test_get_entities_second_call_is_cache_hit(mock_session):
    """Miss then hit: upstream POST only once."""
    cache = InMemoryCache()
    service = OpenGINService(cache=cache, singleflight=SingleFlight())
    entity = Entity(id="entity_123")
    mock_session.post.return_value = MockResponse(
        {"body": [{"id": "entity_123", "name": "Test Entity"}]}
    )

    first = await service.get_entities(entity)
    second = await service.get_entities(entity)

    assert first == second == [Entity(id="entity_123", name="Test Entity")]
    assert mock_session.post.call_count == 1


@pytest.mark.asyncio
async def test_get_entities_null_cache_always_misses(mock_session):
    """NullCache never stores — proves wiring does not falsely cache."""
    service = OpenGINService(cache=NullCache(), singleflight=SingleFlight())
    entity = Entity(id="entity_123")
    mock_session.post.return_value = MockResponse(
        {"body": [{"id": "entity_123", "name": "Test Entity"}]}
    )

    await service.get_entities(entity)
    await service.get_entities(entity)

    assert mock_session.post.call_count == 2


@pytest.mark.asyncio
async def test_get_entities_not_found_is_not_cached(mock_session):
    """404 must not poison the cache; a later success can still be fetched."""
    cache = InMemoryCache()
    service = OpenGINService(cache=cache, singleflight=SingleFlight())
    entity = Entity(id="entity_123")

    mock_session.post.return_value = MockResponse({"body": []}, status=200)
    with pytest.raises(NotFoundError):
        await service.get_entities(entity)

    mock_session.post.return_value = MockResponse(
        {"body": [{"id": "entity_123", "name": "Recovered"}]}
    )
    result = await service.get_entities(entity)

    assert result == [Entity(id="entity_123", name="Recovered")]
    assert mock_session.post.call_count == 2


@pytest.mark.asyncio
async def test_fetch_relation_second_call_is_cache_hit(mock_session):
    cache = InMemoryCache()
    service = OpenGINService(cache=cache, singleflight=SingleFlight())
    entity_id = "entity_123"
    relation = Relation(
        name=RelationNameEnum.AS_MINISTER.value,
        direction=RelationDirectionEnum.OUTGOING.value,
    )
    mock_session.post.return_value = MockResponse(
        [
            {
                "id": "relation_123",
                "name": RelationNameEnum.AS_MINISTER.value,
                "direction": RelationDirectionEnum.OUTGOING.value,
            }
        ]
    )

    first = await service.fetch_relation(entity_id, relation=relation)
    second = await service.fetch_relation(entity_id, relation=relation)

    assert first == second
    assert len(first) == 1
    assert first[0].id == "relation_123"
    assert mock_session.post.call_count == 1
