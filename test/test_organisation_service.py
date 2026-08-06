import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.enums import EntityIdEnum, RelationDirectionEnum, RelationNameEnum
from src.exception import BadRequestError, InternalServerError, NotFoundError
from src.models import Entity, Relation
from src.utils import Util


@pytest.mark.asyncio
async def test_enrich_person_data_as_president(
    organisation_service, mock_opengin_service
):
    selected_date = "2023-10-27"
    president_id = "pres_123"
    is_president = True

    mock_opengin_service.get_entities.return_value = [
        Entity(id=president_id, name="mocked_protobuf_name")
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="John Doe",
    ):
        result = await organisation_service.enrich_person_data(
            selected_date=selected_date,
            president_id=president_id,
            is_president=is_president,
        )

    assert result == {
        "id": president_id,
        "name": "John Doe",
        "isNew": False,
        "isPresident": True,
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=president_id)
    )


@pytest.mark.asyncio
async def test_enrich_person_data_as_not_president(
    organisation_service, mock_opengin_service
):
    selected_date = "2023-10-27"
    president_id = "pres_123"
    person_relation = Relation(
        relatedEntityId="person_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id="person_123", name="mocked_protobuf_name")
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="John Doe",
    ):
        result = await organisation_service.enrich_person_data(
            selected_date=selected_date,
            president_id=president_id,
            person_relation=person_relation,
        )

    assert result == {
        "id": "person_123",
        "name": "John Doe",
        "isNew": True,
        "isPresident": False,
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=person_relation.relatedEntityId)
    )


@pytest.mark.asyncio
async def test_enrich_department_item(organisation_service, mock_opengin_service):
    department_relation = Relation(
        relatedEntityId="department_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )
    selected_date = "2023-10-27"

    mock_opengin_service.get_entities.return_value = [
        Entity(id="department_123", name="mocked_protobuf_name")
    ]

    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            id="",
            relatedEntityId="department_123",
            name=RelationNameEnum.AS_CATEGORY.value,
            startTime="2020-08-09T00:00:00Z",
            endTime="2022-03-08T00:00:00Z",
            direction=RelationDirectionEnum.OUTGOING.value,
        )
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="Department_of_security",
    ):
        result = await organisation_service.enrich_department_item(
            selected_date=selected_date, department_relation=department_relation
        )

    assert result == {
        "id": "department_123",
        "name": "Department_of_security",
        "isNew": True,
        "hasData": True,
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=department_relation.relatedEntityId)
    )


@pytest.mark.asyncio
async def test_enrich_department_item_with_no_data(
    organisation_service, mock_opengin_service
):
    department_relation = Relation(
        relatedEntityId="department_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )
    selected_date = "2023-10-27"

    mock_opengin_service.get_entities.return_value = [
        Entity(id="department_123", name="mocked_protobuf_name")
    ]

    mock_opengin_service.fetch_relation.return_value = []

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="Department_of_security",
    ):
        result = await organisation_service.enrich_department_item(
            selected_date=selected_date, department_relation=department_relation
        )

    assert result == {
        "id": "department_123",
        "name": "Department_of_security",
        "isNew": True,
        "hasData": False,
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=department_relation.relatedEntityId)
    )


@pytest.mark.asyncio
async def test_enrich_department_item_not_new(
    organisation_service, mock_opengin_service
):
    department_relation = Relation(
        relatedEntityId="department_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )
    selected_date = "2024-10-27"

    mock_opengin_service.get_entities.return_value = [
        Entity(id="department_123", name="mocked_protobuf_name")
    ]

    mock_opengin_service.fetch_relation.return_value = []

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="Department_of_security",
    ):
        result = await organisation_service.enrich_department_item(
            selected_date=selected_date, department_relation=department_relation
        )

    assert result == {
        "id": "department_123",
        "name": "Department_of_security",
        "isNew": False,
        "hasData": False,
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=department_relation.relatedEntityId)
    )


@pytest.mark.asyncio
async def test_active_portfolio_list_invalid_president_id(
    organisation_service, mock_opengin_service
):
    president_id = "invalid_president_123"
    selected_date = "2021-10-27"

    mock_opengin_service.get_entities.side_effect = NotFoundError("President not found")

    with pytest.raises(NotFoundError):
        await organisation_service.active_portfolio_list(
            president_id=president_id, selected_date=selected_date
        )

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=president_id)
    )


@pytest.mark.asyncio
async def test_active_portfolio_list_valid_president_id(
    organisation_service, mock_opengin_service
):
    president_id = "president_123"
    selected_date = "2021-10-27"

    mock_opengin_service.get_entities.return_value = [
        Entity(id=president_id, name="mocked_protobuf_name")
    ]
    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            id="portfolio_relation_123",
            relatedEntityId="portfolio_123",
            name=RelationNameEnum.AS_MINISTER.value,
            startTime="2020-08-09T00:00:00Z",
            endTime="2022-03-08T00:00:00Z",
            direction=RelationDirectionEnum.OUTGOING.value,
        )
    ]

    with patch(
        "src.services.organisation_service.OrganisationService.process_portfolio_item",
        new_callable=AsyncMock,
    ) as mock_process_portfolio_item:
        mock_process_portfolio_item.return_value = {
            "id": "portfolio_123",
            "name": "Portfolio X",
            "type": "cabinetMinister",
            "isNew": False,
            "ministers": [],
        }

        result = await organisation_service.active_portfolio_list(
            president_id=president_id, selected_date=selected_date
        )

    assert result == {
        "NoOfCabinetMinistries": 1,
        "NoOfStateMinistries": 0,
        "newMinistries": 0,
        "newMinisters": 0,
        "ministriesUnderPresident": 0,
        "portfolioList": [
            {
                "id": "portfolio_123",
                "name": "Portfolio X",
                "type": "cabinetMinister",
                "isNew": False,
                "ministers": [],
            }
        ],
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=president_id)
    )
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId=president_id,
        relation=Relation(
            name=RelationNameEnum.AS_MINISTER.value,
            activeAt=f"{selected_date}T00:00:00Z",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    )
    mock_process_portfolio_item.assert_called_once_with(
        mock_opengin_service.fetch_relation.return_value[0],
        president_id,
        selected_date,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("relations", [None, []])
async def test_active_portfolio_list_valid_president_without_active_relations(
    organisation_service, mock_opengin_service, relations
):
    president_id = "president_123"
    selected_date = "2021-10-27"

    mock_opengin_service.get_entities.return_value = [
        Entity(id=president_id, name="mocked_protobuf_name")
    ]
    mock_opengin_service.fetch_relation.return_value = relations

    with patch(
        "src.services.organisation_service.OrganisationService.process_portfolio_item",
        new_callable=AsyncMock,
    ) as mock_process_portfolio_item:
        result = await organisation_service.active_portfolio_list(
            president_id=president_id, selected_date=selected_date
        )

    assert result == {
        "NoOfCabinetMinistries": 0,
        "NoOfStateMinistries": 0,
        "newMinistries": 0,
        "newMinisters": 0,
        "ministriesUnderPresident": 0,
        "portfolioList": [],
    }
    mock_process_portfolio_item.assert_not_awaited()


@pytest.mark.asyncio
async def test_departments_by_portfolio_id_success(
    organisation_service, mock_opengin_service
):
    portfolio_id = "portfolio_123"
    selected_date = "2021-10-27"

    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            id="",
            relatedEntityId="portfolio_123",
            name=RelationNameEnum.AS_DEPARTMENT.value,
            startTime="2020-08-09T00:00:00Z",
            endTime="2022-03-08T00:00:00Z",
            direction=RelationDirectionEnum.OUTGOING.value,
        )
    ]

    # Patch enrich_department_item with AsyncMock returning the department dict
    with patch(
        "src.services.organisation_service.OrganisationService.enrich_department_item",
        new_callable=AsyncMock,
    ) as mock_enrich_department:
        mock_enrich_department.return_value = {
            "id": "department_123",
            "name": "Department_of_security",
            "isNew": False,
            "hasData": False,
        }

        result = await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    assert result == {
        "totalDepartments": 1,
        "newDepartments": 0,
        "departmentList": [
            {
                "id": "department_123",
                "name": "Department_of_security",
                "isNew": False,
                "hasData": False,
            }
        ],
    }

    # Check fetch_relation was called correctly
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId=portfolio_id,
        relation=Relation(
            name=RelationNameEnum.AS_DEPARTMENT.value,
            activeAt=f"{selected_date}T00:00:00Z",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    )

    # Ensure enrich_department_item was called once with the correct args
    mock_enrich_department.assert_called_once_with(
        department_relation=mock_opengin_service.fetch_relation.return_value[0],
        selected_date=selected_date,
    )


@pytest.mark.asyncio
async def test_departments_by_portfolio_id_empty_portfolio_id(organisation_service):
    portfolio_id = ""
    selected_date = "2021-10-27"

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )


@pytest.mark.asyncio
async def test_departments_by_portfolio_id_none_portfolio_id(organisation_service):
    portfolio_id = None
    selected_date = "2021-10-27"

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )


@pytest.mark.asyncio
async def test_departments_by_portfolio_id_empty_selected_date(organisation_service):
    portfolio_id = "portfolio_123"
    selected_date = ""

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )


@pytest.mark.asyncio
async def test_departments_by_portfolio_id_none_selected_date(organisation_service):
    portfolio_id = "portfolio_123"
    selected_date = None

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )


@pytest.mark.asyncio
async def test_prime_minister_success(organisation_service, mock_opengin_service):
    selected_date = "2021-10-27"

    mock_response = Relation(
        name=RelationNameEnum.AS_PRIME_MINISTER.value,
        activeAt="",
        relatedEntityId="cit_3",
        startTime="2022-07-26T00:00:00Z",
        endTime="2024-09-23T00:00:00Z",
        id="person_123",
        direction=RelationDirectionEnum.OUTGOING.value,
    )
    mock_opengin_service.fetch_relation.return_value = [mock_response]

    # Patch enrich_department_item with AsyncMock returning the department dict
    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {
            "id": "person_123",
            "name": "Person X",
            "isNew": False,
            "isPresident": False,
        }

        result = await organisation_service.fetch_prime_minister(
            selected_date=selected_date
        )

    assert result == {
        "body": {
            "id": "person_123",
            "name": "Person X",
            "isNew": False,
            "term": "2022 Jul - 2024 Sep",
        }
    }

    # Check fetch_relation was called correctly
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId=EntityIdEnum.GOVERNMENT.value,
        relation=Relation(
            name=RelationNameEnum.AS_PRIME_MINISTER.value,
            activeAt=Util.normalize_timestamp(selected_date),
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    )


@pytest.mark.asyncio
async def test_prime_minister_without_person_data(
    organisation_service, mock_opengin_service
):
    selected_date = "2021-10-27"

    mock_response = Relation(
        name=RelationNameEnum.AS_PRIME_MINISTER.value,
        activeAt="",
        relatedEntityId="cit_3",
        startTime="2022-07-26T00:00:00Z",
        endTime="2024-09-23T00:00:00Z",
        id="person_123",
        direction=RelationDirectionEnum.OUTGOING.value,
    )
    mock_opengin_service.fetch_relation.return_value = [mock_response]

    # Patch enrich_department_item with AsyncMock returning the department dict
    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {}

        result = await organisation_service.fetch_prime_minister(
            selected_date=selected_date
        )

    assert result == {"body": {}}

    # Check fetch_relation was called correctly
    mock_opengin_service.fetch_relation.assert_called_with(
        entityId=EntityIdEnum.GOVERNMENT.value,
        relation=Relation(
            name=RelationNameEnum.AS_PRIME_MINISTER.value,
            activeAt=Util.normalize_timestamp(selected_date),
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    )


@pytest.mark.asyncio
async def test_prime_minister_without_selected_date(organisation_service):
    selected_date = None

    with pytest.raises(BadRequestError):
        await organisation_service.fetch_prime_minister(selected_date=selected_date)


@pytest.mark.asyncio
async def test_prime_minister_with_empty_selected_date(organisation_service):
    selected_date = ""

    with pytest.raises(BadRequestError):
        await organisation_service.fetch_prime_minister(selected_date=selected_date)


@pytest.mark.asyncio
async def test_prime_minister_with_no_relation(
    organisation_service, mock_opengin_service
):
    selected_date = "2021-10-27"

    mock_opengin_service.fetch_relation.return_value = []

    result = await organisation_service.fetch_prime_minister(
        selected_date=selected_date
    )
    assert result == {"body": {}}


@pytest.mark.asyncio
async def test_prime_minister_with_internal_server_error(
    organisation_service, mock_opengin_service
):
    selected_date = "2021-10-27"
    original_error_message = "OpenGIN service error"

    mock_opengin_service.fetch_relation.side_effect = Exception(original_error_message)

    with pytest.raises(InternalServerError) as exc_info:
        await organisation_service.fetch_prime_minister(selected_date=selected_date)

    root_cause = exc_info.value.__cause__
    assert isinstance(root_cause, Exception)
    assert str(root_cause) == original_error_message


@pytest.mark.asyncio
async def test_department_history_timeline_success(
    organisation_service, mock_opengin_service
):
    # Setup IDs
    # Lineage: dep_01 -> dep_02 (via RENAMED_TO)
    department_id = "dep_01"

    # Mock _get_renamed_lineage
    # 1. dep_01 -> fetch_relation(RENAMED_TO) -> [dep_02]
    # 2. dep_02 -> fetch_relation(RENAMED_TO) -> []

    # Mock _fetch_and_map_relations (AS_DEPARTMENT)
    # 1. dep_01 -> [min_01 (2020-01 to 2021-01)]
    # 2. dep_02 -> [min_02 (2021-01 to 2022-01)]

    # Mock _fetch_and_map_entities (Ministries)
    # min_01, min_02

    # Mock _fetch_and_map_relations (AS_APPOINTED)
    # min_01 -> [pers_01 (2020-02 to 2020-08)]
    # min_02 -> [pers_01 (2021-05 to 2021-12)]

    # Mock _fetch_and_map_entities (Persons)
    # pers_01

    # Mock President context
    # gov_01 -> fetch_relation(AS_PRESIDENT) -> [pres_01 (Open-ended)]
    # pres_01 -> get_entities -> President Entity

    async def fetch_relation_handler(entityId, relation):
        if relation.name == "RENAMED_TO":
            return [Relation(relatedEntityId="dep_02")] if entityId == "dep_01" else []
        if relation.name == "AS_DEPARTMENT":
            if entityId == "dep_01":
                return [
                    Relation(
                        relatedEntityId="min_01",
                        startTime="2020-01-01T00:00:00Z",
                        endTime="2021-01-01T00:00:00Z",
                    )
                ]
            if entityId == "dep_02":
                return [
                    Relation(
                        relatedEntityId="min_02",
                        startTime="2021-01-01T00:00:00Z",
                        endTime="2022-01-01T00:00:00Z",
                    )
                ]
        if relation.name == "AS_APPOINTED":
            if entityId == "min_01":
                return [
                    Relation(
                        relatedEntityId="pers_01",
                        startTime="2020-02-01T00:00:00Z",
                        endTime="2020-08-01T00:00:00Z",
                    )
                ]
            if entityId == "min_02":
                return [
                    Relation(
                        relatedEntityId="pers_01",
                        startTime="2021-05-01T00:00:00Z",
                        endTime="2021-12-01T00:00:00Z",
                    )
                ]
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [
                Relation(
                    relatedEntityId="pres_01",
                    startTime="2019-01-01T00:00:00Z",
                    endTime="",
                )
            ]
        return []

    async def get_entities_handler(entity):
        mapping = {
            "min_01": "4d696e6973747279204f6e65",
            "min_02": "4d696e69737472792054776f",
            "pers_01": "4d696e69737465722041",
            "pres_01": "507265736964656e742058",
        }
        name_hex = mapping.get(entity.id)
        return (
            [Entity(id=entity.id, name=f'{{"value": "{name_hex}"}}')]
            if name_hex
            else []
        )

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler
    mock_opengin_service.get_entities.side_effect = get_entities_handler

    result = await organisation_service.department_history_timeline(
        department_id=department_id
    )

    assert result is not None
    assert isinstance(result, list)

    # We expect:
    # 1. 2021-12-01 to 2022-01-01: Ministry Two - Gap (filled by President X)
    # 2. 2021-05-01 to 2021-12-01: Ministry Two - Minister A
    # 3. 2021-01-01 to 2021-05-01: Ministry Two - Gap (filled by President X)
    # 4. 2020-08-01 to 2021-01-01: Ministry One - Gap (filled by President X)
    # 5. 2020-02-01 to 2020-08-01: Ministry One - Minister A
    # 6. 2020-01-01 to 2020-02-01: Ministry One - Gap (filled by President X)

    # Note: Sequential entries with SAME MINISTER and SAME MINISTRY NAME are collapsed.
    # In this test, min_01 and min_02 have different names ("Ministry One" vs "Ministry Two"),
    # so Minister A won't collapse across them.

    assert len(result) == 6
    assert result[0]["minister_name"] == "President X"
    assert result[1]["minister_name"] == "Minister A"
    assert result[1]["ministry_name"] == "Ministry Two"
    assert result[4]["minister_name"] == "Minister A"
    assert result[4]["ministry_name"] == "Ministry One"
    assert "period" in result[0]
    assert "startTime" not in result[0]
    assert "endTime" not in result[0]


@pytest.mark.asyncio
async def test_department_history_timeline_collapsing(
    organisation_service, mock_opengin_service
):
    # Setup IDs: Same ministry name ("Ministry of Media") for two different ministry IDs
    department_id = "dep_01"

    async def fetch_relation_handler(entityId, relation):
        if relation.name == "RENAMED_TO":
            return []
        if relation.name == "AS_DEPARTMENT":
            return [
                Relation(
                    relatedEntityId="min_01",
                    startTime="2020-01-01T00:00:00Z",
                    endTime="2021-01-01T00:00:00Z",
                ),
                Relation(
                    relatedEntityId="min_02",
                    startTime="2021-01-01T00:00:00Z",
                    endTime="2022-01-01T00:00:00Z",
                ),
            ]
        if relation.name == "AS_APPOINTED":
            if entityId == "min_01":
                return [
                    Relation(
                        relatedEntityId="pers_01",
                        startTime="2020-01-01T00:00:00Z",
                        endTime="2021-01-01T00:00:00Z",
                    )
                ]
            if entityId == "min_02":
                return [
                    Relation(
                        relatedEntityId="pers_01",
                        startTime="2021-01-01T00:00:00Z",
                        endTime="2022-01-01T00:00:00Z",
                    )
                ]
        return []

    async def get_entities_handler(entity):
        if entity.id in ["min_01", "min_02"]:
            # "Ministry of Media" in hex
            return [
                Entity(
                    id=entity.id, name='{"value": "4d696e6973747279206f66204d65646961"}'
                )
            ]
        if entity.id == "pers_01":
            # "Ranil" in hex
            return [Entity(id="pers_01", name='{"value": "52616e696c"}')]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler
    mock_opengin_service.get_entities.side_effect = get_entities_handler

    result = await organisation_service.department_history_timeline(
        department_id=department_id
    )

    # Should collapse into ONE entry because same name and same person across min_01 and min_02
    assert len(result) == 1
    assert result[0]["minister_name"] == "Ranil"
    assert result[0]["period"] == "2020-01-01 - 2022-01-01"


@pytest.mark.asyncio
async def test_get_renamed_lineage_chain(organisation_service, mock_opengin_service):
    # Chain: A -> B -> C
    start_id = "A"
    mock_opengin_service.fetch_relation.side_effect = [
        [Relation(relatedEntityId="B")],  # A -> B
        [Relation(relatedEntityId="C")],  # B -> C
        [],  # C -> none
    ]

    result = await organisation_service._get_renamed_lineage(start_id)
    assert result == {"A", "B", "C"}
    assert mock_opengin_service.fetch_relation.call_count == 3


@pytest.mark.asyncio
async def test_get_renamed_lineage_no_renaming(
    organisation_service, mock_opengin_service
):
    start_id = "A"
    mock_opengin_service.fetch_relation.return_value = []

    result = await organisation_service._get_renamed_lineage(start_id)
    assert result == {"A"}
    mock_opengin_service.fetch_relation.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_and_map_entities_success(
    organisation_service, mock_opengin_service
):
    entity_ids = ["e1", "e2"]
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="e1", name="name1")],
        [Entity(id="e2", name="name2")],
    ]

    result = await organisation_service._fetch_and_map_entities(entity_ids)

    assert len(result) == 2
    assert result["e1"].id == "e1"
    assert result["e2"].id == "e2"
    assert mock_opengin_service.get_entities.call_count == 2


@pytest.mark.asyncio
async def test_fetch_and_map_entities_partial_failure(
    organisation_service, mock_opengin_service
):
    entity_ids = ["e1", "e2"]
    # Suppose e2 fails or returns nothing
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="e1", name="name1")],
        Exception("Failed to fetch"),
    ]

    result = await organisation_service._fetch_and_map_entities(entity_ids)

    assert len(result) == 1
    assert "e1" in result
    assert "e2" not in result


@pytest.mark.asyncio
async def test_resolve_entity_names_success(organisation_service, mock_opengin_service):
    entity_ids = ["e1", "e2", "e1"]
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="e1", name="encoded_name_1")],
        [Entity(id="e2", name="encoded_name_2")],
    ]

    with patch(
        "services.organisation_service.Util.decode_protobuf_attribute_name",
        side_effect=lambda name: f"decoded_{name}",
    ):
        result = await organisation_service.resolve_entity_names(entity_ids)

    assert result == {
        "e1": "decoded_encoded_name_1",
        "e2": "decoded_encoded_name_2",
    }
    assert mock_opengin_service.get_entities.call_count == 2


@pytest.mark.asyncio
async def test_resolve_entity_names_partial_failure(
    organisation_service, mock_opengin_service
):
    entity_ids = ["e1", "e2"]
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="e1", name="encoded_name_1")],
        Exception("Failed to fetch"),
    ]

    with patch(
        "services.organisation_service.Util.decode_protobuf_attribute_name",
        side_effect=lambda name: f"decoded_{name}",
    ):
        result = await organisation_service.resolve_entity_names(entity_ids)

    assert result == {"e1": "decoded_encoded_name_1"}


@pytest.mark.asyncio
async def test_resolve_entity_names_empty_list(organisation_service):
    result = await organisation_service.resolve_entity_names([])
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_and_map_relations_success(
    organisation_service, mock_opengin_service
):
    entity_ids = ["e1", "e2"]
    query = Relation(name="TEST")
    r1 = Relation(relatedEntityId="r1")
    r2 = Relation(relatedEntityId="r2")

    mock_opengin_service.fetch_relation.side_effect = [[r1], [r2]]

    result = await organisation_service._fetch_and_map_relations(entity_ids, query)

    assert len(result) == 2
    assert result["e1"] == [r1]
    assert result["e2"] == [r2]


@pytest.mark.asyncio
async def test_fetch_and_map_relations_with_errors(
    organisation_service, mock_opengin_service
):
    entity_ids = ["e1", "e2"]
    query = Relation(name="TEST")

    mock_opengin_service.fetch_relation.side_effect = [
        [Relation(relatedEntityId="r1")],
        Exception("Error"),
    ]

    result = await organisation_service._fetch_and_map_relations(entity_ids, query)

    assert len(result) == 2
    assert len(result["e1"]) == 1
    assert result["e2"] == []  # Should default to empty list on error


@pytest.mark.asyncio
async def test_fetch_cabinet_flow_too_many_dates(organisation_service):
    president_id = "pres1"
    dates = [
        f"2024-09-{day:02d}" for day in range(1, 12)
    ]  # 11 dates; max allowed is 10

    with pytest.raises(BadRequestError):
        await organisation_service.fetch_cabinet_flow(president_id, dates)


@pytest.mark.asyncio
async def test_fetch_cabinet_flow_single_date_fails(organisation_service):
    president_id = "pres1"
    dates = ["2024-09-23"]

    with pytest.raises(ValueError):
        await organisation_service.fetch_cabinet_flow(president_id, dates)


@pytest.mark.asyncio
async def test_department_moves_between_ministers(organisation_service):
    organisation_service.get_ministers_and_departments = AsyncMock(
        side_effect=[
            [
                {"ministerId": "min4", "departmentId": "dep177"},
                {"ministerId": "min4", "departmentId": "dep175"},
                {"ministerId": "min4", "departmentId": "dep182"},
                {"ministerId": "min4", "departmentId": "dep176"},
            ],
            [
                {"ministerId": "min9", "departmentId": "dep177"},  # moved
                {"ministerId": "min4", "departmentId": "dep175"},  # same
                {"ministerId": "min4", "departmentId": "dep182"},  # same
                {"ministerId": "min10", "departmentId": "dep176"},  # moved
            ],
        ]
    )

    mock_entity1 = MagicMock()
    mock_entity1.id = "min4"
    mock_entity1.name = "Minister 4"

    mock_entity2 = MagicMock()
    mock_entity2.id = "min9"
    mock_entity2.name = "Minister 9"

    mock_entity3 = MagicMock()
    mock_entity3.id = "min10"
    mock_entity3.name = "Minister 10"

    organisation_service.opengin_service.get_entities = AsyncMock(
        return_value=[mock_entity1, mock_entity2, mock_entity3]
    )

    result = await organisation_service.fetch_cabinet_flow(
        president_id="pres1", dates=["2024-01-01", "2024-02-01"]
    )

    # 2 departments moved (dep177 and dep176)
    assert len(result["links"]) == 3

    # total movements should equal 4
    total_flow = sum(link["value"] for link in result["links"])
    assert total_flow == 4

    all_department_ids = {
        department_id
        for link in result["links"]
        for department_id in link["departmentIds"]
    }
    assert all_department_ids == {"dep175", "dep176", "dep177", "dep182"}
    for link in result["links"]:
        assert link["value"] == len(link["departmentIds"])

    # nodes should exist
    assert len(result["nodes"]) > 0

    # date statuses should be ok
    assert result["dates"][0]["status"] == "ok"
    assert result["dates"][1]["status"] == "ok"

    # dependency should be called once per date
    assert organisation_service.get_ministers_and_departments.call_count == 2


@pytest.mark.asyncio
async def test_no_departments(organisation_service):
    organisation_service.get_ministers_and_departments = AsyncMock(return_value=[])

    result = await organisation_service.fetch_cabinet_flow(
        "pres1", ["2024-01-01", "2024-01-02"]
    )

    assert result["nodes"] == []
    assert result["links"] == []
    assert result["dates"][0]["status"] == "no_data"
    assert result["dates"][0]["departmentsCount"] == 0
    assert result["dates"][1]["status"] == "no_data"
    assert result["dates"][1]["departmentsCount"] == 0


@pytest.mark.asyncio
async def test_no_departments_for_one_date(organisation_service):
    organisation_service.get_ministers_and_departments = AsyncMock(
        side_effect=[
            [
                {"ministerId": "min3", "departmentId": "dep177"},
                {"ministerId": "min4", "departmentId": "dep175"},
                {"ministerId": "min5", "departmentId": "dep182"},
                {"ministerId": "min6", "departmentId": "dep176"},
            ],
            [],
        ]
    )

    mock_entity1 = MagicMock()
    mock_entity1.id = "min3"
    mock_entity1.name = "Minister 3"

    mock_entity2 = MagicMock()
    mock_entity2.id = "min4"
    mock_entity2.name = "Minister 4"

    mock_entity3 = MagicMock()
    mock_entity3.id = "min5"
    mock_entity3.name = "Minister 5"

    mock_entity4 = MagicMock()
    mock_entity4.id = "min6"
    mock_entity4.name = "Minister 6"

    organisation_service.opengin_service.get_entities = AsyncMock(
        return_value=[mock_entity1, mock_entity2, mock_entity3, mock_entity4]
    )

    result = await organisation_service.fetch_cabinet_flow(
        president_id="pres1", dates=["2024-01-01", "2024-02-01"]
    )

    # no movement on departments since the second date is empty
    assert len(result["links"]) == 0

    # total movements should equal 0
    total_flow = sum(link["value"] for link in result["links"])
    assert total_flow == 0

    # nodes should exist
    assert len(result["nodes"]) > 0
    assert len(result["nodes"]) == 4

    # date statuses should be ok and one date should be no_data
    assert result["dates"][0]["status"] == "ok"
    assert result["dates"][0]["departmentsCount"] == 4
    assert result["dates"][1]["status"] == "no_data"
    assert result["dates"][1]["departmentsCount"] == 0

    # dependency should be called once per date
    assert organisation_service.get_ministers_and_departments.call_count == 2


@pytest.mark.asyncio
async def test_bridge_across_empty_middle_date(organisation_service):
    organisation_service.get_ministers_and_departments = AsyncMock(
        side_effect=[
            [
                {"ministerId": "min4", "departmentId": "dep177"},
                {"ministerId": "min4", "departmentId": "dep175"},
                {"ministerId": "min4", "departmentId": "dep182"},
                {"ministerId": "min4", "departmentId": "dep176"},
            ],
            [],
            [
                {"ministerId": "min9", "departmentId": "dep177"},
                {"ministerId": "min4", "departmentId": "dep175"},
                {"ministerId": "min4", "departmentId": "dep182"},
                {"ministerId": "min10", "departmentId": "dep176"},
            ],
        ]
    )

    mock_entity1 = MagicMock()
    mock_entity1.id = "min4"
    mock_entity1.name = "Minister 4"

    mock_entity2 = MagicMock()
    mock_entity2.id = "min9"
    mock_entity2.name = "Minister 9"

    mock_entity3 = MagicMock()
    mock_entity3.id = "min10"
    mock_entity3.name = "Minister 10"

    organisation_service.opengin_service.get_entities = AsyncMock(
        return_value=[mock_entity1, mock_entity2, mock_entity3]
    )

    result = await organisation_service.fetch_cabinet_flow(
        president_id="pres1",
        dates=["2024-01-01", "2024-02-01", "2024-03-01"],
    )

    assert result["dates"][0]["status"] == "ok"
    assert result["dates"][0]["departmentsCount"] == 4
    assert result["dates"][1]["status"] == "no_data"
    assert result["dates"][1]["departmentsCount"] == 0
    assert result["dates"][2]["status"] == "ok"
    assert result["dates"][2]["departmentsCount"] == 4

    # links bridge across the empty middle date (same as two consecutive ok dates)
    assert len(result["links"]) == 3
    total_flow = sum(link["value"] for link in result["links"])
    assert total_flow == 4

    all_department_ids = {
        department_id
        for link in result["links"]
        for department_id in link["departmentIds"]
    }
    assert all_department_ids == {"dep175", "dep176", "dep177", "dep182"}

    node_times = {node["time"] for node in result["nodes"]}
    assert node_times == {"2024-01-01", "2024-03-01"}

    assert organisation_service.get_ministers_and_departments.call_count == 3


@pytest.mark.asyncio
async def test_one_date_failure(organisation_service):
    organisation_service.get_ministers_and_departments = AsyncMock(
        side_effect=[
            Exception("API failed"),
            [{"ministerId": "min3", "departmentId": "dep177"}],
        ]
    )

    mock_entity1 = MagicMock()
    mock_entity1.id = "min3"
    mock_entity1.name = "Minister 3"

    organisation_service.opengin_service.get_entities = AsyncMock(
        return_value=[mock_entity1]
    )

    result = await organisation_service.fetch_cabinet_flow(
        "pres1", ["2024-01-01", "2024-02-01"]
    )

    assert result["dates"][0]["status"] == "error"
    assert result["dates"][1]["status"] == "ok"


@pytest.mark.asyncio
async def test_invalid_response_type(organisation_service):
    organisation_service.get_ministers_and_departments = AsyncMock(
        side_effect=["hi test 1", "hi test 2"]
    )

    result = await organisation_service.fetch_cabinet_flow(
        "pres1", ["2024-01-01", "2024-01-02"]
    )

    assert result["dates"][0]["status"] == "error"
    assert result["dates"][1]["status"] == "error"


@pytest.mark.asyncio
async def test_multiple_departments_aggregation(organisation_service):
    """
    Test that multiple departments moving along the same minister path are aggregated into a single link
    with correct value.
    """

    # Two dates: both departments move from min1 -> min2
    organisation_service.get_ministers_and_departments = AsyncMock(
        side_effect=[
            [  #
                {"ministerId": "min1", "departmentId": "dep1"},
                {"ministerId": "min1", "departmentId": "dep2"},
            ],
            [
                {"ministerId": "min2", "departmentId": "dep1"},
                {"ministerId": "min2", "departmentId": "dep2"},
            ],
        ]
    )

    mock_entity1 = MagicMock()
    mock_entity1.id = "min1"
    mock_entity1.name = "Minister 1"

    mock_entity2 = MagicMock()
    mock_entity2.id = "min2"
    mock_entity2.name = "Minister 2"

    organisation_service.opengin_service.get_entities = AsyncMock(
        return_value=[mock_entity1, mock_entity2]
    )

    result = await organisation_service.fetch_cabinet_flow(
        president_id="pres1", dates=["2024-01-01", "2024-02-01"]
    )

    # There should be exactly one link (min1 -> min2)
    assert len(result["links"]) == 1

    # The value should be 2 because two departments moved along this path
    link = result["links"][0]
    assert link["value"] == 2
    assert set(link["departmentIds"]) == {"dep1", "dep2"}

    # Nodes should exist for both ministers
    node_ids = {node["id"] for node in result["nodes"]}
    assert node_ids == {"min1", "min2"}

    # Dates statuses should both be "ok"
    assert result["dates"][0]["status"] == "ok"
    assert result["dates"][1]["status"] == "ok"

    # Dependency should be called once per date
    assert organisation_service.get_ministers_and_departments.call_count == 2


@pytest.mark.asyncio
async def test_enrich_body_item_is_new_true(organisation_service, mock_opengin_service):
    body_relation = Relation(
        relatedEntityId="body_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )
    selected_date = "2023-10-27T00:00:00Z"

    mock_opengin_service.get_entities.return_value = [
        Entity(
            id="body_123",
            name="mocked_protobuf_name",
            kind={"major": "Body", "minor": "Council"},
        )
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="National Police Academy",
    ):
        result = await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date=selected_date
        )

    assert result == {
        "id": "body_123",
        "name": "National Police Academy",
        "isNew": True,
        "type": "Council",
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id="body_123")
    )


@pytest.mark.asyncio
async def test_enrich_body_item_is_new_false(
    organisation_service, mock_opengin_service
):
    body_relation = Relation(
        relatedEntityId="body_123",
        startTime="2020-01-01T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )
    selected_date = "2023-10-27T00:00:00Z"

    mock_opengin_service.get_entities.return_value = [
        Entity(
            id="body_123",
            name="mocked_protobuf_name",
            kind={"major": "Body", "minor": "Council"},
        )
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="National Police Academy",
    ):
        result = await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date=selected_date
        )

    assert result == {
        "id": "body_123",
        "name": "National Police Academy",
        "isNew": False,
        "type": "Council",
    }


@pytest.mark.asyncio
async def test_enrich_body_item_empty_minor_kind(
    organisation_service, mock_opengin_service
):
    body_relation = Relation(
        relatedEntityId="body_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )
    selected_date = "2023-10-27T00:00:00Z"

    mock_opengin_service.get_entities.return_value = [
        Entity(id="body_123", name="mocked_protobuf_name")
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="Unnamed Body",
    ):
        result = await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date=selected_date
        )

    assert result["type"] == ""


@pytest.mark.asyncio
async def test_enrich_body_item_missing_related_entity_id(organisation_service):
    body_relation = Relation(
        relatedEntityId="",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )

    with pytest.raises(ValueError):
        await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date="2023-10-27T00:00:00Z"
        )


@pytest.mark.asyncio
async def test_enrich_body_item_entity_not_found(
    organisation_service, mock_opengin_service
):
    body_relation = Relation(
        relatedEntityId="body_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )

    mock_opengin_service.get_entities.side_effect = NotFoundError("Entity not found")

    with pytest.raises(NotFoundError):
        await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date="2023-10-27T00:00:00Z"
        )


@pytest.mark.asyncio
async def test_enrich_body_item_get_entities_generic_error(
    organisation_service, mock_opengin_service
):
    body_relation = Relation(
        relatedEntityId="body_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )

    mock_opengin_service.get_entities.side_effect = Exception("connection reset")

    with pytest.raises(InternalServerError):
        await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date="2023-10-27T00:00:00Z"
        )


@pytest.mark.asyncio
async def test_enrich_body_item_get_entities_empty_list(
    organisation_service, mock_opengin_service
):
    body_relation = Relation(
        relatedEntityId="body_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = []

    with pytest.raises(NotFoundError):
        await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date="2023-10-27T00:00:00Z"
        )


@pytest.mark.asyncio
async def test_enrich_body_item_name_decode_failure(
    organisation_service, mock_opengin_service
):
    body_relation = Relation(
        relatedEntityId="body_123",
        startTime="2023-10-27T00:00:00Z",
        endTime="2024-10-27T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id="body_123", name="malformed_protobuf_bytes")
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        side_effect=ValueError("bad protobuf"),
    ):
        with pytest.raises(InternalServerError):
            await organisation_service.enrich_body_item(
                body_relation=body_relation, selected_date="2023-10-27T00:00:00Z"
            )


@pytest.mark.asyncio
async def test_bodies_by_department_empty_department_id(organisation_service):
    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id="", selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_none_department_id(organisation_service):
    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id=None, selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_empty_selected_date(organisation_service):
    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date=""
        )


@pytest.mark.asyncio
async def test_bodies_by_department_none_selected_date(organisation_service):
    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date=None
        )


@pytest.mark.asyncio
async def test_bodies_by_department_department_not_found(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_entity.side_effect = NotFoundError("not found")

    with pytest.raises(NotFoundError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )

    mock_opengin_service.fetch_relation.assert_not_called()


@pytest.mark.asyncio
async def test_bodies_by_department_fetch_entity_generic_error(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_entity.side_effect = Exception("connection reset")

    with pytest.raises(InternalServerError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )

    mock_opengin_service.fetch_relation.assert_not_called()


@pytest.mark.asyncio
async def test_bodies_by_department_department_entity_none(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_entity.return_value = None

    with pytest.raises(NotFoundError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )

    mock_opengin_service.fetch_relation.assert_not_called()


@pytest.mark.asyncio
async def test_bodies_by_department_fetch_relation_bad_request(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_entity.return_value = Entity(id="department_123")
    mock_opengin_service.fetch_relation.side_effect = BadRequestError("bad request")

    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_fetch_relation_not_found(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_entity.return_value = Entity(id="department_123")
    mock_opengin_service.fetch_relation.side_effect = NotFoundError("not found")

    with pytest.raises(NotFoundError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_fetch_relation_generic_error(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_entity.return_value = Entity(id="department_123")
    # simulates the Neo4j DateTime parse error class of failure
    mock_opengin_service.fetch_relation.side_effect = Exception(
        "Neo4jError: Neo.ClientError.Statement.SyntaxError"
    )

    with pytest.raises(InternalServerError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_no_relations_found(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_entity.return_value = Entity(id="department_123")
    mock_opengin_service.fetch_relation.return_value = []

    result = await organisation_service.bodies_by_department(
        department_id="department_123", selected_date="2023-10-27"
    )

    assert result == {
        "totalBodies": 0,
        "newBodies": 0,
        "bodyList": [],
    }


@pytest.mark.asyncio
async def test_bodies_by_department_success(organisation_service, mock_opengin_service):
    department_id = "department_123"
    selected_date = "2023-10-27"

    mock_opengin_service.fetch_entity.return_value = Entity(id=department_id)
    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            id="",
            relatedEntityId="body_1",
            name=RelationNameEnum.AS_BODY.value,
            startTime="2023-10-27T00:00:00Z",
            endTime="",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
        Relation(
            id="",
            relatedEntityId="body_2",
            name=RelationNameEnum.AS_BODY.value,
            startTime="2020-01-01T00:00:00Z",
            endTime="",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    ]

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_body_item",
        new_callable=AsyncMock,
    ) as mock_enrich_body:
        mock_enrich_body.side_effect = [
            {
                "id": "body_1",
                "name": "National Police Academy",
                "isNew": True,
                "type": "Council",
            },
            {
                "id": "body_2",
                "name": "Assets vested to the Treasury",
                "isNew": False,
                "type": "",
            },
        ]

        result = await organisation_service.bodies_by_department(
            department_id=department_id, selected_date=selected_date
        )

    assert result == {
        "totalBodies": 2,
        "newBodies": 1,
        "bodyList": [
            {
                "id": "body_1",
                "name": "National Police Academy",
                "isNew": True,
                "type": "Council",
            },
            {
                "id": "body_2",
                "name": "Assets vested to the Treasury",
                "isNew": False,
                "type": "",
            },
        ],
    }

    mock_opengin_service.fetch_entity.assert_called_once_with(entityId=department_id)
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId=department_id,
        relation=Relation(
            name=RelationNameEnum.AS_BODY.value,
            activeAt=Util.normalize_timestamp(selected_date),
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    )
    assert mock_enrich_body.call_count == 2


@pytest.mark.asyncio
async def test_bodies_by_department_partial_enrichment_failure(
    organisation_service, mock_opengin_service
):
    department_id = "department_123"
    selected_date = "2023-10-27"

    mock_opengin_service.fetch_entity.return_value = Entity(id=department_id)
    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            id="",
            relatedEntityId="body_1",
            name=RelationNameEnum.AS_BODY.value,
            startTime="2023-10-27T00:00:00Z",
            endTime="",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
        Relation(
            id="",
            relatedEntityId="body_2",
            name=RelationNameEnum.AS_BODY.value,
            startTime="2020-01-01T00:00:00Z",
            endTime="",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    ]

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_body_item",
        new_callable=AsyncMock,
    ) as mock_enrich_body:
        mock_enrich_body.side_effect = [
            {
                "id": "body_1",
                "name": "National Police Academy",
                "isNew": True,
                "type": "Council",
            },
            InternalServerError("enrichment failed for body_2"),
        ]

        result = await organisation_service.bodies_by_department(
            department_id=department_id, selected_date=selected_date
        )

    assert result == {
        "totalBodies": 1,
        "newBodies": 1,
        "bodyList": [
            {
                "id": "body_1",
                "name": "National Police Academy",
                "isNew": True,
                "type": "Council",
            }
        ],
    }


@pytest.mark.asyncio
async def test_bodies_by_department_all_enrichments_fail(
    organisation_service, mock_opengin_service
):
    department_id = "department_123"
    selected_date = "2023-10-27"

    mock_opengin_service.fetch_entity.return_value = Entity(id=department_id)
    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            id="",
            relatedEntityId="body_1",
            name=RelationNameEnum.AS_BODY.value,
            startTime="2023-10-27T00:00:00Z",
            endTime="",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
        Relation(
            id="",
            relatedEntityId="body_2",
            name=RelationNameEnum.AS_BODY.value,
            startTime="2020-01-01T00:00:00Z",
            endTime="",
            direction=RelationDirectionEnum.OUTGOING.value,
        ),
    ]

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_body_item",
        new_callable=AsyncMock,
    ) as mock_enrich_body:
        mock_enrich_body.side_effect = [
            InternalServerError("enrichment failed for body_1"),
            InternalServerError("enrichment failed for body_2"),
        ]

        with pytest.raises(InternalServerError):
            await organisation_service.bodies_by_department(
                department_id=department_id, selected_date=selected_date
            )


@pytest.mark.asyncio
async def test_bodies_by_department_passes_normalized_date_to_enrich(
    organisation_service, mock_opengin_service
):
    department_id = "department_123"
    selected_date = "2023-10-27"
    normalized_date = Util.normalize_timestamp(selected_date)

    body_relation = Relation(
        id="",
        relatedEntityId="body_1",
        name=RelationNameEnum.AS_BODY.value,
        startTime=normalized_date,
        endTime="",
        direction=RelationDirectionEnum.OUTGOING.value,
    )
    mock_opengin_service.fetch_entity.return_value = Entity(id=department_id)
    mock_opengin_service.fetch_relation.return_value = [body_relation]

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_body_item",
        new_callable=AsyncMock,
    ) as mock_enrich_body:
        mock_enrich_body.return_value = {
            "id": "body_1",
            "name": "National Police Academy",
            "isNew": True,
            "type": "Council",
        }

        await organisation_service.bodies_by_department(
            department_id=department_id, selected_date=selected_date
        )

    mock_enrich_body.assert_called_once_with(
        body_relation=body_relation, selected_date=normalized_date
    )
