import json
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


# --- Tests for fetch_presidents ---
@pytest.mark.asyncio
async def test_fetch_presidents_success(organisation_service, mock_opengin_service):

    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            relatedEntityId="p1",
            startTime="2020-01-01T00:00:00Z",
            endTime="2022-01-01T00:00:00Z",
        ),
        Relation(relatedEntityId="p1", startTime="2022-06-01T00:00:00Z", endTime=""),
    ]

    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="g_org", created="2020-05-01T00:00:00Z", name="org_gzt")],
        [Entity(id="g_per", created="2022-08-01T00:00:00Z", name="per_gzt")],
        [Entity(id="p1", name="President One")],  # president name fetch
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        side_effect=lambda x: x,
    ):
        result = await organisation_service.fetch_presidents()

        presidents = result["body"]
        assert len(presidents) == 1
        president = presidents[0]
        assert president["id"] == "p1"
        assert president["name"] == "President One"
        assert len(president["tenureList"]) == 2

        # Check gazettes are inside the first term (2020 term)
        term1_gazettes = president["tenureList"][0]["gazetteList"]
        assert len(term1_gazettes) == 1
        assert term1_gazettes[0]["date"] == "2020-05-01"
        assert isinstance(term1_gazettes[0]["idList"], list)
        assert term1_gazettes[0]["idList"] == ["org_gzt"]

        # Check gazettes are inside the second term (2022 term)
        term2_gazettes = president["tenureList"][1]["gazetteList"]
        assert len(term2_gazettes) == 1
        assert term2_gazettes[0]["date"] == "2022-08-01"
        assert isinstance(term2_gazettes[0]["idList"], list)
        assert term2_gazettes[0]["idList"] == ["per_gzt"]

        # Verify JSON serializability of the entire response
        json_output = json.dumps(result)
        assert isinstance(json_output, str)


@pytest.mark.asyncio
async def test_fetch_presidents_no_data(organisation_service, mock_opengin_service):
    mock_opengin_service.fetch_relation.return_value = []

    result = await organisation_service.fetch_presidents()

    assert result == {"body": []}


@pytest.mark.asyncio
async def test_fetch_presidents_no_gazettes(organisation_service, mock_opengin_service):
    mock_opengin_service.fetch_relation.return_value = [
        Relation(relatedEntityId="p1", startTime="2020-01-01T00:00:00Z", endTime="")
    ]

    mock_opengin_service.get_entities.side_effect = [
        [],  # No organization gazettes
        [],  # No person gazettes
        [Entity(id="p1", name="President One")],
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        side_effect=lambda x: x,
    ):
        result = await organisation_service.fetch_presidents()

        presidents = result["body"]
        assert len(presidents) == 1
        assert presidents[0]["name"] == "President One"
        assert presidents[0]["tenureList"][0]["gazetteList"] == []


@pytest.mark.asyncio
async def test_fetch_presidents_sorting_with_multiple_terms(
    organisation_service, mock_opengin_service
):
    # Setup:
    # p_old started in 2010
    # p_multi started in 2005 AND 2022.
    # Even though p_multi has a 2005 term, their 2022 term should put them at the TOP.

    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            relatedEntityId="p_old",
            startTime="2010-01-01T00:00:00Z",
            endTime="2015-01-01T00:00:00Z",
        ),
        Relation(
            relatedEntityId="p_multi",
            startTime="2005-01-01T00:00:00Z",
            endTime="2009-12-31T00:00:00Z",
        ),
        Relation(
            relatedEntityId="p_multi", startTime="2022-01-01T00:00:00Z", endTime=""
        ),
    ]

    mock_opengin_service.get_entities.side_effect = [
        [],
        [],  # no gazettes for either
        [Entity(id="p_old", name="Old President")],
        [Entity(id="p_multi", name="Multi-term President")],
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        side_effect=lambda x: x,
    ):
        result = await organisation_service.fetch_presidents()

        presidents = result["body"]

        # p_multi should be first because 2022 > 2010
        assert presidents[0]["id"] == "p_multi"
        assert presidents[1]["id"] == "p_old"


@pytest.mark.asyncio
async def test_fetch_presidents_internal_error(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.fetch_relation.side_effect = Exception("Database down")

    with pytest.raises(InternalServerError):
        await organisation_service.fetch_presidents()


@pytest.mark.asyncio
async def test_fetch_presidents_gazette_on_last_day_of_tenure_is_included(
    organisation_service, mock_opengin_service
):
    """
    A gazette published on the EXACT endDate of a tenure must be included
    in that tenure's gazetteList.
    """
    # p1 has a single tenure ending on 2022-01-01
    mock_opengin_service.fetch_relation.return_value = [
        Relation(
            relatedEntityId="p1",
            startTime="2020-01-01T00:00:00Z",
            endTime="2022-01-01T00:00:00Z",  # last day is 2022-01-01
        ),
    ]

    # The gazette is published on the exact last day of p1's tenure
    mock_opengin_service.get_entities.side_effect = [
        [
            Entity(created="2022-01-01T00:00:00Z", name="last_day_gazette")
        ],  # org gazettes
        [],  # person gazettes
        [Entity(id="p1", name="President One")],  # p1 name fetch
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        side_effect=lambda x: x,
    ):
        result = await organisation_service.fetch_presidents()

        presidents = result["body"]
        assert len(presidents) == 1
        president = presidents[0]
        assert president["id"] == "p1"
        assert president["name"] == "President One"
        assert len(president["tenureList"]) == 1

        tenure = president["tenureList"][0]
        assert tenure["endDate"] == "2022-01-01"

        # The gazette on the exact last day must be INCLUDED, not dropped
        assert len(tenure["gazetteList"]) == 1
        assert tenure["gazetteList"][0]["date"] == "2022-01-01"
        assert "last_day_gazette" in tenure["gazetteList"][0]["idList"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start_time, selected_date, expected_is_new",
    [
        ("2023-10-27T00:00:00Z", "2023-10-27T00:00:00Z", True),
        ("2020-01-01T00:00:00Z", "2023-10-27T00:00:00Z", False),
    ],
)
async def test_enrich_body_item_is_new(
    organisation_service,
    mock_opengin_service,
    start_time,
    selected_date,
    expected_is_new,
):
    body_relation = Relation(relatedEntityId="body_123", startTime=start_time)
    mock_opengin_service.get_entities.return_value = [
        Entity(
            id="body_123",
            name="mocked_protobuf_name",
            kind={"major": "Body", "minor": "Council"},
        )
    ]

    with patch(
        "src.services.organisation_service.Util.decode_protobuf_attribute_name",
        return_value="decoded_name",
    ):
        result = await organisation_service.enrich_body_item(
            body_relation=body_relation, selected_date=selected_date
        )

    assert result == {
        "id": "body_123",
        "name": "decoded_name",
        "isNew": expected_is_new,
        "type": "Council",
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id="body_123")
    )


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
    mock_opengin_service.get_entities.side_effect = NotFoundError("not found")

    with pytest.raises(NotFoundError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )

    mock_opengin_service.fetch_relation.assert_not_called()


@pytest.mark.asyncio
async def test_bodies_by_department_get_entities_generic_error(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.get_entities.side_effect = Exception("connection reset")

    with pytest.raises(InternalServerError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )

    mock_opengin_service.fetch_relation.assert_not_called()


@pytest.mark.asyncio
async def test_bodies_by_department_department_entity_empty_list(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.get_entities.return_value = []

    with pytest.raises(NotFoundError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )

    mock_opengin_service.fetch_relation.assert_not_called()


@pytest.mark.asyncio
async def test_bodies_by_department_fetch_relation_bad_request(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.get_entities.return_value = [Entity(id="department_123")]
    mock_opengin_service.fetch_relation.side_effect = BadRequestError("bad request")

    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_fetch_relation_not_found(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.get_entities.return_value = [Entity(id="department_123")]
    mock_opengin_service.fetch_relation.side_effect = NotFoundError("not found")

    with pytest.raises(NotFoundError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_fetch_relation_generic_error(
    organisation_service, mock_opengin_service
):
    mock_opengin_service.get_entities.return_value = [Entity(id="department_123")]
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
    mock_opengin_service.get_entities.return_value = [Entity(id="department_123")]
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

    mock_opengin_service.get_entities.return_value = [Entity(id=department_id)]
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
                "name": "Body 1 Name",
                "isNew": True,
                "type": "Council",
            },
            {
                "id": "body_2",
                "name": "Body 2 Name",
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
                "name": "Body 1 Name",
                "isNew": True,
                "type": "Council",
            },
            {
                "id": "body_2",
                "name": "Body 2 Name",
                "isNew": False,
                "type": "",
            },
        ],
    }

    mock_opengin_service.get_entities.assert_called_once_with(
        entity=Entity(id=department_id)
    )
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

    mock_opengin_service.get_entities.return_value = [Entity(id=department_id)]
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
                "name": "Body 1 Name",
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
                "name": "Body 1 Name",
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

    mock_opengin_service.get_entities.return_value = [Entity(id=department_id)]
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
    mock_opengin_service.get_entities.return_value = [Entity(id=department_id)]
    mock_opengin_service.fetch_relation.return_value = [body_relation]

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_body_item",
        new_callable=AsyncMock,
    ) as mock_enrich_body:
        mock_enrich_body.return_value = {
            "id": "body_1",
            "name": "Body 1 Name",
            "isNew": True,
            "type": "Council",
        }

        await organisation_service.bodies_by_department(
            department_id=department_id, selected_date=selected_date
        )

    mock_enrich_body.assert_called_once_with(
        body_relation=body_relation, selected_date=normalized_date
    )


@pytest.mark.asyncio
async def test_bodies_by_department_whitespace_department_id(organisation_service):
    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id="   ", selected_date="2023-10-27"
        )


@pytest.mark.asyncio
async def test_bodies_by_department_whitespace_selected_date(organisation_service):
    with pytest.raises(BadRequestError):
        await organisation_service.bodies_by_department(
            department_id="department_123", selected_date="   "
        )


# --- Tests for get_persons_by_portfolio ---


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "portfolio_id, selected_date",
    [
        ("", "2026-04-21"),
        (None, "2026-04-21"),
        ("   ", "2026-04-21"),
        ("min-12", ""),
        ("min-12", None),
        ("min-12", "       "),
    ],
)
async def test_get_persons_by_portfolio_invalid_inputs(
    organisation_service, portfolio_id, selected_date
):
    with pytest.raises(BadRequestError):
        await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_not_found(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"

    mock_opengin_service.get_entities.return_value = []

    with pytest.raises(NotFoundError):
        await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    # should fail before ever querying for president or ministers
    mock_opengin_service.fetch_relation.assert_not_called()


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_multiple_presidents_raises_internal_server_error(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"

    mock_opengin_service.get_entities.return_value = [
        Entity(id=portfolio_id, name="mocked_protobuf_name")
    ]

    async def fetch_relation_handler(entityId, relation):
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [
                Relation(relatedEntityId="pres_1"),
                Relation(relatedEntityId="pres_2"),
            ]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler

    with pytest.raises(InternalServerError):
        await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_no_minister_no_president_raises_not_found(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"

    mock_opengin_service.get_entities.return_value = [
        Entity(id=portfolio_id, name="mocked_protobuf_name")
    ]
    # no president relation, no appointed minister relation
    mock_opengin_service.fetch_relation.return_value = []

    with pytest.raises(NotFoundError):
        await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_appointed_minister_success(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"
    president_id = "pres_123"
    minister_relation = Relation(
        relatedEntityId="cit_minister_1",
        startTime="2020-01-01T00:00:00Z",
        endTime="2030-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id=portfolio_id, name="mocked_protobuf_name")
    ]

    async def fetch_relation_handler(entityId, relation):
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [Relation(relatedEntityId=president_id)]
        if (
            entityId == portfolio_id
            and relation.name == RelationNameEnum.AS_APPOINTED.value
        ):
            return [minister_relation]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {
            "id": "cit_minister_1",
            "name": "Test Minister",
            "isNew": False,
            "isPresident": False,
        }

        result = await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    assert result == {
        "totalCount": 1,
        "newCount": 0,
        "personList": [
            {
                "id": "cit_minister_1",
                "name": "Test Minister",
                "isNew": False,
                "isPresident": False,
            }
        ],
    }

    mock_enrich_person.assert_called_once_with(
        person_relation=minister_relation,
        president_id=president_id,
        selected_date=selected_date,
    )


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_minister_who_is_president_flagged_true(
    organisation_service, mock_opengin_service
):
    """When the appointed minister for a portfolio is also the current president,
    isPresident should be True in the returned person data."""
    portfolio_id = "min-12"
    selected_date = "2026-04-21"
    president_id = "pres_123"
    minister_relation = Relation(
        relatedEntityId=president_id,
        startTime="2020-01-01T00:00:00Z",
        endTime="2030-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id=portfolio_id, name="mocked_protobuf_name")
    ]

    async def fetch_relation_handler(entityId, relation):
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [Relation(relatedEntityId=president_id)]
        if (
            entityId == portfolio_id
            and relation.name == RelationNameEnum.AS_APPOINTED.value
        ):
            return [minister_relation]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {
            "id": president_id,
            "name": "President Acting As Minister",
            "isNew": False,
            "isPresident": True,
        }

        result = await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    assert result["personList"][0]["isPresident"] is True


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_new_minister_counted(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"
    president_id = "pres_123"
    minister_relation = Relation(
        relatedEntityId="cit_minister_1",
        startTime=Util.normalize_timestamp(selected_date),
        endTime="2030-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id=portfolio_id, name="mocked_protobuf_name")
    ]

    async def fetch_relation_handler(entityId, relation):
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [Relation(relatedEntityId=president_id)]
        if (
            entityId == portfolio_id
            and relation.name == RelationNameEnum.AS_APPOINTED.value
        ):
            return [minister_relation]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {
            "id": "cit_minister_1",
            "name": "New Minister",
            "isNew": True,
            "isPresident": False,
        }

        result = await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    assert result["newCount"] == 1
    assert result["totalCount"] == 1


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_no_minister_falls_back_to_president(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"
    president_id = "pres_123"

    mock_kind = MagicMock()
    mock_kind.minor = "cabinetMinister"

    mock_portfolio_entity = MagicMock(spec=Entity)
    mock_portfolio_entity.id = portfolio_id
    mock_portfolio_entity.name = "mocked_protobuf_name"
    mock_portfolio_entity.kind = mock_kind

    mock_opengin_service.get_entities.return_value = [mock_portfolio_entity]

    async def fetch_relation_handler(entityId, relation):
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [Relation(relatedEntityId=president_id)]
        if (
            entityId == portfolio_id
            and relation.name == RelationNameEnum.AS_APPOINTED.value
        ):
            return []  # no minister appointed
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {
            "id": president_id,
            "name": "The President",
            "isNew": False,
            "isPresident": True,
        }

        result = await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    assert result["totalCount"] == 1
    assert result["personList"][0]["id"] == president_id
    assert result["personList"][0]["isPresident"] is True

    mock_enrich_person.assert_called_once_with(
        president_id=president_id,
        is_president=True,
        selected_date=selected_date,
    )


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_partial_enrichment_failure_is_skipped(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"
    president_id = "pres_123"

    minister_ok = Relation(
        relatedEntityId="cit_ok",
        startTime="2020-01-01T00:00:00Z",
        endTime="2030-01-01T00:00:00Z",
    )
    minister_fail = Relation(
        relatedEntityId="cit_fail",
        startTime="2020-01-01T00:00:00Z",
        endTime="2030-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id=portfolio_id, name="mocked_protobuf_name")
    ]

    async def fetch_relation_handler(entityId, relation):
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [Relation(relatedEntityId=president_id)]
        if (
            entityId == portfolio_id
            and relation.name == RelationNameEnum.AS_APPOINTED.value
        ):
            return [minister_ok, minister_fail]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler

    async def enrich_side_effect(
        selected_date, person_relation=None, president_id=None, is_president=False
    ):
        if person_relation.relatedEntityId == "cit_fail":
            raise InternalServerError("boom")
        return {
            "id": "cit_ok",
            "name": "OK Minister",
            "isNew": False,
            "isPresident": False,
        }

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
        side_effect=enrich_side_effect,
    ):
        result = await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    assert result["totalCount"] == 1
    assert result["personList"][0]["id"] == "cit_ok"


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_all_enrichment_failures_raises_internal_server_error(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"
    president_id = "pres_123"

    minister_relation = Relation(
        relatedEntityId="cit_1",
        startTime="2020-01-01T00:00:00Z",
        endTime="2030-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id=portfolio_id, name="mocked_protobuf_name")
    ]

    async def fetch_relation_handler(entityId, relation):
        if (
            entityId == EntityIdEnum.GOVERNMENT.value
            and relation.name == RelationNameEnum.AS_PRESIDENT.value
        ):
            return [Relation(relatedEntityId=president_id)]
        if (
            entityId == portfolio_id
            and relation.name == RelationNameEnum.AS_APPOINTED.value
        ):
            return [minister_relation]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler

    with patch(
        "src.services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock,
        side_effect=InternalServerError("boom"),
    ):
        with pytest.raises(InternalServerError):
            await organisation_service.get_persons_by_portfolio(
                portfolio_id=portfolio_id, selected_date=selected_date
            )


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_unexpected_exception_wrapped(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"
    original_error_message = "OpenGIN service error"

    mock_opengin_service.get_entities.side_effect = Exception(original_error_message)

    with pytest.raises(InternalServerError) as exc_info:
        await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )

    root_cause = exc_info.value.__cause__
    assert isinstance(root_cause, Exception)
    assert str(root_cause) == original_error_message


@pytest.mark.asyncio
async def test_get_persons_by_portfolio_bad_request_error_not_wrapped(
    organisation_service, mock_opengin_service
):
    portfolio_id = "min-12"
    selected_date = "2026-04-21"

    mock_opengin_service.get_entities.side_effect = BadRequestError("bad id format")

    with pytest.raises(BadRequestError):
        await organisation_service.get_persons_by_portfolio(
            portfolio_id=portfolio_id, selected_date=selected_date
        )
