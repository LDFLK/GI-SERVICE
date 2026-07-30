import pytest
from unittest.mock import MagicMock, patch
from src.exception import InternalServerError, NotFoundError


@pytest.mark.asyncio
async def test_get_gazette_data_points_success(document_service, mock_opengin_service):
    org_gazette_1 = MagicMock()
    org_gazette_1.id = "idx-1"
    org_gazette_1.created = "2023-01-15T10:00:00Z"
    org_gazette_2 = MagicMock()
    org_gazette_2.id = "idx-2"
    org_gazette_2.created = "2023-01-20T12:00:00Z"
    org_gazette_3 = MagicMock()
    org_gazette_3.id = "idx-3"
    org_gazette_3.created = "2024-05-05T08:00:00Z"

    person_gazette_1 = MagicMock()
    person_gazette_1.id = "idx-p1"
    person_gazette_1.created = "2023-02-10T14:00:00Z"
    person_gazette_2 = MagicMock()
    person_gazette_2.id = "idx-p2"
    person_gazette_2.created = "2024-05-25T16:00:00Z"

    mock_opengin_service.get_entities.side_effect = [
        [
            org_gazette_1,
            org_gazette_2,
            org_gazette_3,
        ],
        [person_gazette_1, person_gazette_2],
    ]

    result = await document_service.get_gazette_data_points()

    assert "data" in result
    assert len(result["data"]) == 2
    assert result["data"][0]["year"] == 2023
    assert len(result["data"][0]["values"]) == 12
    assert result["data"][0]["values"][0] == 2
    assert result["data"][0]["values"][1] == 1
    assert all(
        count == 0
        for i, count in enumerate(result["data"][0]["values"])
        if i not in [0, 1]
    )

    assert result["data"][1]["year"] == 2024
    assert len(result["data"][1]["values"]) == 12
    assert result["data"][1]["values"][4] == 2
    assert all(
        count == 0 for i, count in enumerate(result["data"][1]["values"]) if i != 4
    )

    # Verify sorting
    assert [y["year"] for y in result["data"]] == [2023, 2024]


@pytest.mark.asyncio
async def test_get_gazette_data_points_empty(document_service, mock_opengin_service):
    mock_opengin_service.get_entities.side_effect = NotFoundError("not found")

    result = await document_service.get_gazette_data_points()

    assert result == {"data": []}


@pytest.mark.asyncio
async def test_get_gazette_data_points_internal_server_error(
    document_service, mock_opengin_service
):
    with patch(
        "src.services.document_service.asyncio.gather", side_effect=Exception("failure")
    ):
        with pytest.raises(InternalServerError):
            await document_service.get_gazette_data_points()


@pytest.mark.asyncio
async def test_get_gazette_data_points_malformed_object(
    document_service, mock_opengin_service
):
    # good gazette
    good_gazette = MagicMock()
    good_gazette.id = "good-idx"
    good_gazette.created = "2023-01-15T10:00:00Z"

    # malformed gazette
    bad_gazette = MagicMock(spec=[])  # No attributes

    mock_opengin_service.get_entities.side_effect = [
        [good_gazette],
        [bad_gazette],
    ]

    # Should not raise InternalServerError
    result = await document_service.get_gazette_data_points()

    assert "data" in result
    assert len(result["data"]) == 1
    assert result["data"][0]["year"] == 2023
    assert len(result["data"][0]["values"]) == 12
    assert result["data"][0]["values"][0] == 1


@pytest.mark.asyncio
async def test_get_gazette_data_points_robustness(
    document_service, mock_opengin_service
):
    # 1. Valid gazette
    v1 = MagicMock()
    v1.id = "valid-1"
    v1.created = "2023-01-15T10:00:00Z"

    # 2. Missing created attribute (AttributeError)
    v2 = MagicMock(spec=["id"])
    v2.id = "no-created-attr"

    # 3. None created value
    v3 = MagicMock()
    v3.id = "none-created"
    v3.created = None

    # 4. Invalid date string (Util returns None or strptime fails)
    v4 = MagicMock()
    v4.id = "invalid-date"
    v4.created = "not-a-date"

    # 5. Missing ID attribute (AttributeError)
    v5 = MagicMock(spec=["created"])
    v5.created = "2023-02-15T10:00:00Z"

    # 6. Another valid gazette to ensure we continue processing
    v6 = MagicMock()
    v6.id = "valid-2"
    v6.created = "2024-05-15T10:00:00Z"

    mock_opengin_service.get_entities.side_effect = [
        [v1, v2, v3, v4, v5],
        [v6],
    ]

    result = await document_service.get_gazette_data_points()

    # Result should contain 2023 (from v1) and 2024 (from v6)
    assert "data" in result
    assert len(result["data"]) == 2

    assert result["data"][0]["year"] == 2023
    assert result["data"][0]["values"][0] == 1  # Jan

    assert result["data"][1]["year"] == 2024
    assert result["data"][1]["values"][4] == 1  # May
