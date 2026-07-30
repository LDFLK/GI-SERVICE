import pytest
from pydantic import ValidationError

from src.models import AttributeFilterRecord, AttributeFilterRecords


def test_attribute_filter_record_defaults_operator_to_eq():
    record = AttributeFilterRecord(field_name="status", value="active")

    assert record.operator == "eq"


def test_attribute_filter_records_serializes_multiple_filters():
    filters = AttributeFilterRecords(
        records=[
            AttributeFilterRecord(
                field_name="status", operator="neq", value="inactive"
            ),
            AttributeFilterRecord(field_name="name", operator="contains", value="john"),
        ]
    )

    assert filters.model_dump(mode="json") == {
        "records": [
            {"field_name": "status", "operator": "neq", "value": "inactive"},
            {"field_name": "name", "operator": "contains", "value": "john"},
        ]
    }


@pytest.mark.parametrize(
    "operator",
    ["eq", "neq", "gt", "lt", "gte", "lte", "contains", "notcontains"],
)
def test_attribute_filter_record_accepts_supported_operators(operator):
    record = AttributeFilterRecord(field_name="amount", operator=operator, value="10")

    assert record.operator == operator


def test_attribute_filter_record_requires_field_name_and_value():
    with pytest.raises(ValidationError):
        AttributeFilterRecord(value="active")

    with pytest.raises(ValidationError):
        AttributeFilterRecord(field_name="status")


@pytest.mark.parametrize("operator", ["invalid", "equals", "EQ", "like"])
def test_attribute_filter_record_rejects_invalid_operators(operator):
    with pytest.raises(ValidationError) as exc_info:
        AttributeFilterRecord(field_name="status", operator=operator, value="active")

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("operator",)
    assert errors[0]["type"] == "literal_error"
