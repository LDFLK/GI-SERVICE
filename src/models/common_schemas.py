from pydantic import BaseModel, Field, field_validator
from datetime import date as _date


class Date(BaseModel):
    """
    Request body carrying the as-of date for point-in-time lookups.
    """

    date: str = Field(
        ...,
        description="Date to query persons as-of.",
        examples=["2026-04-21"],
    )

    @field_validator("date")
    @classmethod
    def _validate_iso_date(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("date must not be empty")
        try:
            _date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("date must be in ISO-8601 format (YYYY-MM-DD)") from exc
        return value
