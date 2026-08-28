from pydantic import BaseModel, Field, ConfigDict
from datetime import date
from typing import List


class PersonSource(BaseModel):
    """
    Person source schema
    """

    name: str
    political_party: str | None = None
    date_of_birth: date | None = None
    religion: str | None = None
    profession: str | None = None
    email: str | None = None
    phone_number: str | None = None
    education_qualifications: str | None = None
    professional_qualifications: str | None = None
    image_url: str | None = None


class PersonResponse(PersonSource):
    """
    Person response schema inherited from the PersonSource
    """

    age: int | None = None


class MinistryHistoryItem(BaseModel):
    """One ministry appointment in a person's history."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Ministry ID", examples=["min-xx"])
    name: str = Field(
        ..., description="Ministry name", examples=["Ministry of Finance"]
    )
    term: str = Field(
        ..., description="Formatted term string", examples=["2018 - 2020"]
    )
    is_president: bool = Field(
        ...,
        description="True if this person was president at some point during this term",
    )


class PersonHistoryResponse(BaseModel):
    """Flat response — no envelope, matches actual return shape (not the docstring's body wrapper)."""

    model_config = ConfigDict(extra="forbid")

    ministry_history: List[MinistryHistoryItem] = Field(default_factory=list)
    ministries_worked_at: int = Field(..., ge=0, examples=[3])
    worked_as_president: int = Field(..., ge=0, examples=[1])
