from pydantic import BaseModel
from datetime import date


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
