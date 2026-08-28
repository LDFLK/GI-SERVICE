from pydantic import BaseModel, Field, ConfigDict
from typing import List


class DataCatalogRequest(BaseModel):
    categoryIds: list[str] = Field(None, description="List of category IDs")


class DatasetYearsRequest(BaseModel):
    datasetIds: list[str] = Field(None, description="List of dataset IDs")

class CategoryItem(BaseModel):
    """One entry in categories — a decoded name mapped to all entity IDs sharing that name."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Decoded category name", examples=["Health"])
    categoryIds: List[str] = Field(
        ..., description="Entity IDs sharing this category name", examples=[["cat_1", "cat_2"]]
    )


class DatasetItem(BaseModel):
    """One entry in datasets — a decoded name mapped to all entity IDs sharing that name."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Decoded dataset name", examples=["Population"])
    datasetIds: List[str] = Field(
        ..., description="Entity IDs sharing this dataset name", examples=[["ds_1", "ds_2"]]
    )


class DataCatalogResponse(BaseModel):
    """Flat response — no envelope."""

    model_config = ConfigDict(extra="forbid")

    categories: List[CategoryItem] = Field(default_factory=list)
    datasets: List[DatasetItem] = Field(default_factory=list)