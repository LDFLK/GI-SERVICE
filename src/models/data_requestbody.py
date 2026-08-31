from pydantic import BaseModel, Field, ConfigDict
from typing import List, Any


class DataCatalogRequest(BaseModel):
    categoryIds: list[str] = Field(None, description="List of category IDs")


class DatasetYearsRequest(BaseModel):
    datasetIds: list[str] = Field(None, description="List of dataset IDs")


class CategoryItem(BaseModel):
    """One entry in categories — a decoded name mapped to all entity IDs sharing that name."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Decoded category name", examples=["Health"])
    categoryIds: List[str] = Field(
        ...,
        description="Entity IDs sharing this category name",
        examples=[["cat_1", "cat_2"]],
    )


class DatasetItem(BaseModel):
    """One entry in datasets — a decoded name mapped to all entity IDs sharing that name."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Decoded dataset name", examples=["Population"])
    datasetIds: List[str] = Field(
        ...,
        description="Entity IDs sharing this dataset name",
        examples=[["ds_1", "ds_2"]],
    )


class DataCatalogResponse(BaseModel):
    """Flat response — no envelope."""

    model_config = ConfigDict(extra="forbid")

    categories: List[CategoryItem] = Field(default_factory=list)
    datasets: List[DatasetItem] = Field(default_factory=list)


class DatasetYearEntry(BaseModel):
    """One available year for a dataset group."""

    model_config = ConfigDict(extra="forbid")

    datasetId: str = Field(
        ..., description="Dataset entity ID for this year", examples=["ds_2022"]
    )
    year: str = Field(
        ...,
        description="Year extracted from the entity's created date, 'Unknown' if unavailable",
        examples=["2022"],
    )


class DatasetAvailableYearsResponse(BaseModel):
    """Flat response — no envelope."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description="Common dataset name, title-cased with year removed",
        examples=["Population"],
    )
    years: List[DatasetYearEntry] = Field(default_factory=list)


class DatasetRootItem(BaseModel):
    """Root department/state minister/cabinet minister entity for a dataset."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Root entity ID", examples=["dep-xx"])
    name: str = Field(
        ..., description="Decoded root entity name", examples=["Ministry of Finance"]
    )
    type: str = Field(
        ...,
        description="Root entity kind.minor value, e.g. Department, StateMinister, CabinetMinister",
        examples=["Department"],
    )


class DatasetNotFoundResponse(BaseModel):
    """Returned instead of DatasetRootItem when no root entity is found."""

    model_config = ConfigDict(extra="forbid")

    detail: str = Field(..., examples=["Dataset not found"])


class TabularData(BaseModel):
    """Data structure for type='tabular'."""

    model_config = ConfigDict(extra="forbid")

    columns: List[str] = Field(
        ..., description="Column names", examples=[["id", "name", "year"]]
    )
    rows: List[List[Any]] = Field(
        ...,
        description="Row data, each row aligned to columns",
        examples=[[["1", "Test", "2020"]]],
    )


class DataAttributesResponse(BaseModel):
    """Response for a dataset's formatted attributes. `data` shape depends on `type`."""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(
        ..., description="tabular | document | graph", examples=["tabular"]
    )
    data: TabularData | dict = Field(
        ...,
        description="Type-specific payload; strictly validated only when type='tabular'",
    )


class DataAttributesNotFoundResponse(BaseModel):
    """Returned instead of DataAttributesResponse when the dataset or its relations aren't found."""

    model_config = ConfigDict(extra="forbid")

    message: str = Field(..., examples=["Dataset or its relations not found"])


class EntityKind(BaseModel):
    """Major/minor kind classification, reused across dataset and category entries."""

    model_config = ConfigDict(extra="forbid")

    major: str = Field(..., description="Kind major value", examples=["CATEGORY"])
    minor: str = Field(..., description="Kind minor value", examples=["Department"])


class DatasetInfo(BaseModel):
    """Basic dataset identity info."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Dataset ID", examples=["ds_xx"])
    name: str = Field(..., description="Decoded dataset name", examples=["Population"])
    kind: EntityKind


class CategoryHierarchyItem(BaseModel):
    """One category in the hierarchy, from immediate parent up to (and including) the root."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Category ID", examples=["cat_xx"])
    name: str = Field(..., description="Decoded category name", examples=["Health"])
    kind: EntityKind


class DatasetCategoriesResponse(BaseModel):
    """Flat response — no envelope."""

    model_config = ConfigDict(extra="forbid")

    dataset: DatasetInfo
    categories: List[CategoryHierarchyItem] = Field(default_factory=list)
