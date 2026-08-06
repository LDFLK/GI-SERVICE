from fastapi import APIRouter, Depends, Query, Body, Path
from src.models import Date
from src.services import OpenGINService, OrganisationService
from typing import Sequence

router = APIRouter(prefix="/v1/organisation", tags=["Organisation"])


def get_organisation_service():
    opengin_service = OpenGINService()
    return OrganisationService(opengin_service)


@router.post(
    "/active-portfolio-list",
    summary="Get active portfolio list.",
    description="Returns a list of portfolios under a given president and a given date.",
)
async def active_portfolio_list(
    presidentId: str = Query(..., description="ID of the president"),
    body: Date = Body(...),
    service: OrganisationService = Depends(get_organisation_service),
):
    service_response = await service.active_portfolio_list(presidentId, body.date)
    return service_response


@router.post(
    "/departments-by-portfolio/{portfolio_id}",
    summary="Get active departments for a portfolio.",
    description="Returns a list of departments under a given portfolio and a given date.",
)
async def departments_by_portfolio(
    portfolio_id: str = Path(..., description="ID of the portfolio"),
    body: Date = Body(...),
    service: OrganisationService = Depends(get_organisation_service),
):
    service_response = await service.departments_by_portfolio(
        portfolio_id=portfolio_id, selected_date=body.date
    )
    return service_response


@router.post("/prime-minister")
async def prime_minister(
    body: Date = Body(...),
    service: OrganisationService = Depends(get_organisation_service),
):
    service_response = await service.fetch_prime_minister(selected_date=body.date)
    return service_response


@router.post("/cabinet-flow/{president_id}")
async def cabinet_flow(
    president_id: str = Path(..., description="ID of the president"),
    dates: Sequence[str] = Body(...),
    service: OrganisationService = Depends(get_organisation_service),
):
    service_response = await service.fetch_cabinet_flow(
        president_id=president_id, dates=dates
    )
    return service_response


@router.post(
    "/entity-names",
    summary="Resolve entity IDs to display names.",
    description="Returns a dictionary mapping each entity ID to its decoded display name.",
)
async def entity_names(
    entity_ids: list[str] = Body(
        ...,
        description="List of entity IDs to resolve.",
        example=["2153-12_dep_168", "2153-12_dep_171"],
    ),
    service: OrganisationService = Depends(get_organisation_service),
):
    return await service.resolve_entity_names(entity_ids)


@router.get(
    "/department-history/{department_id}",
    summary="Get department history timeline.",
    description="Returns a timeline of a department including ministry relations and ministers.",
)
async def department_history_timeline(
    department_id: str = Path(..., description="ID of the department"),
    service: OrganisationService = Depends(get_organisation_service),
):
    service_response = await service.department_history_timeline(
        department_id=department_id
    )

    return service_response

@router.post(
    "/department/{department_id}/bodies",
    summary="Get active bodies for a department.",
    description="Returns a list of bodies under a given department and a given date.",
)
async def bodies_by_department(
    department_id: str = Path(..., description="ID of the department"),
    body: Date = Body(...),
    service: OrganisationService = Depends(get_organisation_service),
):
    service_response = await service.bodies_by_department(
        department_id=department_id, selected_date=body.date
    )
    return service_response
