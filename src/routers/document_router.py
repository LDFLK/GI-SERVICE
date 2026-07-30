from fastapi import APIRouter, Depends
from src.services import OpenGINService, DocumentService

router = APIRouter(prefix="/v1/document", tags=["Document"])


def get_document_service():
    opengin_service = OpenGINService()
    return DocumentService(opengin_service)


@router.get(
    "/data-points",
    summary="Get gazette data points.",
    description="Returns a list of years with the number of gazettes created for each month",
)
async def gazette_data_points(service: DocumentService = Depends(get_document_service)):
    service_response = await service.get_gazette_data_points()
    return service_response
