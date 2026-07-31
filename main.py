from fastapi import FastAPI
from src.routers import (
    organisation_router,
    data_router,
    search_router,
    person_router,
    document_router,
)
from src.core import settings
from fastapi.middleware.cors import CORSMiddleware
from src.middleware import ThrottlingMiddleware
from src.utils import http_client
from src.cache import cache
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Same lifecycle as HTTP: open shared resources once per worker, close on shutdown
    await http_client.start()
    await cache.connect()
    yield
    await cache.close()
    await http_client.close()


app = FastAPI(
    title="GI - Service",
    description="API Adapter to the OpenGIn (Open General Information Network)",
    version="1.0.0",
    lifespan=lifespan,
)

allowed_origins = [
    origin.strip() for origin in settings.ALLOWED_ORIGINS.split(",") if origin.strip()
]
if not allowed_origins:
    raise ValueError("ALLOWED_ORIGINS is not configured")


app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ThrottlingMiddleware)

app.include_router(organisation_router)
app.include_router(data_router)
app.include_router(search_router)
app.include_router(person_router)
app.include_router(document_router)
