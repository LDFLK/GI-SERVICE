import asyncio
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from src.core.config import settings

logger = logging.getLogger(__name__)

class ThrottlingMiddleware(BaseHTTPMiddleware):
    """
    Throttling middleware that queues excess requests instead of rejecting them.
    
    Uses an asyncio.Semaphore to limit the number of concurrently processed requests.
    When the limit is reached, new requests WAIT in a queue instead of getting 503'd.
    
    If a request waits longer than `timeout` seconds, it gets a 429 Too Many Requests
    response (which is the correct HTTP status for throttling).
    """
    
    def __init__(self, app):
        super().__init__(app)
        self.timeout = settings.THROTTLING_TIMEOUT
        self.semaphore = asyncio.Semaphore(settings.THROTTLING_MAX_CONCURRENT)
    
    async def dispatch(self, request: Request, call_next):
        try:
            # Wait for a slot to open up (with timeout)
            await asyncio.wait_for(self.semaphore.acquire(), timeout=self.timeout)
        except asyncio.TimeoutError:
            # Only reject if the request has been waiting too long
            logger.warning(f"Request throttled after {self.timeout}s wait: {request.method} {request.url.path}")
            return JSONResponse(
                status_code=429,
                content={"detail": "Server is busy. Please try again shortly."}
            )
        
        try:
            response = await call_next(request)
            return response
        finally:
            self.semaphore.release()
