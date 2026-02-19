from aiohttp import ClientSession, ClientTimeout, TCPConnector
from typing import Optional
from src.core.config import settings
class HTTPClient:
    "Single HTTP client for the application"

    def __init__(self):
        self._session: Optional[ClientSession] = None

        self.total_seconds = settings.HTTP_TIMEOUT_TOTAL
        self.connect_seconds = settings.HTTP_TIMEOUT_CONNECT
        self.sock_connect_seconds = settings.HTTP_TIMEOUT_SOCK_CONNECT
        self.sock_read_seconds = settings.HTTP_TIMEOUT_SOCK_READ

        self.timeout = ClientTimeout(total=self.total_seconds, connect=self.connect_seconds, sock_connect=self.sock_connect_seconds, sock_read=self.sock_read_seconds)
        
        # Connection pool configuration
        self.pool_size = settings.HTTP_POOL_SIZE
        self.pool_size_per_host = settings.HTTP_POOL_SIZE_PER_HOST
        self.ttl_dns_cache = settings.HTTP_TTL_DNS_CACHE

    async def start(self):
        """Create session on app startup"""
        if self._session is None or self._session.closed:
            # Configure TCPConnector with connection pool limits
            connector = TCPConnector(
                limit=self.pool_size, 
                limit_per_host=self.pool_size_per_host,
                ttl_dns_cache=self.ttl_dns_cache, 
                force_close=False, 
                enable_cleanup_closed=True
            )
            self._session = ClientSession(
                timeout=self.timeout,
                connector=connector
            )

    async def close(self):
        """Close session on app shutdown"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    @property
    def session(self) -> ClientSession:
        if self._session is None or self._session.closed:
            raise RuntimeError("HTTP client not initialized")
        return self._session

# Create a global instance
http_client = HTTPClient()