from types import TracebackType
from urllib.parse import urlparse

import httpx

from hyperion.config import http_config
from hyperion.logging import get_logger

logger = get_logger("hyperion-http")


def redact_url(url: str, replace: str = "***") -> str:
    """Replace password from {url} (if any) with {replace}.
    If the url contains no password, url is returned unchanged.
    """
    parsed = urlparse(url)
    if parsed.password:
        return parsed._replace(netloc=f"{parsed.username}:{replace}@{parsed.hostname}").geturl()
    return url


class AsyncHTTPClientWrapper:
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None
        self._stacklevel = 0

    async def __aenter__(self) -> httpx.AsyncClient:
        self._stacklevel += 1
        if self._client is None:
            logger.debug("Creating and entering httpx async client.")
            self._client = httpx.AsyncClient(mounts=self._get_proxy_mounts())
            await self._client.__aenter__()
        return self._client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self._stacklevel -= 1
        if self._stacklevel == 0 and self._client is not None:
            logger.info("Closing httpx async client.")
            await self._client.__aexit__(exc_type, exc_value, traceback)
            self._client = None

    @staticmethod
    def _get_proxy_mounts() -> dict[str, httpx.AsyncHTTPTransport]:
        proxy_mounts: dict[str, httpx.AsyncHTTPTransport] = {}
        if http_config.proxy_http:
            redacted_url = redact_url(http_config.proxy_http)
            logger.info("Configuring HTTP proxy for http://", proxy_url=redacted_url)
            proxy_mounts["http://"] = httpx.AsyncHTTPTransport(proxy=http_config.proxy_http)
        if http_config.proxy_https:
            redacted_url = redact_url(http_config.proxy_https)
            logger.info("Configuring HTTP proxy for https://", proxy_url=redacted_url)
            proxy_mounts["https://"] = httpx.AsyncHTTPTransport(proxy=http_config.proxy_https)
        return proxy_mounts
