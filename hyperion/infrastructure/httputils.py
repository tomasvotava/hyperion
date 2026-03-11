from urllib.parse import urlparse

import httpx

from hyperion.config import http_config
from hyperion.log import get_logger

logger = get_logger("hyperion-http")


def redact_url(url: str, replace: str = "***") -> str:
    """Replace password from {url} (if any) with {replace}.
    If the url contains no password, url is returned unchanged.
    """
    parsed = urlparse(url)
    if parsed.password:
        return parsed._replace(netloc=f"{parsed.username}:{replace}@{parsed.hostname}").geturl()
    return url


def get_proxy_mounts() -> dict[str, httpx.AsyncHTTPTransport]:
    """Build httpx proxy mounts from HYPERION_HTTP_PROXY_HTTP / HYPERION_HTTP_PROXY_HTTPS env vars.

    If only one of the two is set, it is used for both http:// and https://.
    """
    proxy_mounts: dict[str, httpx.AsyncHTTPTransport] = {}
    if http_config.proxy_http:
        redacted_url = redact_url(http_config.proxy_http)
        logger.info("Configuring HTTP proxy for http://", proxy_url=redacted_url)
        proxy_mounts["http://"] = httpx.AsyncHTTPTransport(proxy=http_config.proxy_http)
        if not http_config.proxy_https:
            logger.info("Configuring HTTP proxy for https://", proxy_url=redacted_url)
            proxy_mounts["https://"] = httpx.AsyncHTTPTransport(proxy=http_config.proxy_http)
    if http_config.proxy_https:
        redacted_url = redact_url(http_config.proxy_https)
        logger.info("Configuring HTTP proxy for https://", proxy_url=redacted_url)
        proxy_mounts["https://"] = httpx.AsyncHTTPTransport(proxy=http_config.proxy_https)
        if not http_config.proxy_http:
            logger.info("Configuring HTTP proxy for http://", proxy_url=redacted_url)
            proxy_mounts["http://"] = httpx.AsyncHTTPTransport(proxy=http_config.proxy_https)
    return proxy_mounts
