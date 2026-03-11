from unittest.mock import patch

import pytest

from hyperion.infrastructure.httputils import get_proxy_mounts, redact_url


@pytest.mark.parametrize(
    ("url", "replace", "expected"),
    [
        (
            "https://username:password@hostname.tld/path/to/resource",  # pragma: allowlist secret
            "***",
            "https://username:***@hostname.tld/path/to/resource",
        ),
        ("http://u:p@h.tld", "*", "http://u:*@h.tld"),
        ("http://nothing.tld", "***", "http://nothing.tld"),
        (
            "https://u:p@h.tld/path?param1=value&param2=abcd",
            "*REDACTED*",
            "https://u:*REDACTED*@h.tld/path?param1=value&param2=abcd",
        ),
    ],
)
def test_redact_url(url: str, replace: str, expected: str) -> None:
    assert redact_url(url, replace) == expected


class TestGetProxyMounts:
    def test_no_proxy(self) -> None:
        with patch("hyperion.infrastructure.httputils.http_config") as mock_config:
            mock_config.proxy_http = None
            mock_config.proxy_https = None
            mounts = get_proxy_mounts()
        assert mounts == {}

    def test_http_proxy_only(self) -> None:
        with patch("hyperion.infrastructure.httputils.http_config") as mock_config:
            mock_config.proxy_http = "http://proxy:8080"
            mock_config.proxy_https = None
            mounts = get_proxy_mounts()
        assert "http://" in mounts
        assert "https://" in mounts

    def test_https_proxy_only(self) -> None:
        with patch("hyperion.infrastructure.httputils.http_config") as mock_config:
            mock_config.proxy_http = None
            mock_config.proxy_https = "http://proxy:8443"
            mounts = get_proxy_mounts()
        assert "http://" in mounts
        assert "https://" in mounts

    def test_both_proxies(self) -> None:
        with patch("hyperion.infrastructure.httputils.http_config") as mock_config:
            mock_config.proxy_http = "http://proxy:8080"
            mock_config.proxy_https = "http://proxy:8443"
            mounts = get_proxy_mounts()
        assert "http://" in mounts
        assert "https://" in mounts
