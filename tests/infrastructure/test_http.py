import pytest

from hyperion.infrastructure.http import redact_url


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
