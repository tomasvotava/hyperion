import shutil
import signal
import socket
import subprocess
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest
from env_proxy.env_proxy import apply_env


def _get_free_tcp_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("localhost", 0))
        return cast(int, sock.getsockname()[1])


@pytest.fixture(scope="session")
def data_dir() -> Path:
    data_path = (Path(__file__).parent / "data").resolve()
    if not data_path.exists():
        raise FileNotFoundError(f"Tests data directory not found in {data_path}.")
    return data_path


@pytest.fixture(scope="session")
def _moto_server() -> Iterator[None]:
    port = _get_free_tcp_port()
    moto_server_path = shutil.which("moto_server")
    if not moto_server_path:
        raise RuntimeError("moto_server was not found in the PATH.")
    with (
        subprocess.Popen(("moto_server", "-H", "localhost", "-p", str(port)), executable=moto_server_path) as pipe,  # noqa: S603
        apply_env(
            AWS_ACCESS_KEY_ID="testing",
            AWS_SECRET_ACCESS_KEY="testing",  # noqa: S106 # pragma: allowlist secret
            AWS_SECURITY_TOKEN="testing",  # noqa: S106 # pragma: allowlist secret
            AWS_SESSION_TOKEN="testing",  # noqa: S106 # pragma: allowlist secret
            AWS_DEFAULT_REGION="us-east-1",
            AWS_ENDPOINT_URL=f"http://localhost:{port}",
        ),
    ):
        try:
            yield
        finally:
            pipe.send_signal(signal.SIGTERM)
            try:
                pipe.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                pipe.kill()
