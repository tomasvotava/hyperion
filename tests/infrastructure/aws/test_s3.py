from io import BytesIO
from pathlib import Path

import boto3
import botocore.exceptions
import pytest

from hyperion.asyncutils import AsyncTaskQueue, aiter_any
from hyperion.infrastructure.aws import S3Client

TEST_BUCKET = "HYPERION_S3_TEST"
MULTI_TEST_FILES_COUNT = 100


@pytest.fixture(scope="module", autouse=True)
def _bucket(_moto_server: None) -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=TEST_BUCKET)


def test_s3_download_nonexisting() -> None:
    client = S3Client()
    with pytest.raises(botocore.exceptions.ClientError, match=r"An error occurred .NoSuchKey."):
        client.get_object_attributes(TEST_BUCKET, "test.txt")


def test_s3_upload() -> None:
    client = S3Client()
    file = BytesIO(b"test")
    client.upload(file, TEST_BUCKET, "test.txt")
    attributes = client.get_object_attributes(TEST_BUCKET, "test.txt")
    assert attributes.object_size == 4
    assert client.download_as_string(TEST_BUCKET, "test.txt") == "test"


async def test_s3_upload_async() -> None:
    client = S3Client()
    file = BytesIO(b"test")
    await client.upload_async(file, TEST_BUCKET, "test_async.txt")
    assert client.get_object_attributes(TEST_BUCKET, "test_async.txt").object_size == 4
    assert client.download_as_string(TEST_BUCKET, "test_async.txt") == "test"


def test_s3_upload_path(tmpdir: Path) -> None:
    test_file = Path(tmpdir / "test_file.txt")
    test_file.write_text("test file", encoding="utf-8")
    client = S3Client()
    client.upload(test_file, TEST_BUCKET, "test_file.txt")
    assert client.get_object_attributes(TEST_BUCKET, "test_file.txt").object_size == 9
    assert client.download_as_string(TEST_BUCKET, "test_file.txt") == "test file"


async def test_s3_upload_path_async(tmpdir: Path) -> None:
    test_file = Path(tmpdir / "test_file_async.txt")
    test_file.write_text("test file async", encoding="utf-8")
    client = S3Client()
    await client.upload_async(test_file, TEST_BUCKET, "test_file_async.txt")
    assert client.get_object_attributes(TEST_BUCKET, "test_file_async.txt").object_size == 15
    assert client.download_as_string(TEST_BUCKET, "test_file_async.txt") == "test file async"


def test_s3_download_path(tmpdir: Path) -> None:
    content = BytesIO(b"test file download")

    client = S3Client()
    client.upload(content, TEST_BUCKET, "test_file_download.txt")

    test_file = Path(tmpdir / "test_file_download.txt")
    assert not test_file.exists()

    client.download(TEST_BUCKET, "test_file_download.txt", test_file)

    assert test_file.exists()
    assert test_file.read_text(encoding="utf-8") == "test file download"


def test_s3_download_filelike() -> None:
    content = BytesIO(b"test file download filelike")

    client = S3Client()
    client.upload(content, TEST_BUCKET, "test_file_download_filelike.txt")

    with BytesIO() as file:
        client.download(TEST_BUCKET, "test_file_download_filelike.txt", file)
        file.seek(0)
        assert file.read() == b"test file download filelike"


async def test_s3_multiupload() -> None:
    client = S3Client()
    async with AsyncTaskQueue[None]() as queue:
        async for file_id in aiter_any(range(MULTI_TEST_FILES_COUNT)):
            file = BytesIO(f"content of file {file_id}".encode())
            await queue.add_task(client.upload_async(file, TEST_BUCKET, f"multitest/file-{file_id}.txt"))
    for file_id in range(MULTI_TEST_FILES_COUNT):
        assert client.download_as_string(TEST_BUCKET, f"multitest/file-{file_id}.txt") == f"content of file {file_id}"


def test_s3_iter_objects() -> None:
    client = S3Client()
    for fileno in range(100):
        with BytesIO(b"test") as fake_file:
            client.upload(fake_file, TEST_BUCKET, f"listing-prefix/{fileno}.file")
    filelist = list(client.iter_objects(TEST_BUCKET, "listing-prefix/"))
    assert len(filelist) == 100
    assert filelist[0].startswith("listing-prefix/")
    assert filelist[0].endswith(".file")
