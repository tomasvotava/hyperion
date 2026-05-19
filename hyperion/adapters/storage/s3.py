"""S3 :class:`StoragePort` adapter (requires boto3 / aioboto3 -- future ``[aws]``).

Self-contained: it does not import the legacy ``hyperion.infrastructure.aws``
``S3Client`` (kept untouched for Catalog until S5/S6). The bucket -- and an
optional key prefix -- are fixed at construction, so the port surface stays
key-only. ``iter_keys`` strips the storage-side prefix back off so this adapter
shares one key namespace with the memory/filesystem adapters.
"""

from __future__ import annotations

from asyncio import Semaphore
from collections.abc import AsyncIterator, Iterator
from contextlib import ExitStack, asynccontextmanager, contextmanager
from io import BytesIO
from typing import IO, TYPE_CHECKING, cast

import aioboto3
import boto3
import botocore.exceptions

from hyperion.config import storage_config
from hyperion.log import get_logger
from hyperion.ports.storage import ObjectAttributes, ObjectNotFoundError

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import PaginatorConfigTypeDef

logger = get_logger("adapters.storage.s3")

_NOT_FOUND_CODES = {"NoSuchKey", "404"}


def _is_not_found(error: botocore.exceptions.ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in _NOT_FOUND_CODES


class S3Storage:
    """A :class:`StoragePort` backed by a single S3 bucket (optionally prefixed)."""

    _storage_semaphore: Semaphore | None = None

    @classmethod
    @asynccontextmanager
    async def _semaphore(cls) -> AsyncIterator[None]:
        """Bound concurrent async uploads to ``storage_config.max_concurrency``."""
        if cls._storage_semaphore is None:
            logger.debug("Initializing storage semaphore.", max_concurrency=storage_config.max_concurrency)
            cls._storage_semaphore = Semaphore(storage_config.max_concurrency)
        async with cls._storage_semaphore:
            yield

    def __init__(self, bucket: str, prefix: str = "") -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._client = boto3.client("s3")
        self._aio_session = aioboto3.Session()

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def put(self, key: str, data: bytes | IO[bytes]) -> None:
        fileobj: IO[bytes] = BytesIO(data) if isinstance(data, bytes) else data
        logger.debug("Uploading object to S3.", bucket=self._bucket, key=key)
        try:
            self._client.upload_fileobj(fileobj, self._bucket, self._full_key(key))
        except botocore.exceptions.ClientError:
            logger.error("Error when uploading object to S3.", bucket=self._bucket, key=key)
            raise

    async def put_async(self, key: str, data: bytes | IO[bytes]) -> None:
        with ExitStack() as stack:
            fileobj: IO[bytes] = BytesIO(data) if isinstance(data, bytes) else data
            stack.callback(fileobj.close)
            async with self._semaphore(), self._aio_session.client("s3") as s3:
                try:
                    await s3.upload_fileobj(fileobj, self._bucket, self._full_key(key))
                except botocore.exceptions.ClientError:
                    logger.error("Error when uploading object to S3.", bucket=self._bucket, key=key)
                    raise

    def get(self, key: str) -> bytes:
        try:
            return self._client.get_object(Bucket=self._bucket, Key=self._full_key(key))["Body"].read()
        except botocore.exceptions.ClientError as error:
            if _is_not_found(error):
                raise ObjectNotFoundError(key) from error
            raise

    @contextmanager
    def open(self, key: str) -> Iterator[IO[bytes]]:
        try:
            body = self._client.get_object(Bucket=self._bucket, Key=self._full_key(key))["Body"]
        except botocore.exceptions.ClientError as error:
            if _is_not_found(error):
                raise ObjectNotFoundError(key) from error
            raise
        try:
            yield cast("IO[bytes]", body)
        finally:
            body.close()

    def iter_keys(self, prefix: str) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        pagination_config: PaginatorConfigTypeDef = {}
        full_prefix = self._full_key(prefix)
        logger.debug("Listing S3 bucket contents.", bucket=self._bucket, prefix=full_prefix)
        for response in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix, PaginationConfig=pagination_config):
            if "Contents" not in response:
                continue
            for s3_object in response["Contents"]:
                yield s3_object["Key"][len(self._prefix) :]

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._full_key(key))
        except botocore.exceptions.ClientError as error:
            if _is_not_found(error):
                return False
            raise
        return True

    def delete(self, key: str) -> None:
        # S3 delete_object is idempotent -- deleting a missing key is a no-op.
        self._client.delete_object(Bucket=self._bucket, Key=self._full_key(key))

    def get_attributes(self, key: str) -> ObjectAttributes:
        """Return metadata for *key* via ``head_object``.

        .. note::
            For multipart uploads, ``head_object`` returns a composite ETag
            (``"<md5>-<part_count>"``) that is **not** a plain content MD5
            and differs from the value ``get_object_attributes`` would return.
            :meth:`put` / :meth:`put_async` go through boto3's managed
            ``upload_fileobj``, which switches to a multipart upload for
            payloads above ``TransferConfig.multipart_threshold`` (8 MiB by
            default), so larger objects do get a composite ETag here. Callers
            must not treat ``etag`` as a portable content MD5 across backends
            or upload strategies.
        """
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=self._full_key(key))
        except botocore.exceptions.ClientError as error:
            if _is_not_found(error):
                raise ObjectNotFoundError(key) from error
            raise
        return ObjectAttributes(
            etag=response["ETag"].strip('"'),
            size=int(response["ContentLength"]),
            last_modified=response["LastModified"],
        )
