"""AWS helpers and methods."""

import datetime
import logging
from contextlib import ExitStack
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import IO, BinaryIO, cast

import aioboto3
import boto3
import botocore.exceptions

from hyperion.logging import get_logger

logger = get_logger("aws")

logging.getLogger("botocore.endpoint").setLevel("WARNING")


class S3StorageClass(str, Enum):
    STANDARD = "STANDARD"
    REDUCED_REDUNDANCY = "REDUCED_REDUNDANCY"
    STANDARD_IA = "STANDARD_IA"
    ONEZONE_IA = "ONEZONE_IA"
    INTELLIGENT_TIERING = "INTELLIGENT_TIERING"
    GLACIER = "GLACIER"
    DEEP_ARCHIVE = "DEEP_ARCHIVE"
    OUTPOSTS = "OUTPOSTS"
    GLACIER_IR = "GLACIER_IR"
    SNOW = "SNOW"
    EXPRESS_ONEZONE = "EXPRESS_ONEZONE"


@dataclass
class S3ObjectAttributes:
    last_modified: datetime.datetime
    etag: str
    storage_class: S3StorageClass
    object_size: int


class S3Client:
    def __init__(self) -> None:
        self._client = boto3.client("s3")
        self._aio_session = aioboto3.Session()

    async def upload_async(self, file: str | Path | BinaryIO | IO[bytes], bucket: str, name: str) -> None:
        file_context = ExitStack()
        if isinstance(file, str | Path):
            path = Path(file)
            logger.debug("Uploading from path.", path=path.as_posix(), bucket=bucket, name=name)
            file = file_context.enter_context(path.open("rb"))
        with file_context:
            async with self._aio_session.client("s3") as s3:
                await s3.upload_fileobj(file, bucket, name)

    def upload(self, file: str | Path | BinaryIO | IO[bytes], bucket: str, name: str) -> None:
        if isinstance(file, str | Path):
            file = Path(file)
            logger.debug("Uploading from path.", path=file.as_posix(), bucket=bucket, name=name)
            try:
                self._client.upload_file(file, bucket, name)
            except botocore.exceptions.ClientError:
                logger.error("Error when uploading file to S3.", file=file.as_posix(), bucket=bucket, name=name)
                raise
            return
        logger.debug("Uploading from an open file stream.", bucket=bucket, name=name)
        try:
            self._client.upload_fileobj(file, bucket, name)
        except botocore.exceptions.ClientError:
            logger.error("Error when uploading file to S3.", bucket=bucket, name=name)
            raise

    def get_object_attributes(self, bucket: str, name: str) -> S3ObjectAttributes:
        response = self._client.get_object_attributes(
            Bucket=bucket,
            Key=name,
            ObjectAttributes=[
                "ETag",
                "Checksum",
                "ObjectParts",
                "StorageClass",
                "ObjectSize",
            ],
        )
        try:
            return S3ObjectAttributes(
                last_modified=response["LastModified"],
                etag=response["ETag"],
                storage_class=S3StorageClass(response["StorageClass"]),
                object_size=int(response["ObjectSize"]),
            )
        except Exception:
            logger.error(
                "Failed to get object attributes, the response is probably invalid.",
                bucket=bucket,
                object_name=name,
                response=response,
            )
            raise

    def download(self, bucket: str, name: str, file: str | Path | BinaryIO | IO[bytes]) -> None:
        if isinstance(file, str | Path):
            file = Path(file)
            logger.debug("Downloading into a path.", path=file.as_posix(), bucket=bucket, name=name)
            try:
                self._client.download_file(bucket, name, file)
            except botocore.exceptions.ClientError:
                logger.error("Error when downloading file from S3.", bucket=bucket, name=name, path=file.as_posix())
                raise
            return
        logger.debug("Downloading into a file stream.", bucket=bucket, name=name)
        try:
            self._client.download_fileobj(bucket, name, file)
        except botocore.exceptions.ClientError:
            logger.error("Error when downloading file from S3.", bucket=bucket, name=name)
            raise

    def download_as_string(self, bucket: str, name: str) -> str:
        logger.debug("Downloading object as a string.", bucket=bucket, name=name)
        try:
            return cast(str, self._client.get_object(Bucket=bucket, Key=name)["Body"].read().decode("utf-8"))
        except botocore.exceptions.ClientError:
            logger.error("Error when downloading file from S3.", bucket=bucket, name=name)
            raise
