"""DynamoDB-backed :class:`Cache` adapter (requires boto3 -- ``[aws]``)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast

import boto3

from hyperion.log import get_logger
from hyperion.ports.cache import DEFAULT_TTL_SECONDS, Cache, CachingError

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.type_defs import ScanInputTableScanTypeDef

DYNAMODB_MAX_LENGTH = 65535

logger = get_logger("cache")


class DynamoDBCache(Cache):
    """A DynamoDB cache for our shenanigans."""

    TTL_ATTRIBUTE_NAME = "time_to_live"

    def __init__(
        self, prefix: str, hash_keys: bool = True, default_ttl: int = DEFAULT_TTL_SECONDS, table_name: str | None = None
    ):
        super().__init__(prefix, hash_keys, default_ttl)
        self.client = boto3.resource("dynamodb")
        if table_name is None:
            raise ValueError("No table_name was provided for DynamoDBCache.")
        self.table_name = table_name
        self.table = self.client.Table(table_name)

    def _get(self, key: str) -> str | None:
        if (cached := self._get_bytes(key)) is None:
            return None
        return cached.decode("utf-8")

    def _set(self, key: str, value: str) -> None:
        return self._set_bytes(key, value.encode("utf-8"))

    def _set_bytes(self, key: str, value: bytes) -> None:
        expiration_time = int(time.time()) + self.default_ttl
        cache_key = self._key(key)
        compressed_value = self._compress_bytes(value)
        if len(compressed_value) > DYNAMODB_MAX_LENGTH:
            logger.warning(
                "Value is too long to store in DynamoDB.",
                original_key=cache_key,
                cache_key=cache_key,
                length=len(compressed_value),
            )
            raise CachingError(f"Value is too long to store in DynamoDB: {len(compressed_value)}")
        self.table.put_item(
            Item={"key": cache_key, "value": compressed_value, self.TTL_ATTRIBUTE_NAME: expiration_time}
        )

    def _get_bytes(self, key: str) -> bytes | None:
        cache_key = self._key(key)
        response = self.table.get_item(Key={"key": cache_key})
        item = response.get("Item")
        if item:
            return self._decompress_bytes(bytes(cast(Any, item["value"])))
        return None

    def _delete(self, key: str) -> None:
        key = self._key(key)
        self.table.delete_item(Key={"key": key})

    def _clear(self) -> None:
        """
        Deletes all items in the table.

        Warning: DynamoDB doesn't have a built-in clear mechanism, so we scan and delete all
        items manually. Pagination is handled via ``LastEvaluatedKey`` so tables larger than
        the ~1 MB scan page limit are cleared completely.
        """
        logger.info("Clearing cache.", cache_table=self.table_name)
        scan_kwargs: ScanInputTableScanTypeDef = {
            "ProjectionExpression": "#k",
            "ExpressionAttributeNames": {"#k": "key"},
        }
        with self.table.batch_writer() as batch:
            while True:
                response = self.table.scan(**scan_kwargs)
                for item in response.get("Items", []):
                    batch.delete_item(Key={"key": item["key"]})
                if "LastEvaluatedKey" not in response:
                    break
                scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    def _hit(self, key: str) -> bool:
        cache_key = self._key(key)
        item = self.table.get_item(Key={"key": cache_key}, ProjectionExpression=self.TTL_ATTRIBUTE_NAME).get("Item")
        if not item:
            return False
        item_ttl = int(cast(Any, item.get(self.TTL_ATTRIBUTE_NAME) or 0))
        return bool(item and item_ttl > int(time.time()))
