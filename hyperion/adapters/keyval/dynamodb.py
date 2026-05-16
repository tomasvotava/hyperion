"""DynamoDB-backed :class:`KeyValueStore` adapter (requires boto3 -- ``[aws]``).

The table must have a key attribute and a value attribute.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

import boto3

from hyperion.ports.keyval import CompressionType, KeyValueStore

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.type_defs import ScanInputTableScanTypeDef


class DynamoDBStore(KeyValueStore):
    """A key-value store using DynamoDB.

    The table must have a key attribute and a value attribute.
    """

    def __init__(
        self,
        prefix: str | None = None,
        compression: CompressionType | None = None,
        table_name: str | None = None,
        key_attribute: str = "key",
        value_attribute: str = "value",
    ):
        super().__init__(prefix, compression)
        self.client = boto3.resource("dynamodb")
        if table_name is None:
            raise ValueError("No table name provided for DynamoDBStore.")
        self.table_name = table_name
        self.table = self.client.Table(self.table_name)
        self.key_attribute = key_attribute
        self.value_attribute = value_attribute

    def _get_raw(self, hashed_key: str) -> bytes | None:
        response = self.table.get_item(Key={self.key_attribute: hashed_key})
        item = response.get("Item")
        if not item:
            return None
        return bytes(cast(Any, item[self.value_attribute]))

    def _set_raw(self, hashed_key: str, compresed_value: bytes) -> None:
        self.table.put_item(Item={self.key_attribute: hashed_key, self.value_attribute: compresed_value})

    def _delete_raw(self, hashed_key: str) -> None:
        self.table.delete_item(Key={self.key_attribute: hashed_key})

    def _iter_all_keys(self) -> Iterable[str]:
        scan_kwargs: ScanInputTableScanTypeDef = {
            "ProjectionExpression": "#k",
            "ExpressionAttributeNames": {"#k": self.key_attribute},
        }
        while True:
            response = self.table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                key = str(item[self.key_attribute])
                if self.prefix:
                    key = key.replace(f"{self.prefix}:", "", 1)
                yield key
            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
