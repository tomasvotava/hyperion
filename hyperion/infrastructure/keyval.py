"""Concrete key-value store adapters.

If you need a store with a TTL, look at :class:`hyperion.ports.cache.Cache`
instead.

.. deprecated::
    The abstract :class:`KeyValueStore` and the :data:`CompressionType` alias
    moved to :mod:`hyperion.ports.keyval`. Import them from there. This module
    keeps them importable (with a :class:`DeprecationWarning`) for the whole
    ``hyperion-sdk`` 1.x line and still hosts the concrete adapters until S6.
"""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, TypeGuard, cast

import boto3

from hyperion._compat import moved_attr
from hyperion.ports.keyval import CompressionType as _CompressionType
from hyperion.ports.keyval import KeyValueStore as _KeyValueStore

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.type_defs import ScanInputTableScanTypeDef

    from hyperion.ports.keyval import CompressionType, KeyValueStore

_MOVED: dict[str, tuple[object, str]] = {
    "KeyValueStore": (_KeyValueStore, "hyperion.ports.keyval"),
    "CompressionType": (_CompressionType, "hyperion.ports.keyval"),
}

__all__ = [
    "CompressionType",
    "DynamoDBStore",
    "InMemoryStore",
    "KeyValueStore",
    "is_valid_compression_type",
]


def __getattr__(name: str) -> object:
    if name in _MOVED:
        value, new_module = _MOVED[name]
        return moved_attr(
            name=name, value=value, old_module="hyperion.infrastructure.keyval", new_module=new_module
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def is_valid_compression_type(compression_type: str) -> TypeGuard[_CompressionType]:
    return compression_type in ("snappy", "gzip")


class DynamoDBStore(_KeyValueStore):
    """A key-value store using DynamoDB.

    The table must have a key attribute and a value attribute.
    """

    def __init__(
        self,
        prefix: str | None = None,
        compression: _CompressionType | None = None,
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


class InMemoryStore(_KeyValueStore):
    """An in-memory key-value store.

    This store is not persistent and is not shared between instances.
    """

    def __init__(self, prefix: str | None = None, compression: _CompressionType | None = None):
        super().__init__(prefix, compression)
        self.store: dict[str, bytes] = {}

    def _get_raw(self, hashed_key: str) -> bytes | None:
        return self.store.get(hashed_key)

    def _set_raw(self, hashed_key: str, compresed_value: bytes) -> None:
        self.store[hashed_key] = compresed_value

    def _delete_raw(self, hashed_key: str) -> None:
        self.store.pop(hashed_key, None)

    def _iter_all_keys(self) -> Iterable[str]:
        yield from self.store
