"""Key-Value stores.

If you need a store with a TTL, look at Cache instead.
"""

import gzip
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from fnmatch import fnmatch
from typing import Literal, cast

import boto3
import snappy

CompressionType = Literal["snappy", "gzip"]


class KeyValueStore(ABC, Iterable[str]):
    """A key-value store."""

    def __init__(self, prefix: str | None = None, compression: CompressionType | None = None) -> None:
        """Initialize the store.

        Args:
            prefix: A prefix to add to all keys.
            compression: The compression algorithm to use.
        """
        self.prefix = prefix
        self.compression = compression

    def _key(self, key: str) -> str:
        """Return the key with the prefix added."""
        prefix = f"{self.prefix}:" if self.prefix else ""
        return f"{prefix}{key}"

    def _compress(self, value: str) -> bytes:
        """Compress a value using configured compression.

        If no compression is selected, the string is encoded as utf-8 into bytes.

        Args:
            value: The value to compress.

        Returns:
            bytes: The compressed value.

        Raises:
            ValueError: If an unsupported compression is selected.
        """
        value_bytes = value.encode("utf-8")
        match self.compression:
            case None:
                return value_bytes
            case "snappy":
                return cast(bytes, snappy.compress(value_bytes))
            case "gzip":
                return gzip.compress(value_bytes)
            case _:
                raise ValueError(f"Unsupported compression {self.compression!r}.")

    def _decompress(self, value: bytes) -> str:
        """Decompress a value using configured compression.

        If no compression is selected, the bytes are decoded as utf-8 into a string.

        Args:
            value: The value to decompress.

        Returns:
            str: The decompressed value.

        Raises:
            ValueError: If an unsupported compression is selected.
            TypeError: If the decompression returns an unexpected type.
        """
        match self.compression:
            case None:
                return value.decode("utf-8")
            case "snappy":
                value_decompressed = snappy.decompress(value)
                if isinstance(value_decompressed, str):
                    return value_decompressed
                elif isinstance(value_decompressed, bytes):
                    return value_decompressed.decode("utf-8")
                raise TypeError(
                    "Unexpected value returned from snappy decompression, "
                    f"expected str | bytes, got {type(value_decompressed)!r}."
                )
            case "gzip":
                return gzip.decompress(value).decode("utf-8")
            case _:
                raise ValueError(f"Unsupported compression {self.compression!r}.")

    @abstractmethod
    def _get_raw(self, hashed_key: str) -> bytes | None:
        """Get a raw uncompressed value from the store.

        The key passed here must already be hashed and prefixed.

        Args:
            hashed_key: The key to get.

        Returns:
            bytes | None: The raw value, or None if not found.
        """

    def get(self, key: str) -> str | None:
        """Gets a value from the store."""
        if value := self._get_raw(self._key(key)):
            return self._decompress(value)
        return None

    @abstractmethod
    def _set_raw(self, hashed_key: str, compresed_value: bytes) -> None:
        """Set a raw, compressed bytes value in the store.

        The key passed here must already by hashed and prefixed.

        Args:
            hashed_key: The key to set.
            compresed_value: The compressed value to set.
        """

    def set(self, key: str, value: str) -> None:
        """Sets a value in the store."""
        return self._set_raw(self._key(key), self._compress(value))

    @abstractmethod
    def _delete_raw(self, hashed_key: str) -> None:
        """Delete a value from the store.

        The key passed here must already by hashed and prefixed.

        Args:
            hashed_key: The key to delete.
        """

    def __iter__(self) -> Iterator[str]:
        """Iterate all keys available in the store."""
        return iter(self._iter_all_keys())

    @abstractmethod
    def _iter_all_keys(self) -> Iterable[str]:
        """Iterate all keys available in the store."""

    def keys(self, match: str = "*") -> Iterable[str]:
        """Iterate all keys available in the store that match the given expression.

        Nothing fancy is supported, UNIX-style fnmatch is performed.

        Args:
            match: The expression to match.

        Returns:
            Iterable[str]: The keys that match the expression
        """
        for key in self._iter_all_keys():
            if fnmatch(key, match):
                yield key

    def delete(self, key: str) -> None:
        """Delete a value from the store.

        Args:
            key: The key to delete.
        """
        return self._delete_raw(self._key(key))

    def exists(self, key: str) -> bool:
        """Returns whether a key exists in the store.

        Args:
            key: The key to check.
        """
        return key in self.keys()

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def __getitem__(self, key: str) -> str | None:
        return self.get(key)

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __delitem__(self, key: str) -> None:
        return self.delete(key)


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
        return bytes(item[self.value_attribute])

    def _set_raw(self, hashed_key: str, compresed_value: bytes) -> None:
        self.table.put_item(Item={self.key_attribute: hashed_key, self.value_attribute: compresed_value})

    def _delete_raw(self, hashed_key: str) -> None:
        self.table.delete_item(Key={self.key_attribute: hashed_key})

    def _iter_all_keys(self) -> Iterable[str]:
        scan_kwargs = {"ProjectionExpression": "#k", "ExpressionAttributeNames": {"#k": self.key_attribute}}
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


class InMemoryStore(KeyValueStore):
    """An in-memory key-value store.

    This store is not persistent and is not shared between instances.
    """

    def __init__(self, prefix: str | None = None, compression: CompressionType | None = None):
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
