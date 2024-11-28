from typing import Any, TypeVar

T = TypeVar("T")


def assert_type(variable: Any, assertion: type[T]) -> T:
    if not isinstance(variable, assertion):
        raise TypeError(f"Expected {assertion}, got {type(variable)}.")
    return variable
