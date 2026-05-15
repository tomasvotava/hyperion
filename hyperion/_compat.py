"""Internal helpers for the DDD refactor's import-relocation deprecation shims.

Not public API. Old module locations keep their moved symbols importable for the
whole ``hyperion-sdk`` 1.x line via a module-level :pep:`562` ``__getattr__`` that
delegates here. The symbols are removed in 2.0.
"""

from __future__ import annotations

import warnings
from typing import Any

# stacklevel chain when a caller does ``from old.module import Moved`` (or
# ``old.module.Moved``): frame 1 = this function, frame 2 = the old module's
# ``__getattr__``, frame 3 = the caller's own line. ``stacklevel=3`` points the
# warning at the consumer's import statement, not at hyperion internals.
_STACKLEVEL = 3


def moved_attr(*, name: str, value: Any, old_module: str, new_module: str) -> Any:
    """Emit a :class:`DeprecationWarning` for a relocated symbol and return it.

    Args:
        name: The public attribute name as accessed on the old module.
        value: The relocated object to return (imported from ``new_module``).
        old_module: Dotted path of the deprecated location.
        new_module: Dotted path of the new canonical location.

    Returns:
        The ``value`` unchanged, so the old import path keeps resolving.
    """
    warnings.warn(
        f"{old_module}.{name} has moved to {new_module}.{name}; import it from "
        f"{new_module} instead. The {old_module} alias is deprecated and will be "
        f"removed in hyperion-sdk 2.0.",
        DeprecationWarning,
        stacklevel=_STACKLEVEL,
    )
    return value
