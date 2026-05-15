"""Port: object storage abstraction.

This is an intentionally empty placeholder. The full ``StoragePort`` Protocol
(``put``/``get``/``open``/``iter_keys``/``exists``/``delete``/``get_attributes``
and their async variants) is defined in step S4 of the DDD refactor, when
``Catalog`` is inverted onto it. Defined now so ``hyperion.ports.storage``
imports cleanly and downstream layout/CI checks can reference it.
"""

from __future__ import annotations

from typing import Protocol


class StoragePort(Protocol):
    """Abstraction over object storage backends. Filled in S4 (see module docstring)."""
