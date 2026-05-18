"""Application layer: use-cases that orchestrate domain + ports.

May import from ``hyperion.domain``, ``hyperion.ports`` and the
``hyperion.catalog`` package; never imported by ``ports`` or ``domain``. No
eager re-exports here -- import the concrete module you need explicitly.
"""
