"""Adapters layer: concrete port implementations.

Intentionally empty -- importing ``hyperion.adapters`` (or any subpackage)
must not eagerly pull a backend's heavy dependencies. Import the specific
adapter module you need (e.g. ``hyperion.adapters.storage.s3``).
"""
