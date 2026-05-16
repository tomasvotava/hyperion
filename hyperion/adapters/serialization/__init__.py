"""Serialization adapters.

Empty on purpose: no eager re-exports so touching the ``hyperion.adapters``
namespace stays free of the ``[catalog]`` (fastavro) dependency. Import the
concrete serializer explicitly from :mod:`hyperion.adapters.serialization.avro`.
"""
