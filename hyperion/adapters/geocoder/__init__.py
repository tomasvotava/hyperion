"""Geocoder adapters. Import the concrete module you need explicitly.

``static`` is lite (offline / tests); ``google`` requires ``googlemaps``
(``[geo]``). No eager re-exports here -- touching this namespace must stay
googlemaps-free.
"""
