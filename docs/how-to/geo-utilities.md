# Use the geo utilities

Hyperion's geo support is split so that Haversine distance math stays in the
**lite core** (`hyperion.domain.geo`), while Google Maps geocoding is an opt-in
adapter behind the `[geo]` extra.

```bash
pip install 'hyperion-sdk[geo]'
```

## Geocode and reverse-geocode

```python
from hyperion.adapters.geocoder.google import GoogleMaps
from hyperion.domain.geo import Location

gmaps = GoogleMaps.from_config()  # reads HYPERION_GEO_GMAPS_API_KEY

with gmaps:
    location = gmaps.geocode("1600 Amphitheatre Parkway, Mountain View, CA")
    print(f"Latitude: {location.latitude}, Longitude: {location.longitude}")

    named_location = gmaps.reverse_geocode(location)
    print(f"Address: {named_location.address}")
    print(f"Country: {named_location.country}")
```

Use `GoogleMaps` as a context manager so its HTTP client is opened and closed
cleanly.

## Distance math without the extra

`hyperion.domain.geo.Location` provides Haversine distance and lives in the
lite core — no `[geo]` extra and no API key required for pure geometry.

## See also

- [Configure backends via environment](configure-via-environment.md) for
  `HYPERION_GEO_GMAPS_API_KEY`.
- API: [`hyperion.adapters.geocoder.google`](../reference/adapters/geocoder/google.md),
  [`hyperion.domain.geo`](../reference/domain/geo.md).
