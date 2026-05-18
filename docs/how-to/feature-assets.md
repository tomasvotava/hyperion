# Work with FeatureAssets

A `FeatureAsset` represents processed feature data at a given **time
resolution** (for analytics or ML features), optionally with extra partition
keys. Use it when data is time-bucketed rather than raw.

## Store a feature asset

```python
from hyperion.catalog.catalog import Catalog
from hyperion.domain.assets import FeatureAsset
from hyperion.dateutils import TimeResolution
from datetime import datetime, timezone

catalog = Catalog.from_config()

resolution = TimeResolution(1, "d")  # 1-day resolution
asset = FeatureAsset(
    name="customer_activity",
    partition_date=datetime.now(timezone.utc),
    resolution=resolution,
    schema_version=1,
)

catalog.store_asset(asset, [
    {"customer_id": 1, "activity_score": 87.5, "date": datetime.now(timezone.utc)},
    {"customer_id": 2, "activity_score": 92.1, "date": datetime.now(timezone.utc)},
])
```

## Iterate a time range

`iter_feature_store_partitions` yields the `FeatureAsset` for each partition in
a date range; retrieve each one to read its records:

```python
from_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
to_date = datetime(2023, 1, 31, tzinfo=timezone.utc)

for feature_asset in catalog.iter_feature_store_partitions(
    feature_name="customer_activity",
    resolution="1d",          # string form accepted too
    date_from=from_date,
    date_to=to_date,
):
    for record in catalog.retrieve_asset(feature_asset):
        print(record)
```

## See also

- [Repartition data](repartition-data.md) to change partition granularity.
- [Fetch data with Asset Collections](asset-collections.md) for a typed,
  declarative interface over many feature assets at once.
- [Assets and the Catalog](../explanation/assets-and-catalog.md) for the
  concepts behind asset types.
