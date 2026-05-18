# Repartition data

Use `Catalog.repartition` to change the partition granularity of a stored
asset — for example, re-bucket day-partitioned data into hourly partitions
keyed off a timestamp field.

```python
import asyncio
from datetime import datetime, timezone

from hyperion.catalog.catalog import Catalog
from hyperion.domain.assets import DataLakeAsset
from hyperion.dateutils import TimeResolutionUnit


async def repartition_data():
    catalog = Catalog.from_config()

    asset = DataLakeAsset(
        name="web_logs",
        date=datetime.now(timezone.utc),
        schema_version=1,
    )

    await catalog.repartition(
        asset,
        granularity=TimeResolutionUnit("h"),  # repartition by hour
        date_attribute="timestamp",           # field to derive the partition from
    )


asyncio.run(repartition_data())
```

`repartition` is asynchronous — run it inside an event loop (`asyncio.run`, or
`await` it from existing async code).

## See also

- [Work with FeatureAssets](feature-assets.md) for time-resolution concepts.
- API: [`hyperion.catalog.catalog`](../reference/catalog/catalog.md),
  [`hyperion.dateutils`](../reference/dateutils.md).
