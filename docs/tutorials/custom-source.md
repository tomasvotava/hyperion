# Building a custom Source

A **Source** extracts data from an external system and yields assets into the
catalog. In this tutorial you will write one, run it in-process, and then
expose it as a command-line tool.

This builds on [Your first DataLakeAsset](first-datalake-asset.md) — you should
already have a working local catalog (or `Catalog.from_config()` configured).

## 1. Write the Source

Subclass `Source`, give it a `source` name, and implement the async `run`
method as an async generator yielding `SourceAsset` objects:

```python
import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

from hyperion.catalog.catalog import Catalog
from hyperion.domain.assets import DataLakeAsset
from hyperion.sources.base import Source, SourceAsset


class MyCustomSource(Source):
    source = "my-custom-source"

    async def run(self, start_date=None, end_date=None) -> AsyncIterator[SourceAsset]:
        # Where you would call an API, read a file, query a DB, ...
        data = [
            {"id": 1, "name": "Item 1", "timestamp": datetime.now(timezone.utc)},
            {"id": 2, "name": "Item 2", "timestamp": datetime.now(timezone.utc)},
        ]

        asset = DataLakeAsset(name="my-custom-data", date=datetime.now(timezone.utc))
        yield SourceAsset(asset=asset, data=data)
```

## 2. Run it in-process

Drive the source with a catalog directly:

```python
if __name__ == "__main__":
    asyncio.run(MyCustomSource._run(Catalog.from_config()))
```

## 3. Run it on AWS Lambda

The base class can handle a Lambda event for you:

```python
def lambda_handler(event, context):
    MyCustomSource.handle_aws_lambda_event(event, context)
```

## 4. Expose a CLI ("Argo Workflow" mode)

`SourceRunner` generates a click-based CLI for one or more sources. Create
`running_sources.py`:

```python
from hyperion.sources.cli import SourceRunner
from my_sources import FirstSource, SecondSource

if __name__ == "__main__":
    SourceRunner(FirstSource, SecondSource).cli()
```

Invoke a source from the command line — the command name comes from each
class's `source` attribute:

```bash
python running_sources.py my-custom-source run \
    --start-date 2025-01-01 \
    --end-date 2025-02-01 \
    --params '{"foo": "bar"}' \
    --queue-file /argo/output/messages.json \
    --queue-overwrite
```

All generated options are optional:

| Option | Meaning |
|---|---|
| `--start-date` | start date passed to the source |
| `--end-date` | end date passed to the source |
| `--params` | JSON string of source-specific params |
| `--params-from` | path to a JSON file of source-specific params |
| `--queue-file` | output file for queued messages |
| `--queue-overwrite` | overwrite the queue file if it exists |

## What you learned

You implemented the `Source` contract, ran it three ways (in-process, Lambda,
CLI), and saw how `SourceRunner` turns sources into operable tools. See the
[Source framework explanation](../explanation/architecture.md) for how sources
fit the overall design, and
[Run a Source](../how-to/run-a-source.md) for deployment recipes.
