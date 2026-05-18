# Run a Source

Once you have written a `Source` (see the
[custom Source tutorial](../tutorials/custom-source.md)), there are three ways
to run it.

## In-process

Drive the source directly with a catalog:

```python
import asyncio
from hyperion.catalog.catalog import Catalog

asyncio.run(MyCustomSource._run(Catalog.from_config()))
```

## On AWS Lambda

Let the base class handle the Lambda event:

```python
def lambda_handler(event, context):
    MyCustomSource.handle_aws_lambda_event(event, context)
```

Schedule the Lambda (e.g. EventBridge) for periodic extraction; pass backfill
parameters through the event payload.

## As a CLI ("Argo Workflow" mode)

`SourceRunner` builds a click CLI over one or more sources:

```python
from hyperion.sources.cli import SourceRunner
from my_sources import FirstSource, SecondSource

if __name__ == "__main__":
    SourceRunner(FirstSource, SecondSource).cli()
```

```bash
python running_sources.py first-source run \
    --start-date 2025-01-01 \
    --end-date 2025-02-01 \
    --params '{"foo": "bar"}' \
    --queue-file /argo/output/messages.json \
    --queue-overwrite
```

The command name is the `source` attribute of each `Source` subclass. All
options are optional — see the
[custom Source tutorial](../tutorials/custom-source.md#4-expose-a-cli-argo-workflow-mode)
for the full option table.

## See also

- API: [`hyperion.sources.base`](../reference/sources/base.md),
  [`hyperion.sources.cli`](../reference/sources/cli.md).
- [Configure backends via environment](configure-via-environment.md) for
  `HYPERION_SOURCE_PARAMS` and queue settings.
