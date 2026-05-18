# Configure backends via environment

Hyperion is configured through environment variables (via
[`EnvProxy`](https://github.com/tomasvotava/env-proxy)).
`Catalog.from_config()`, `Cache.from_config()`, `GoogleMaps.from_config()` and
the other `*.from_config()` factories read these and wire the right adapters —
so the same code runs locally or on AWS by changing configuration only.

For local development you can put these in a `.env` file:

```bash
# Common settings
HYPERION_COMMON_LOG_PRETTY=True
HYPERION_COMMON_LOG_LEVEL=INFO
HYPERION_COMMON_SERVICE_NAME=my-service

# Storage settings
HYPERION_STORAGE_DATA_LAKE_BUCKET=my-data-lake-bucket
HYPERION_STORAGE_FEATURE_STORE_BUCKET=my-feature-store-bucket
HYPERION_STORAGE_PERSISTENT_STORE_BUCKET=my-persistent-store-bucket
HYPERION_STORAGE_SCHEMA_PATH=s3://my-schema-bucket/schemas
HYPERION_STORAGE_MAX_CONCURRENCY=5

# Queue settings
## SQS Queue
HYPERION_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
## OR file queue (local)
HYPERION_QUEUE_PATH=/tmp/hyperion-queue.json
HYPERION_QUEUE_PATH_OVERWRITE=True   # overwrite queue file if it exists (default: False)

# Secrets settings
HYPERION_SECRETS_BACKEND=AWSSecretsManager

# HTTP settings (optional)
HYPERION_HTTP_PROXY_HTTP=http://proxy:8080
HYPERION_HTTP_PROXY_HTTPS=http://proxy:8080

# Geo settings (optional)
HYPERION_GEO_GMAPS_API_KEY=your-google-maps-api-key

# Cache settings
HYPERION_STORAGE_CACHE_DYNAMODB_TABLE=my-cache-table   # optional DynamoDB cache table
HYPERION_STORAGE_CACHE_DYNAMODB_DEFAULT_TTL=3600       # default cache TTL (seconds)
HYPERION_STORAGE_CACHE_LOCAL_PATH=/tmp/hyperion-cache  # local file cache path
HYPERION_STORAGE_CACHE_KEY_PREFIX=my-prefix            # optional cache-key prefix

# Source parameters (optional)
HYPERION_SOURCE_PARAMS={"key": "value"}                # JSON passed to the source
```

The authoritative list of every configuration option is the
[`hyperion.config`](../reference/config.md) module in the API reference (its
source mirrors the environment variable names).

!!! tip "`from_config()` does not configure a cache"
    `Catalog.from_config()` wires storage (and queue/schema) from the
    environment but does **not** attach a cache. To enable asset caching pass
    `cache=` to the `Catalog` constructor explicitly — see
    [Use a cached Catalog](cached-catalog.md).
