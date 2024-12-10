from dotenv import find_dotenv, load_dotenv
from env_proxy import EnvConfig, EnvProxy, Field

load_dotenv(find_dotenv(usecwd=True))


class CommonConfig(EnvConfig):
    env_proxy = EnvProxy(prefix="HYPERION_COMMON")

    log_pretty: bool = Field(
        default=False, description="If truthy, logs will be pretty-printed. Defaults to JSON logs."
    )
    log_level: str = Field(default="DEBUG", description="The minimum log level.")
    service_name: str = Field(description="The name of the service.", default="hyperion")


class StorageConfig(EnvConfig):
    env_proxy = EnvProxy(prefix="HYPERION_STORAGE")

    data_lake_bucket: str = Field(description="Data lake store bucket name.")
    feature_store_bucket: str = Field(description="Feature store bucket name.")
    persistent_store_bucket: str = Field(description="Persistent store bucket name.")

    data_lake_prefix: str = Field(default="", description="Optional data lake paths prefix.")
    feature_store_prefix: str = Field(default="", description="Optional feature store paths prefix.")
    persistent_store_prefix: str = Field(default="", description="Optional persistent store path prefix.")

    schema_path: str = Field(description="The path to the schema store. Supported schemes: 'file', 's3', 'python'.")

    cache_dynamodb_table: str | None = Field(
        description="The name of DynamoDB table that will be used as key-value cache. If not set, memory cache is used."
    )
    cache_dynamodb_default_ttl: int = Field(
        default=60, description="Default TTL for key-value cache in DynamoDB (in seconds)."
    )

    cache_local_path: str | None = Field(
        description="Path to a directory where cache is going to be stored to.", default=None
    )
    cache_key_prefix: str = Field(default="", description="Optional prefix for key-value cache items.")


class QueueConfig(EnvConfig):
    env_proxy = EnvProxy(prefix="HYPERION_QUEUE")

    url: str | None = Field(description="The URL of the SQS queue.", default=None)


class SecretsConfig(EnvConfig):
    env_proxy = EnvProxy(prefix="HYPERION_SECRETS")

    backend: str | None = Field(
        description="The backend to use for secrets management. Can only be `AWSSecretsManager` or None.", default=None
    )


class GeoConfig(EnvConfig):
    env_proxy = EnvProxy(prefix="HYPERION_GEO")

    gmaps_api_key: str | None = Field(description="Google Maps API key.")


config = CommonConfig()
storage_config = StorageConfig()
geo_config = GeoConfig()
queue_config = QueueConfig()
secrets_config = SecretsConfig()
