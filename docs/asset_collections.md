# Asset Collections in Hyperion

Asset Collections provide a type-safe, declarative way to fetch and work with data assets from the Hyperion catalog.

## Overview

The `AssetCollection` system makes it easy to:

- Define what feature data you need with proper type definitions
- Fetch all required data in a single call
- Access typed data with full IDE support
- Control concurrency during fetching
- Work with either Pydantic models or Polars DataFrames

## Basic Usage

### 1. Define a Feature Model

You can define feature models using either Pydantic (for object-oriented data) or Pandera with Polars (for large datasets).

#### Option A: Pydantic Model

Create a model class that extends both `FeatureModel` and `pydantic.BaseModel`:

```python
from pydantic import BaseModel
from hyperion.entities.catalog import FeatureModel
from hyperion.dateutils import TimeResolution
import datetime

class WeatherFeature(FeatureModel, BaseModel):
    asset_name: ClassVar = "weather_data"
    resolution: ClassVar = TimeResolution(1, "d")

    timestamp: datetime.datetime
    temperature: float
    humidity: float
```

#### Option B: Polars Model with Pandera

For large datasets or analytics workloads, create a model using `PolarsFeatureModel`:

```python
import pandera.typing as pt
from typing import Annotated, ClassVar
from pandera.engines.polars_engine import DateTime, Float64
from hyperion.entities.catalog import PolarsFeatureModel
from hyperion.dateutils import TimeResolution

class WeatherPolarsFeature(PolarsFeatureModel):
    _asset_name: ClassVar = "weather_data"
    _resolution: ClassVar = TimeResolution(1, "d")
    _schema_version: ClassVar = 1

    timestamp: pt.Series[Annotated[DateTime, False, "UTC", "us"]]
    temperature: pt.Series[Float64]
    humidity: pt.Series[Float64]
```

### 2. Create an Asset Collection

Define a collection class that declares what feature data you need:

```python
from hyperion.repository.asset_collection import AssetCollection, FeatureFetchSpecifier, PolarsFeatureFetchSpecifier
import datetime

class WeatherDataCollection(AssetCollection):
    # Fetch last 7 days of pydantic weather data 
    weather = FeatureFetchSpecifier(
        WeatherFeature,
        start_date=datetime.timedelta(days=-7)
    )

    # Fetch historical data using Polars for efficient processing
    historical_weather = PolarsFeatureFetchSpecifier(
        WeatherPolarsFeature,
        start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    )
```

### 3. Fetch and Use the Data

```python
# Fetch all data asynchronously
await WeatherDataCollection.fetch_all()

# Access the Pydantic data as objects
for record in WeatherDataCollection.weather:
    print(f"Temperature: {record.temperature}°C at {record.timestamp}")

# Work with Polars data using DataFrame operations
avg_temp = WeatherDataCollection.historical_weather.select(
    pl.col("temperature").mean().alias("avg_temp")
).collect()

# Or collect the Polars data if you need the full DataFrame
historical_df = WeatherDataCollection.historical_weather.collect()
print(f"Records: {len(historical_df)}")
```

## Advanced Features

### Custom Catalog

You can specify a custom catalog for your collection:

```python
class CustomCollection(AssetCollection):
    catalog: ClassVar = my_custom_catalog
    weather = FeatureFetchSpecifier(WeatherFeature)
    weather_polars = PolarsFeatureFetchSpecifier(WeatherPolarsFeature)
```

### Concurrency Control

Control how many concurrent fetches are allowed:

```python
class LimitedConcurrencyCollection(AssetCollection):
    max_concurrency: ClassVar = 4  # Limit to 4 concurrent requests
    weather = FeatureFetchSpecifier(WeatherFeature)
```

### Reset Data

Clear fetched data to fetch again:

```python
# Clear all data
WeatherDataCollection.clear()

# Fetch fresh data
await WeatherDataCollection.fetch_all()
```

## Working with Polars Data

When using `PolarsFeatureFetchSpecifier`, you get a LazyFrame with the following benefits:

1. **Lazy Evaluation**: Operations are only executed when you call `.collect()`
2. **Query Optimization**: Polars optimizes the execution plan
3. **Memory Efficiency**: Great for working with millions of rows
4. **Type Safety**: Full schema validation through Pandera

Example operations:

```python
# Filter data
hot_days = WeatherDataCollection.historical_weather.filter(
    pl.col("temperature") > 30
).collect()

# Aggregations
monthly_avg = WeatherDataCollection.historical_weather.group_by(
    pl.col("timestamp").dt.month()
).agg(
    pl.col("temperature").mean().alias("avg_temp"),
    pl.col("humidity").mean().alias("avg_humidity")
).collect()
```

## Important Notes

1. **Shared Data**: All instances of a collection class share the same data.

2. **Lazy Fetching**: Data is not fetched until `fetch_all()` is called.

3. **Class-level API**: Most methods are class methods, not instance methods.

4. **Requirements**:
   - Pydantic models must have `asset_name` and `resolution` class variables
   - Polars models must have `_asset_name` and `_resolution` class variables

5. **Data Size Considerations**:
   - Use Pydantic models for smaller datasets and when you need object-oriented manipulation
   - Use Polars models for large datasets (millions of rows) and analytical operations

## Date Specifications

You can specify date ranges in multiple ways:

- **Absolute dates**: Use `datetime` objects
- **Relative dates**: Use `timedelta` objects (negative for past, positive for future)
- **Mixed**: Combine absolute and relative dates

A date specification of `None` means:

- For `start_date`: Use minimum date (fetch all historical data)
- For `end_date`: Use current time (fetch up to now)

## Coming Soon

Future versions will include:

- Enhanced Polars streaming capabilities for extremely large datasets
- Column projection to reduce I/O for large datasets
- Support for DataLake and PersistentStore assets
- Advanced caching strategies for feature data
