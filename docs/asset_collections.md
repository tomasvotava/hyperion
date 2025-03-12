# Asset Collections in Hyperion

Asset Collections provide a type-safe, declarative way to fetch and work with data assets from the Hyperion catalog.

## Overview

The `AssetCollection` system makes it easy to:

- Define what feature data you need with proper type definitions
- Fetch all required data in a single call
- Access typed data with full IDE support
- Control concurrency during fetching

## Basic Usage

### 1. Define a Feature Model

First, create a model class that extends both `FeatureModel` and `pydantic.BaseModel`:

```python
from pydantic import BaseModel
from hyperion.entities.catalog import FeatureModel
from hyperion.dateutils import TimeResolution

class WeatherFeature(FeatureModel, BaseModel):
    asset_name: ClassVar = "weather_data"
    resolution: ClassVar = TimeResolution(1, "d")

    timestamp: datetime.datetime
    temperature: float
    humidity: float
```

### 2. Create an Asset Collection

Define a collection class that declares what feature data you need:

```python
from hyperion.collections.asset_collection import AssetCollection, FeatureFetchSpecifier
import datetime

class WeatherDataCollection(AssetCollection):
    # Fetch last 7 days of weather data (calculated from the time when fetch_all() is called)
    weather = FeatureFetchSpecifier(
        WeatherFeature,
        start_date=datetime.timedelta(days=-7)
    )

    # Fetch historical data from a specific date range
    historical_weather = FeatureFetchSpecifier(
        WeatherFeature,
        start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    )
```

### 3. Fetch and Use the Data

```python
# Fetch all data asynchronously
await WeatherDataCollection.fetch_all()

# Access the data with full type safety
for record in WeatherDataCollection.weather:
    print(f"Temperature: {record.temperature}°C at {record.timestamp}")

# Calculate average temperature
avg_temp = sum(record.temperature for record in WeatherDataCollection.weather) / len(WeatherDataCollection.weather)
```

## Advanced Features

### Custom Catalog

You can specify a custom catalog for your collection:

```python
class CustomCollection(AssetCollection):
    catalog: ClassVar = my_custom_catalog
    weather = FeatureFetchSpecifier(WeatherFeature)
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

## Important Notes

1. **Shared Data**: All instances of a collection class share the same data.

2. **Lazy Fetching**: Data is not fetched until `fetch_all()` is called.

3. **Class-level API**: Most methods are class methods, not instance methods.

4. **Requirements**: Feature models must have `asset_name` and `resolution` class variables.

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

- Caching support for feature data
- Support for DataLake and PersistentStore assets
- Validation controls for better performance with large datasets
