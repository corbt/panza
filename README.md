# Panza

Panza is a Python library providing powerful caching and concurrency control for async functions.

## Features

- **Flexible Caching**: Cache results of async functions with SQLite or S3 backends
- **Concurrency Control**: Limit concurrent executions of async functions
- **Easy to Use**: Simple decorator-based API

## Installation

```bash
uv add git+https://github.com/corbt/panza.git
```

## Quick Start

### Caching

Panza offers two cache backends: SQLite for local development and S3 for distributed applications.

#### SQLite Cache

```python
import asyncio
from panza import SQLiteCache

# Create a SQLite cache
cache = SQLiteCache("cache.db")

# Decorate your async function
@cache.cache(id="my_function")
async def fetch_data(param):
    # Expensive operation
    await asyncio.sleep(1)
    return f"Result for {param}"

async def main():
    # First call will execute and cache
    result1 = await fetch_data("test")
    print(result1)  # Result for test

    # Second call will return from cache
    result2 = await fetch_data("test")
    print(result2)  # Result for test (from cache)

    # You can bust the cache for specific parameters
    await fetch_data.bust_cache("test")

    # Or for the entire function
    await fetch_data.bust_cache()

asyncio.run(main())
```

#### S3 Cache

```python
from panza import S3Cache

# Create an S3 cache
cache = S3Cache(
    "my-bucket/cache-prefix",
    auto_create_bucket=True,
    region_name="us-west-2"
)

@cache.cache(id="my_function")
async def fetch_data(param):
    # Expensive operation
    return f"Result for {param}"
```

### Custom Hash Function

You can provide a custom hash function to control how cache keys are generated:

```python
@cache.cache(
    id="my_function",
    hash_func=lambda param: f"custom_key_{param}"
)
async def fetch_data(param):
    # ...
```

### Limiting Concurrency

The `limit_concurrency` decorator prevents too many instances of a function from running at once:

```python
from panza import limit_concurrency

# Limit to 5 concurrent executions
@limit_concurrency(5)
async def process_item(item):
    await asyncio.sleep(1)
    return f"Processed {item}"

# With key-based limiting (limit per key)
@limit_concurrency(3, derive_key=lambda user_id, *args, **kwargs: f"user_{user_id}")
async def process_user_data(user_id, data):
    await asyncio.sleep(1)
    return f"Processed data for user {user_id}"
```

## Combining Features

You can combine caching and concurrency limiting:

```python
from panza import SQLiteCache, limit_concurrency

cache = SQLiteCache("cache.db")

@limit_concurrency(5)
@cache.cache(id="fetch_and_process")
async def fetch_and_process(item_id):
    # Expensive operation with limited concurrency and caching
    return result
```

## Advanced Usage

### Reading from Cache Without Execution

Use `.read_cache()` to check if a result is cached without executing the function:

```python
@cache.cache(id="expensive_function")
async def expensive_function(param):
    await asyncio.sleep(5)  # Simulate expensive operation
    return f"Computed result for {param}"

# Check cache without executing
cache_hit, result = await expensive_function.read_cache("test")
if cache_hit:
    print(f"Found in cache: {result}")
else:
    print("Not in cache")
    # Optionally execute now
    result = await expensive_function("test")
```

### Direct Cache Access

```python
# Set arbitrary values
await cache.set("my_key", "my_value")

# Get values (raises KeyError if not found)
value = await cache.get("my_key")

# Clear the entire cache
await cache.bust_all()
```
