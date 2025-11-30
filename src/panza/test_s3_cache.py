import os
import asyncio
import pytest
import pytest_asyncio
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from panza.cache import S3Cache


# Use pytest_asyncio.fixture instead of pytest.fixture for asynchronous fixtures.
@pytest_asyncio.fixture
async def s3_cache(moto_server):
    # Setup fake credentials to avoid real AWS usage
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    host = "127.0.0.1"
    port = 5543
    endpoint_url = f"http://{host}:{port}"

    # Create the S3Cache instance using the moto server
    cache_instance = S3Cache(
        "test-bucket/prefix",
        auto_create_bucket=True,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
        endpoint_url=endpoint_url,
    )
    # Ensure the backend is set up.
    await cache_instance.ensure_setup()
    yield cache_instance
    # Clean up by busting all cache entries after tests finish.
    await cache_instance.bust_all()


# --- Test Functions ---


# A simple async function to be cached.
async def async_add(a: int, b: int) -> int:
    return a + b


@pytest.mark.asyncio
async def test_async_cache_hit(s3_cache):
    """
    Test that a function decorated with the cache properly caches its result on S3.
    """
    cached_add = s3_cache.cache()(async_add)

    # First call should compute the result and store it in cache.
    result1 = await cached_add(2, 3)
    assert result1 == 5, "Expected 2 + 3 to equal 5."

    # Verify that the result is now cached on S3.
    cache_hit, cached_result = await cached_add.read_cache(2, 3)
    assert cache_hit, "Cache should hit after the first computation."
    assert cached_result == 5, "Cached result should be 5."

    # A subsequent call should return the cached value.
    result2 = await cached_add(2, 3)
    assert result2 == 5, "Subsequent call should return the cached value."


@pytest.mark.asyncio
async def test_custom_cache_id(s3_cache):
    """
    Test caching with a custom cache ID.
    """
    cached_add = s3_cache.cache(id="custom_add")(async_add)

    result = await cached_add(10, 15)
    assert result == 25, "Expected 10 + 15 to equal 25."

    cache_hit, cached_result = await cached_add.read_cache(10, 15)
    assert cache_hit, "Cache should be hit for the custom cache ID."
    assert cached_result == 25, "Cached value should be 25."

    # Bust the specific cache entry and verify that it is removed.
    await cached_add.bust_cache(10, 15)
    cache_hit_after, _ = await cached_add.read_cache(10, 15)
    assert not cache_hit_after, "Cache entry should be removed after busting."


@pytest.mark.asyncio
async def test_direct_cache_operations(s3_cache):
    """
    Test direct cache operations: set, get, and bust_all.
    """
    # Directly set a value in the cache.
    await s3_cache.set("test_direct_key", "initial_value")
    result = await s3_cache.get("test_direct_key")
    assert result == "initial_value", (
        "Direct get should retrieve the initially set value."
    )

    # Overwrite the key with a new value.
    await s3_cache.set("test_direct_key", "updated_value")
    updated_result = await s3_cache.get("test_direct_key")
    assert updated_result == "updated_value", "Value should update to the new value."

    # Bust all entries and ensure that retrieving the key now raises a KeyError.
    await s3_cache.bust_all()
    with pytest.raises(KeyError):
        await s3_cache.get("test_direct_key")
