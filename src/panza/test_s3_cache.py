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
async def s3_cache():
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_access_key or not aws_secret_key:
        pytest.skip("AWS credentials not set in environment; skipping S3 tests.")

    bucket = os.getenv("AWS_S3_BUCKET")
    prefix = "prefix"
    region = os.getenv("AWS_REGION", "us-west-2")
    # Optionally provide a custom S3 endpoint URL (e.g. if you're using a non-AWS S3 provider)
    endpoint = os.getenv("AWS_S3_ENDPOINT_URL")  # Can be None if not provided

    # Create the S3Cache instance.
    cache_instance = S3Cache(
        f"{bucket}/{prefix}",
        auto_create_bucket=True,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
        endpoint_url=endpoint,
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
