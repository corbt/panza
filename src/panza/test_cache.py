import asyncio
import pytest
from .cache import SQLiteCache, S3Cache, normalize_for_hashing
import os
import time
from dataclasses import dataclass


# Parameterized fixture to yield both cache backends
@pytest.fixture(params=["sqlite", "s3"])
def cache(request, tmp_path, moto_server):
    if request.param == "sqlite":
        db_path = str(tmp_path / "test_cache.db")
        c = SQLiteCache(db_path)
        yield c
        asyncio.run(c.bust_all())
        if os.path.exists(db_path):
            os.remove(db_path)
    elif request.param == "s3":
        host = "127.0.0.1"
        port = 5543
        endpoint_url = f"http://{host}:{port}"
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url
        # Initialize S3Cache with the endpoint_url already provided by our moto_server
        c = S3Cache(
            "test-bucket/testprefix",
            auto_create_bucket=True,
            region_name="us-east-1",
            endpoint_url=endpoint_url,
        )
        yield c
        asyncio.run(c.bust_all())
        del os.environ["AWS_ENDPOINT_URL"]


# Test functions to be cached
async def async_add(a: int, b: int) -> int:
    return a + b


async def async_multiply(a: int, b: int) -> int:
    return a * b


@pytest.mark.asyncio
async def test_async_cache_hit(cache):
    cached_add = cache.cache()(async_add)

    # First call - should be computed and then cached
    result1 = await cached_add(2, 3)
    assert result1 == 5

    # Verify cache hit
    cache_hit, cached_result = await cached_add.read_cache(2, 3)
    assert cache_hit
    assert cached_result == 5

    # Verify cached result is returned on subsequent calls
    result2 = await cached_add(2, 3)
    assert result2 == 5


@pytest.mark.asyncio
async def test_custom_cache_id(cache):
    cached_add = cache.cache(id="custom_add")(async_add)

    # Initial call and verify result is cached
    result = await cached_add(2, 3)
    assert result == 5
    cache_hit, cached_result = await cached_add.read_cache(2, 3)
    assert cache_hit
    assert cached_result == 5

    # Bust cache and verify it's gone
    await cached_add.bust_cache()
    cache_hit, _ = await cached_add.read_cache(2, 3)
    assert not cache_hit


@pytest.mark.asyncio
async def test_custom_hash_function(cache):
    def hash_func(a, b):
        return f"{a}+{b}"

    cached_add = cache.cache(hash_func=hash_func)(async_add)

    # Initial call and verify result is cached
    result = await cached_add(2, 3)
    assert result == 5
    cache_hit, cached_result = await cached_add.read_cache(2, 3)
    assert cache_hit
    assert cached_result == 5


@pytest.mark.asyncio
async def test_bust_specific_args(cache):
    cached_add = cache.cache()(async_add)

    # Cache multiple different calls
    result1 = await cached_add(2, 3)
    result2 = await cached_add(4, 5)
    assert result1 == 5
    assert result2 == 9

    # Bust cache for the (2, 3) call only
    await cached_add.bust_cache(2, 3)

    cache_hit1, _ = await cached_add.read_cache(2, 3)
    cache_hit2, cached_result2 = await cached_add.read_cache(4, 5)
    assert not cache_hit1  # (2,3) entry should not exist now
    assert cache_hit2
    assert cached_result2 == 9


@pytest.mark.asyncio
async def test_bust_entire_function(cache):
    cached_add = cache.cache()(async_add)
    cached_multiply = cache.cache()(async_multiply)

    await cached_add(2, 3)
    await cached_multiply(4, 5)

    await cached_add.bust_cache()

    # Verify first cache is cleared but second remains
    add_hit, _ = await cached_add.read_cache(2, 3)
    mult_hit, mult_result = await cached_multiply.read_cache(4, 5)
    assert not add_hit
    assert mult_hit
    assert mult_result == 20


@pytest.mark.asyncio
async def test_bust_entire_cache(cache):
    cached_add1 = cache.cache()(async_add)
    cached_add2 = cache.cache()(async_add)

    # Cache results for both functions
    await cached_add1(2, 3)
    await cached_add2(4, 5)

    # Bust entire cache
    await cache.bust_all()

    # Verify all caches are cleared
    add1_hit, _ = await cached_add1.read_cache(2, 3)
    add2_hit, _ = await cached_add2.read_cache(4, 5)
    assert not add1_hit
    assert not add2_hit


@pytest.mark.asyncio
async def test_direct_cache_operations(cache):
    # Test directly setting and getting a value
    await cache.set("test_key", "test_value")
    result = await cache.get("test_key")
    assert result == "test_value"

    # Test getting a non-existent key
    with pytest.raises(KeyError):
        await cache.get("nonexistent_key")

    # Test overwriting an existing key
    await cache.set("test_key", "new_value")
    result = await cache.get("test_key")
    assert result == "new_value"

    # Test that bust_all clears direct cache entries
    await cache.bust_all()
    with pytest.raises(KeyError):
        await cache.get("test_key")


@dataclass
class DataForTest:
    name: str
    value: int


@pytest.mark.asyncio
async def test_dataclass_cache_behavior(cache):
    """Test that dataclasses with identical contents hit the cache correctly"""

    @cache.cache()
    async def process_data(data: DataForTest) -> str:
        return f"Processed: {data.name} = {data.value}"

    # Create two identical dataclass instances
    data1 = DataForTest("test", 42)
    data2 = DataForTest("test", 42)

    # Verify they are equal but different objects
    assert data1 == data2
    assert data1 is not data2

    # First call - should be computed and cached
    result1 = await process_data(data1)
    assert result1 == "Processed: test = 42"

    # Verify cache hit with original instance
    cache_hit1, cached_result1 = await process_data.read_cache(data1)
    assert cache_hit1
    assert cached_result1 == "Processed: test = 42"

    # Verify cache hit with identical but different instance
    cache_hit2, cached_result2 = await process_data.read_cache(data2)
    assert cache_hit2
    assert cached_result2 == "Processed: test = 42"

    # Test with different dataclass instance should miss cache
    data3 = DataForTest("different", 99)
    cache_hit3, _ = await process_data.read_cache(data3)
    assert not cache_hit3


def test_normalize_for_hashing_sets():
    # Ensure sets are converted to sorted lists
    s = {"b", "a", "c"}
    normalized = normalize_for_hashing(s)
    assert isinstance(normalized, list)
    assert normalized == ["a", "b", "c"]


def test_normalize_for_hashing_dicts():
    # Ensure dict keys are sorted
    d = {"b": 2, "a": 1}
    normalized = normalize_for_hashing(d)
    assert list(normalized.keys()) == ["a", "b"]
    assert normalized["a"] == 1


@pytest.mark.asyncio
async def test_pydantic_normalization_behavior(cache):
    import pydantic
    from typing import Set

    class MyModel(pydantic.BaseModel):
        id: int
        tags: Set[str]

    m = MyModel(id=1, tags={"c", "a", "b"})

    # Access normalize_for_hashing to verify it handles the model
    normalized = normalize_for_hashing(m)

    # Should be a dict with sorted set in tags
    assert isinstance(normalized, dict)
    assert normalized["id"] == 1
    assert normalized["tags"] == ["a", "b", "c"]

    # Now test actual caching
    @cache.cache()
    async def process_model(model: MyModel):
        return "ok"

    await process_model(m)
    hit, _ = await process_model.read_cache(m)
    assert hit
