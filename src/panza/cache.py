from abc import ABC, abstractmethod
import pickle
import hashlib
import asyncio
from typing import Optional, Callable, Tuple, Any
import os

BUST_CACHE = os.getenv("BUST_CACHE", "")
CACHE_ONLY = os.getenv("CACHE_ONLY", "false").lower() != "false"
PRINT_CACHE_MISSES = os.getenv("PRINT_CACHE_MISSES", "false").lower() != "false"

bust_cache_ids = set(BUST_CACHE.split(","))

if "all" in bust_cache_ids:
    print("Busting all caches, good luck")
elif bust_cache_ids != {""}:
    print(f"Busting cache for {bust_cache_ids}")

if CACHE_ONLY:
    print("CACHE_ONLY is set to True. Cache will be used exclusively.")


def normalize_for_hashing(obj):
    """
    Recursively normalize objects for deterministic hashing.
    - Pydantic models -> dicts (keeping sets as sets for sorting)
    - Sets -> sorted lists
    - Dicts -> dicts with sorted keys
    """
    # Check for Pydantic models (v1 and v2)
    # Use mode='python' to keep sets as sets so we can sort them deterministically below
    if hasattr(obj, "model_dump"):
        return normalize_for_hashing(obj.model_dump(mode="python"))
    elif hasattr(obj, "dict") and hasattr(obj, "json"):
        return normalize_for_hashing(obj.dict())

    if isinstance(obj, (list, tuple)):
        return [normalize_for_hashing(x) for x in obj]

    if isinstance(obj, dict):
        # Sort keys to ensure deterministic order
        return {k: normalize_for_hashing(v) for k, v in sorted(obj.items())}

    if isinstance(obj, set):
        # Convert sets to sorted lists
        try:
            return sorted([normalize_for_hashing(x) for x in obj])
        except TypeError:
            # Fallback for non-sortable elements (rare in cache keys)
            return sorted([normalize_for_hashing(x) for x in obj], key=str)

    return obj


class CacheBackend(ABC):
    @abstractmethod
    async def setup(self) -> None:
        pass

    @abstractmethod
    async def get(self, fn_id: str, arg_hash: str) -> Tuple[bool, Any]:
        """Returns (cache_hit, result)"""
        pass

    @abstractmethod
    async def set(self, fn_id: str, arg_hash: str, result: Any) -> None:
        pass

    @abstractmethod
    async def delete(self, fn_id: str, arg_hash: str) -> None:
        """Deletes a cache entry"""
        pass

    @abstractmethod
    async def delete_all(self) -> None:
        """Deletes all cache entries"""
        pass

    @abstractmethod
    async def delete_by_fn_id(self, fn_id: str) -> None:
        """Deletes all cache entries for a specific function ID"""
        pass


class Cache:
    def __init__(self, backend: CacheBackend):
        self.backend = backend
        self._setup_done = False

    async def ensure_setup(self):
        if not self._setup_done:
            await self.backend.setup()
            self._setup_done = True

    def cache(
        self,
        id: Optional[str] = None,
        hash_func: Optional[Callable[..., str]] = None,
        debug: bool = False,
    ):
        def decorator(func):
            if not asyncio.iscoroutinefunction(func):
                raise ValueError(
                    "Cache decorator can only be used with async functions"
                )

            async def async_wrapper(*args, **kwargs):
                await self.ensure_setup()
                fn_id = id if id else func.__name__

                try:
                    if hash_func:
                        arg_hash = hashlib.sha256(
                            hash_func(*args, **kwargs).encode()
                        ).hexdigest()
                    else:
                        arg_hash = hashlib.sha256(
                            pickle.dumps(normalize_for_hashing((args, kwargs)))
                        ).hexdigest()
                except Exception as e:
                    print(
                        f"Error computing arg_hash for fn_id={fn_id} with inputs args={args}, kwargs={kwargs}"
                    )
                    raise e

                if debug:
                    print(f"Debug: fn_id={fn_id}, arg_hash={arg_hash}")

                should_bust_cache = fn_id in bust_cache_ids or "all" in bust_cache_ids
                if debug:
                    print(f"should_bust_cache={should_bust_cache}")

                if not should_bust_cache:
                    cache_hit, cached_result = await self.backend.get(fn_id, arg_hash)
                    if cache_hit:
                        return cached_result

                if CACHE_ONLY:
                    raise Exception(
                        f"Cache miss for fn_id={fn_id} with arg_hash={arg_hash}. CACHE_ONLY is set to True."
                    )

                if PRINT_CACHE_MISSES:
                    print(
                        f"Cache miss for fn_id={fn_id} with arg_hash={arg_hash}. Executing function."
                    )

                result = await func(*args, **kwargs)
                await self.backend.set(fn_id, arg_hash, result)
                return result

            async def bust_cache(*args, **kwargs):
                await self.ensure_setup()
                fn_id = id if id else func.__name__

                if not args and not kwargs:
                    await self.backend.delete_by_fn_id(fn_id)
                    return

                try:
                    if hash_func:
                        arg_hash = hashlib.sha256(
                            hash_func(*args, **kwargs).encode()
                        ).hexdigest()
                    else:
                        arg_hash = hashlib.sha256(
                            pickle.dumps(normalize_for_hashing((args, kwargs)))
                        ).hexdigest()
                    await self.backend.delete(fn_id, arg_hash)
                except Exception as e:
                    print(
                        f"Error computing arg_hash for fn_id={fn_id} with inputs args={args}, kwargs={kwargs}"
                    )
                    raise e

            async def read_cache(*args, **kwargs):
                await self.ensure_setup()
                fn_id = id if id else func.__name__

                try:
                    if hash_func:
                        arg_hash = hashlib.sha256(
                            hash_func(*args, **kwargs).encode()
                        ).hexdigest()
                    else:
                        arg_hash = hashlib.sha256(
                            pickle.dumps(normalize_for_hashing((args, kwargs)))
                        ).hexdigest()
                except Exception as e:
                    print(
                        f"Error computing arg_hash for fn_id={fn_id} with inputs args={args}, kwargs={kwargs}"
                    )
                    raise e

                return await self.backend.get(fn_id, arg_hash)

            async_wrapper.bust_cache = bust_cache
            async_wrapper.read_cache = read_cache
            return async_wrapper

        return decorator

    async def bust_all(self) -> None:
        """Busts the entire cache"""
        await self.ensure_setup()
        await self.backend.delete_all()

    async def set(self, key: str, value: Any) -> None:
        """Directly set a value in the cache using a string key"""
        await self.ensure_setup()
        fn_id = "__direct"
        arg_hash = hashlib.sha256(key.encode()).hexdigest()
        await self.backend.set(fn_id, arg_hash, value)

    async def get(self, key: str) -> Any:
        """
        Directly get a value from the cache using a string key.
        Raises KeyError if the key hasn't been set.
        """
        await self.ensure_setup()
        fn_id = "__direct"
        arg_hash = hashlib.sha256(key.encode()).hexdigest()
        cache_hit, result = await self.backend.get(fn_id, arg_hash)
        if not cache_hit:
            raise KeyError(f"No cache entry found for key: {key}")
        return result


from .sqlite_backend import SQLiteCache
try:
    from .s3_backend import S3Cache
except ImportError:
    pass
