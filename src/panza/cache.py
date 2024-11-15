import sqlite3
import functools
import pickle
import hashlib
import asyncio
from typing import Optional, Callable
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


class SQLiteCache:
    def __init__(self, db_path):
        self.db_path = db_path
        self._setup()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _setup(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                    fn_id TEXT,
                    arg_hash TEXT,
                    result BLOB,
                    chunk_index INTEGER,
                    is_chunked BOOLEAN,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (fn_id, arg_hash, chunk_index)
                )
            """
            )
            conn.commit()

    def _get_from_cache(self, fn_id, arg_hash):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT result, is_chunked, chunk_index FROM cache 
                WHERE fn_id = ? AND arg_hash = ?
                ORDER BY chunk_index
            """,
                (fn_id, arg_hash),
            )
            rows = cursor.fetchall()

            if not rows:
                return False, None

            if not rows[0][1]:  # not chunked
                return True, pickle.loads(rows[0][0])

            # Combine chunks
            result_data = b"".join(row[0] for row in rows)
            return True, pickle.loads(result_data)

    def _set_cache(self, fn_id, arg_hash, result):
        pickled_data = pickle.dumps(result)
        chunk_size = 1024 * 1024  # 1MB chunks
        is_chunked = len(pickled_data) > chunk_size

        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Clear existing entries
            cursor.execute(
                "DELETE FROM cache WHERE fn_id = ? AND arg_hash = ?", (fn_id, arg_hash)
            )

            if not is_chunked:
                cursor.execute(
                    """
                    INSERT INTO cache (fn_id, arg_hash, result, chunk_index, is_chunked)
                    VALUES (?, ?, ?, 0, 0)
                """,
                    (fn_id, arg_hash, pickled_data),
                )
            else:
                # Split into chunks
                chunks = [
                    pickled_data[i : i + chunk_size]
                    for i in range(0, len(pickled_data), chunk_size)
                ]
                for i, chunk in enumerate(chunks):
                    cursor.execute(
                        """
                        INSERT INTO cache (fn_id, arg_hash, result, chunk_index, is_chunked)
                        VALUES (?, ?, ?, ?, 1)
                    """,
                        (fn_id, arg_hash, chunk, i),
                    )

            conn.commit()

    def cache(
        self,
        id: Optional[str] = None,
        hash_func: Optional[Callable[..., str]] = None,
        debug: bool = False,
        bust_cache: bool = False,
    ):
        def decorator(func):
            is_coroutine = asyncio.iscoroutinefunction(func)

            if is_coroutine:

                @functools.wraps(func)
                async def wrapper(*args, runtime_bust_cache=False, **kwargs):
                    if id:
                        fn_id = id
                    else:
                        fn_id = func.__name__

                    try:
                        if hash_func:
                            arg_hash = hashlib.sha256(
                                hash_func(*args, **kwargs).encode()
                            ).hexdigest()
                        else:
                            # if debug:
                            #     print((args, kwargs))
                            arg_hash = hashlib.sha256(
                                pickle.dumps((args, kwargs))
                            ).hexdigest()
                    except Exception as e:
                        print(
                            f"Error computing arg_hash for fn_id={fn_id} with inputs args={args}, kwargs={kwargs}"
                        )
                        raise e  # Re-throw the exception after logging

                    if debug:
                        print(f"Debug: fn_id={fn_id}, arg_hash={arg_hash}")

                    should_bust_cache = (
                        bust_cache
                        or runtime_bust_cache
                        or fn_id in bust_cache_ids
                        or "all" in bust_cache_ids
                    )
                    if debug:
                        print(f"should_bust_cache={should_bust_cache}")

                    if not should_bust_cache:
                        cache_hit, cached_result = self._get_from_cache(fn_id, arg_hash)
                        if debug:
                            print(f"Debug: cache_hit={cache_hit}")
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
                        # print(f"Args: {args}")
                        # print(f"Kwargs: {kwargs}")

                    result = await func(*args, **kwargs)
                    self._set_cache(fn_id, arg_hash, result)
                    return result
            else:

                @functools.wraps(func)
                def wrapper(*args, runtime_bust_cache=False, **kwargs):
                    if id:
                        fn_id = id
                    else:
                        fn_id = func.__name__

                    try:
                        if hash_func:
                            arg_hash = hashlib.sha256(
                                hash_func(*args, **kwargs).encode()
                            ).hexdigest()
                        else:
                            # if debug:
                            #     print((args, kwargs))
                            arg_hash = hashlib.sha256(
                                pickle.dumps((args, kwargs))
                            ).hexdigest()
                    except Exception as e:
                        print(
                            f"Error computing arg_hash for fn_id={fn_id} with inputs args={args}, kwargs={kwargs}"
                        )
                        raise e  # Re-throw the exception after logging

                    if debug:
                        print(f"Debug: fn_id={fn_id}, arg_hash={arg_hash}")

                    should_bust_cache = (
                        bust_cache
                        or runtime_bust_cache
                        or fn_id in bust_cache_ids
                        or "all" in bust_cache_ids
                    )
                    if debug:
                        print(f"should_bust_cache={should_bust_cache}")

                    if not should_bust_cache:
                        cache_hit, cached_result = self._get_from_cache(fn_id, arg_hash)
                        if debug:
                            print(f"Debug: cache_hit={cache_hit}")
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
                        # print(f"Args: {args}")
                        # print(f"Kwargs: {kwargs}")

                    result = func(*args, **kwargs)
                    self._set_cache(fn_id, arg_hash, result)
                    return result

            return wrapper

        return decorator
