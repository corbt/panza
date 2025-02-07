from typing import Tuple, Any
import sqlite3
import pickle
from .cache import CacheBackend, Cache


class SQLiteBackend(CacheBackend):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    async def setup(self):
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

    async def get(self, fn_id: str, arg_hash: str) -> Tuple[bool, Any]:
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

    async def set(self, fn_id: str, arg_hash: str, result: Any) -> None:
        pickled_data = pickle.dumps(result)
        chunk_size = 1024 * 1024  # 1MB chunks
        is_chunked = len(pickled_data) > chunk_size

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM cache WHERE fn_id = ? AND arg_hash = ?",
                (fn_id, arg_hash),
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

    async def delete(self, fn_id: str, arg_hash: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM cache WHERE fn_id = ? AND arg_hash = ?",
                (fn_id, arg_hash),
            )
            conn.commit()

    async def delete_all(self) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cache")
            conn.commit()

    async def delete_by_fn_id(self, fn_id: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cache WHERE fn_id = ?", (fn_id,))
            conn.commit()


class SQLiteCache(Cache):
    def __init__(self, db_path: str):
        super().__init__(SQLiteBackend(db_path))
