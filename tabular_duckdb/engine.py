import asyncio
import logging
import threading
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.errors import QueryException

log = logging.getLogger(__name__)

# httpfs provides the s3:// filesystem used to read the parquet files
EXTENSIONS = ("httpfs",)


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def install_extensions() -> None:
    """Download the extensions once, so that runtime only has to LOAD them."""
    with duckdb.connect(":memory:") as conn:
        for extension in EXTENSIONS:
            conn.execute(f"INSTALL {extension}")


class DuckDBEngine:
    """Owns the DuckDB connections: one for the API queries, a locked-down pool for user SQL."""

    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._sql_pool: asyncio.Queue[duckdb.DuckDBPyConnection] | None = None

    # --- lifecycle -----------------------------------------------------------------

    def _new_connection(self, metadata_cache: bool = False) -> duckdb.DuckDBPyConnection:
        # persistent secrets would make httpfs read ~/.duckdb/stored_secrets, which the
        # sandbox below forbids
        conn = duckdb.connect(":memory:", config={"allow_persistent_secrets": False})
        conn.execute(f"SET threads={int(settings.DUCKDB_THREADS)}")
        conn.execute(f"SET memory_limit={quote_literal(settings.DUCKDB_MEMORY_LIMIT)}")
        for extension in EXTENSIONS:
            try:
                conn.execute(f"LOAD {extension}")
            except duckdb.Error:
                conn.execute(f"INSTALL {extension}")
                conn.execute(f"LOAD {extension}")
        # reuse connections across the many range requests a parquet scan needs
        conn.execute("SET httpfs_connection_caching=true")
        if metadata_cache:
            conn.execute("SET enable_http_metadata_cache=true")
            conn.execute("SET parquet_metadata_cache=true")
        if settings.has_s3_credentials:
            # only needed for a private bucket, public files are read over plain HTTPS
            conn.execute(
                f"""
                CREATE OR REPLACE SECRET tabular_s3 (
                    TYPE s3,
                    KEY_ID {quote_literal(settings.S3_ACCESS_KEY)},
                    SECRET {quote_literal(settings.S3_SECRET_KEY)},
                    ENDPOINT {quote_literal(settings.S3_ENDPOINT)},
                    REGION {quote_literal(settings.S3_REGION)},
                    URL_STYLE {quote_literal(settings.S3_URL_STYLE)},
                    USE_SSL {str(settings.S3_USE_SSL).lower()}
                )
                """
            )
        return conn

    @staticmethod
    def _sandbox(conn: duckdb.DuckDBPyConnection) -> None:
        """Cut off every external access except the parquet prefix, then lock the config.

        `allowed_directories` keeps the resource prefix readable while
        `enable_external_access=false` denies everything else: local files, other prefixes of the
        same bucket, other hosts and extension installation. `lock_configuration` then prevents
        any statement from loosening this.
        """
        prefix = settings.PARQUET_BASE_URL.rstrip("/")
        conn.execute(f"SET allowed_directories=[{quote_literal(prefix)}]")
        conn.execute("SET enable_external_access=false")
        conn.execute("SET lock_configuration=true")

    def _new_main_connection(self) -> duckdb.DuckDBPyConnection:
        """Connection used by the data endpoints, sandboxed on the parquet prefix as well."""
        conn = self._new_connection(metadata_cache=settings.DUCKDB_METADATA_CACHE)
        self._sandbox(conn)
        return conn

    def _new_sql_connection(self) -> duckdb.DuckDBPyConnection:
        """Connection dedicated to the user-provided SQL endpoint.

        Metadata caching is never enabled here: these connections cannot be reconfigured, so
        they would keep serving stale parquet footers after a file is replaced.
        """
        conn = self._new_connection(metadata_cache=False)
        conn.execute("SET autoinstall_known_extensions=false")
        conn.execute("SET autoload_known_extensions=false")
        conn.execute("SET max_expression_depth=500")
        self._sandbox(conn)
        return conn

    async def start(self) -> None:
        self._conn = await asyncio.to_thread(self._new_main_connection)
        pool: asyncio.Queue[duckdb.DuckDBPyConnection] = asyncio.Queue()
        for _ in range(max(1, settings.SQL_POOL_SIZE)):
            pool.put_nowait(await asyncio.to_thread(self._new_sql_connection))
        self._sql_pool = pool

    async def stop(self) -> None:
        if self._sql_pool is not None:
            while not self._sql_pool.empty():
                self._sql_pool.get_nowait().close()
            self._sql_pool = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise QueryException(503, None, "DB unavailable", "DuckDB is not initialized yet")
        return self._conn

    # --- queries -------------------------------------------------------------------

    async def query(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> tuple[list[str], list[tuple]]:
        """Run a query on a dedicated cursor and return (column names, rows)."""

        def run() -> tuple[list[str], list[tuple]]:
            cursor = self.connection.cursor()
            try:
                cursor.execute(sql, list(params or []))
                columns = [description[0] for description in cursor.description or []]
                return columns, cursor.fetchall()
            finally:
                cursor.close()

        return await asyncio.to_thread(run)

    async def query_value(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        _, rows = await self.query(sql, params)
        return rows[0][0] if rows else None

    async def stream(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> AsyncIterator[tuple[list[str], list[tuple]]]:
        """Yield (column names, rows) chunks, keeping a cursor open across chunks."""
        cursor = self.connection.cursor()
        try:
            await asyncio.to_thread(cursor.execute, sql, list(params or []))
            columns = [description[0] for description in cursor.description or []]
            while True:
                rows = await asyncio.to_thread(cursor.fetchmany, settings.STREAM_CHUNK_SIZE)
                if not rows:
                    break
                yield columns, rows
        finally:
            await asyncio.to_thread(cursor.close)

    # --- user SQL ------------------------------------------------------------------

    @asynccontextmanager
    async def sql_connection(self) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        """Borrow a locked-down connection from the pool for the /sql/ endpoint."""
        if self._sql_pool is None:
            raise QueryException(503, None, "DB unavailable", "DuckDB is not initialized yet")
        try:
            # a few slow queries can hold every connection, the others must not queue forever
            conn = await asyncio.wait_for(self._sql_pool.get(), settings.SQL_TIMEOUT)
        except TimeoutError:
            raise QueryException(
                503,
                None,
                "SQL endpoint busy",
                f"No SQL connection got available within {settings.SQL_TIMEOUT} seconds, "
                "retry later",
            ) from None
        try:
            yield conn
        finally:
            self._sql_pool.put_nowait(await asyncio.to_thread(self._recycle, conn))

    def _recycle(self, conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
        """Make sure a connection is still usable before putting it back in the pool."""
        try:
            conn.execute("SELECT 1").fetchall()
            return conn
        except duckdb.Error:
            log.warning("recycling a broken SQL connection")
            try:
                conn.close()
            except duckdb.Error:
                pass
            return self._new_sql_connection()

    @staticmethod
    def run_with_timeout(
        conn: duckdb.DuckDBPyConnection, sql: str, timeout: int
    ) -> tuple[list[str], list[tuple]]:
        """Blocking execution interrupted after `timeout` seconds (run in a thread)."""
        watchdog = threading.Timer(timeout, conn.interrupt)
        watchdog.start()
        try:
            cursor = conn.execute(sql)
            columns = [description[0] for description in cursor.description or []]
            return columns, cursor.fetchall()
        finally:
            watchdog.cancel()


engine = DuckDBEngine()
