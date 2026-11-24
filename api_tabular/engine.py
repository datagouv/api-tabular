import asyncio
import threading
from collections.abc import AsyncGenerator, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

import duckdb

from api_tabular.config import settings
from api_tabular.errors import query_timeout, server_busy

T = TypeVar("T")


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[tuple]


def column_names(cursor: duckdb.DuckDBPyConnection) -> list[str]:
    return [description[0] for description in cursor.description or []]


def fetch(
    conn: duckdb.DuckDBPyConnection, sql: str, params: Sequence[Any] | None = None
) -> QueryResult:
    conn.execute(sql, list(params or []))
    return QueryResult(column_names(conn), conn.fetchall())


def interruptible(conn: duckdb.DuckDBPyConnection, call: Callable[[], T], timeout: float) -> T:
    """Blocking call, interrupted with a `duckdb.InterruptException` after `timeout` seconds."""
    watchdog = threading.Timer(timeout, conn.interrupt)
    watchdog.start()
    try:
        return call()
    finally:
        watchdog.cancel()


class DuckDBEngine:
    """Owns the single DuckDB instance of the process.

    Every query runs on its own connection (`cursor()`), while the HTTP connections and the caches
    of the instance are shared by all.

    Everything is set by `start()`, which the application lifespan runs before serving any
    request."""

    _db: duckdb.DuckDBPyConnection
    # the queries get their own threads: when they saturate, the default executor stays free
    # for /health/, which must keep answering
    _executor: ThreadPoolExecutor
    _slots: asyncio.Semaphore
    # an export only holds a slot while it fetches a chunk, but its result stays open in between,
    # at the pace of the client: the number of open exports is bounded on its own
    _exports: asyncio.Semaphore

    # --- lifecycle -----------------------------------------------------------------

    @staticmethod
    def _open() -> duckdb.DuckDBPyConnection:
        db = duckdb.connect()
        # GLOBAL everywhere: a plain SET of some options (http_timeout...) only applies to this
        # connection, while every query runs on a cursor of its own
        db.execute(f"SET GLOBAL threads={int(settings.DUCKDB_THREADS)}")
        db.execute(f"SET GLOBAL memory_limit={quote_literal(settings.DUCKDB_MEMORY_LIMIT)}")
        db.execute(f"SET GLOBAL temp_directory={quote_literal(settings.DUCKDB_TEMP_DIRECTORY)}")
        # httpfs reads the parquet files over HTTP(S); the other extensions in use (parquet, icu,
        # json) are built into DuckDB. INSTALL does nothing when the image already installed it
        db.execute("INSTALL httpfs")
        db.execute("LOAD httpfs")
        db.execute("SET GLOBAL max_expression_depth=500")
        # the default, which the unsorted pagination relies on to return rows in __id order
        db.execute("SET GLOBAL preserve_insertion_order=true")
        # under this LIMIT, a filtered page reads the filtered columns, then the others for the
        # rows it keeps only (instead of for every scanned row): 50 by default, below a full page
        db.execute(f"SET GLOBAL late_materialization_max_rows={int(settings.PAGE_SIZE_MAX)}")
        # the dates of the files (`created_at`), whatever the time zone of the server
        db.execute("SET GLOBAL TimeZone='UTC'")
        # reuse connections across the many range requests a parquet scan needs
        db.execute("SET GLOBAL httpfs_connection_caching=true")
        # interrupting a query does not cut an HTTP request blocked on the network: httpfs must
        # give up by itself within QUERY_TIMEOUT_SECONDS. Without retries: even with
        # http_retries=1, a 503 is retried seven times with a doubling backoff, holding a query
        # thread for 30 s
        db.execute("SET GLOBAL http_retries=0")
        db.execute(f"SET GLOBAL http_timeout={settings.QUERY_TIMEOUT_SECONDS}")
        # a cached footer is checked against the Last-Modified of a HEAD on every query, which
        # tells a file replaced in the bucket; the HTTP metadata cache saves that HEAD, at the cost
        # of reading a replaced file with the size of the old one
        db.execute("SET GLOBAL parquet_metadata_cache=true")
        if settings.DUCKDB_ENABLE_HTTP_METADATA_CACHE:
            db.execute("SET GLOBAL enable_http_metadata_cache=true")
        return db

    async def start(self) -> None:
        self._db = await asyncio.to_thread(self._open)
        self._executor = ThreadPoolExecutor(
            max_workers=settings.QUERY_WORKERS, thread_name_prefix="duckdb"
        )
        self._slots = asyncio.Semaphore(settings.QUERY_WORKERS)
        self._exports = asyncio.Semaphore(settings.QUERY_WORKERS)

    async def stop(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._db.close()

    # --- queries -------------------------------------------------------------------

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A connection of its own: in a `with`, closed afterwards with everything it created."""
        return self._db.cursor()

    @staticmethod
    @asynccontextmanager
    async def _hold(semaphore: asyncio.Semaphore) -> AsyncGenerator[None]:
        """A slot of the semaphore, or a 503 once QUERY_TIMEOUT_SECONDS is spent waiting for one."""
        try:
            await asyncio.wait_for(semaphore.acquire(), settings.QUERY_TIMEOUT_SECONDS)
        except TimeoutError:
            raise server_busy(settings.QUERY_TIMEOUT_SECONDS) from None
        try:
            yield
        finally:
            semaphore.release()

    async def run(self, cursor: duckdb.DuckDBPyConnection, call: Callable[[], T]) -> T:
        """Run a blocking call on the query threads, interrupted after QUERY_TIMEOUT_SECONDS."""
        timeout = settings.QUERY_TIMEOUT_SECONDS
        async with self._hold(self._slots):
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, interruptible, cursor, call, timeout
            )
            try:
                return await asyncio.shield(future)
            except duckdb.InterruptException as exc:
                raise query_timeout(timeout) from exc
            except asyncio.CancelledError:
                # the client went away: stop the query, and let the thread release the cursor
                # before the caller closes it
                cursor.interrupt()
                await asyncio.wait([future])
                # the interrupted query failed in its thread: read that failure, which asyncio
                # would otherwise log as an error, and Sentry report, for every client going away
                future.exception()
                raise

    async def query(self, sql: str, params: Sequence[Any] | None = None) -> QueryResult:
        with self.cursor() as cursor:
            return await self.run(cursor, lambda: fetch(cursor, sql, params))

    async def query_value(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        rows = (await self.query(sql, params)).rows
        return rows[0][0] if rows else None

    async def stream(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> AsyncGenerator[QueryResult]:
        """Yield the result in chunks, keeping a cursor open across chunks.

        The first chunk is yielded even when empty, so that a CSV export without any row still
        gets its header. The timeout applies to each chunk, not to the whole download, whose
        pace is the client's. At most QUERY_WORKERS exports are open at once."""
        async with self._hold(self._exports):
            with self.cursor() as cursor:

                async def fetch_chunk() -> list[tuple]:
                    return await self.run(
                        cursor, lambda: cursor.fetchmany(settings.STREAM_CHUNK_SIZE)
                    )

                await self.run(cursor, lambda: cursor.execute(sql, list(params or [])))
                columns = column_names(cursor)
                rows = await fetch_chunk()
                yield QueryResult(columns, rows)
                while rows:
                    rows = await fetch_chunk()
                    if rows:
                        yield QueryResult(columns, rows)

    async def ping(self) -> None:
        """Liveness check, run on the default executor so that busy query threads don't fail it."""

        def run() -> None:
            with self.cursor() as cursor:
                cursor.execute("SELECT 1").fetchall()

        await asyncio.to_thread(run)


engine = DuckDBEngine()
