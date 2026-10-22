import asyncio
import logging
import threading
from collections.abc import AsyncIterator, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.errors import query_timeout, server_busy

T = TypeVar("T")

log = logging.getLogger(__name__)

# httpfs provides the s3:// filesystem used to read the parquet files
EXTENSIONS = ("httpfs",)

# where httpfs looks for the system CA bundle to verify HTTPS certificates: it reads it from disk
# on every TLS connection, so the sandbox must allow it (missing paths are ignored)
CA_BUNDLES = (
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/ssl/cert.pem",
)


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


def install_extensions() -> None:
    """Download the extensions once, so that runtime only has to LOAD them."""
    with duckdb.connect(":memory:") as conn:
        for extension in EXTENSIONS:
            conn.execute(f"INSTALL {extension}")


class DuckDBEngine:
    """Owns the DuckDB connections: one for the API queries, a locked-down pool for user SQL.

    Everything is set by `start()`, which the application lifespan runs before serving any
    request."""

    _conn: duckdb.DuckDBPyConnection
    _sql_pool: asyncio.Queue[duckdb.DuckDBPyConnection]
    # the data queries get their own threads: when they saturate, the default executor
    # stays free for /health/, which must keep answering
    _executor: ThreadPoolExecutor
    _slots: asyncio.Semaphore

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
        if settings.S3_ACCESS_KEY and settings.S3_SECRET_KEY:
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
        any statement from loosening this. The CA bundles in `allowed_paths` are public
        certificates, and user SQL cannot read files anyway.

        DuckDB resolves `..` in a local path before checking it, but not in a URL:
        `<prefix>/../x.parquet` passes the check and reaches another prefix of the bucket. No path
        built by the API can hold one, as the resource id must be a UUID and user SQL cannot name
        a file at all.
        """
        prefix = settings.PARQUET_BASE_URL.rstrip("/")
        conn.execute(f"SET allowed_directories=[{quote_literal(prefix)}]")
        conn.execute(f"SET allowed_paths=[{', '.join(map(quote_literal, CA_BUNDLES))}]")
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
        self._executor = ThreadPoolExecutor(
            max_workers=settings.QUERY_WORKERS, thread_name_prefix="duckdb"
        )
        self._slots = asyncio.Semaphore(settings.QUERY_WORKERS)
        self._sql_pool = asyncio.Queue()
        for _ in range(settings.SQL_POOL_SIZE):
            self._sql_pool.put_nowait(await asyncio.to_thread(self._new_sql_connection))

    async def stop(self) -> None:
        while not self._sql_pool.empty():
            self._sql_pool.get_nowait().close()
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._conn.close()

    # --- queries -------------------------------------------------------------------

    async def _run(self, cursor: duckdb.DuckDBPyConnection, call: Callable[[], T]) -> T:
        """Run a blocking call on the data threads, interrupted after QUERY_TIMEOUT."""
        timeout = settings.QUERY_TIMEOUT
        try:
            await asyncio.wait_for(self._slots.acquire(), timeout)
        except TimeoutError:
            raise server_busy(timeout) from None
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
            raise
        finally:
            self._slots.release()

    async def query(self, sql: str, params: Sequence[Any] | None = None) -> QueryResult:
        """Run a query on a dedicated cursor."""
        cursor = self._conn.cursor()
        try:
            return await self._run(cursor, lambda: fetch(cursor, sql, params))
        finally:
            cursor.close()

    async def query_value(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        rows = (await self.query(sql, params)).rows
        return rows[0][0] if rows else None

    async def stream(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> AsyncIterator[QueryResult]:
        """Yield the result in chunks, keeping a cursor open across chunks.

        The first chunk is yielded even when empty, so that a CSV export without any row still
        gets its header. The timeout applies to each chunk, not to the whole download, whose
        pace is the client's."""
        cursor = self._conn.cursor()

        async def fetch() -> list[tuple]:
            return await self._run(cursor, lambda: cursor.fetchmany(settings.STREAM_CHUNK_SIZE))

        try:
            await self._run(cursor, lambda: cursor.execute(sql, list(params or [])))
            columns = column_names(cursor)
            rows = await fetch()
            yield QueryResult(columns, rows)
            while rows:
                rows = await fetch()
                if rows:
                    yield QueryResult(columns, rows)
        finally:
            cursor.close()

    async def ping(self) -> None:
        """Liveness check, run on the default executor so that busy data threads don't fail it."""

        def run() -> None:
            cursor = self._conn.cursor()
            try:
                cursor.execute("SELECT 1").fetchall()
            finally:
                cursor.close()

        await asyncio.to_thread(run)

    # --- user SQL ------------------------------------------------------------------

    @asynccontextmanager
    async def sql_connection(self) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        """Borrow a locked-down connection from the pool for the /sql/ endpoint."""
        try:
            # a few slow queries can hold every connection, the others must not queue forever
            conn = await asyncio.wait_for(self._sql_pool.get(), settings.SQL_TIMEOUT)
        except TimeoutError:
            raise server_busy(settings.SQL_TIMEOUT) from None
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


engine = DuckDBEngine()
