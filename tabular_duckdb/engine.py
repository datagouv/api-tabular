import asyncio
import tempfile
import threading
from collections.abc import AsyncGenerator, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.errors import query_timeout, server_busy

T = TypeVar("T")

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


class DuckDBEngine:
    """Owns the single DuckDB instance of the process.

    The instance is sandboxed, then its configuration is locked, and its database is an empty
    file opened read-only, so that no statement can change its catalog. Secrets are the
    exception: they belong to the instance, and `CREATE SECRET` on one connection is seen by all;
    only the single SELECT rule of /sql/ keeps users from running one. Every query runs on its own
    connection (`cursor()`), whose temporary objects (views, tables, macros) disappear when it is
    closed, while the HTTP connections and the caches of the instance are shared by all.

    Everything is set by `start()`, which the application lifespan runs before serving any
    request."""

    _db: duckdb.DuckDBPyConnection
    _directory: tempfile.TemporaryDirectory
    # the queries get their own threads: when they saturate, the default executor stays free
    # for /health/, which must keep answering
    _executor: ThreadPoolExecutor
    _slots: asyncio.Semaphore
    # an export only holds a slot while it fetches a chunk, but its result stays open in between,
    # at the pace of the client: the number of open exports is bounded on its own
    _exports: asyncio.Semaphore

    # --- lifecycle -----------------------------------------------------------------

    def _open(self) -> duckdb.DuckDBPyConnection:
        # an in-memory database cannot be opened read-only, an empty file can
        path = Path(self._directory.name) / "empty.duckdb"
        duckdb.connect(str(path)).close()
        # persistent secrets would make httpfs read ~/.duckdb/stored_secrets, which the
        # sandbox below forbids
        db = duckdb.connect(str(path), read_only=True, config={"allow_persistent_secrets": False})
        # GLOBAL everywhere: a plain SET of some options (http_timeout...) only applies to this
        # connection, while every query runs on a cursor of its own
        db.execute(f"SET GLOBAL threads={int(settings.DUCKDB_THREADS)}")
        db.execute(f"SET GLOBAL memory_limit={quote_literal(settings.DUCKDB_MEMORY_LIMIT)}")
        # httpfs reads the parquet files over HTTP(S); the other extensions in use (parquet, icu,
        # json) are built into DuckDB. INSTALL does nothing when the image already installed it
        db.execute("INSTALL httpfs")
        db.execute("LOAD httpfs")
        db.execute("SET GLOBAL autoinstall_known_extensions=false")
        db.execute("SET GLOBAL autoload_known_extensions=false")
        db.execute("SET GLOBAL max_expression_depth=500")
        # the dates of the files (`created_at`), whatever the time zone of the server
        db.execute("SET GLOBAL TimeZone='UTC'")
        # reuse connections across the many range requests a parquet scan needs
        db.execute("SET GLOBAL httpfs_connection_caching=true")
        # interrupting a query does not cut an HTTP request blocked on the network: httpfs must
        # give up by itself within QUERY_TIMEOUT. Without retries: even with http_retries=1, a
        # 503 is retried seven times with a doubling backoff, holding a query thread for 30 s
        db.execute("SET GLOBAL http_retries=0")
        db.execute(f"SET GLOBAL http_timeout={settings.QUERY_TIMEOUT}")
        if settings.DUCKDB_METADATA_CACHE:
            db.execute("SET GLOBAL enable_http_metadata_cache=true")
            db.execute("SET GLOBAL parquet_metadata_cache=true")
        self._sandbox(db)
        return db

    @staticmethod
    def _sandbox(db: duckdb.DuckDBPyConnection) -> None:
        """Cut off every external access except the parquet prefix, then lock the config.

        `allowed_directories` keeps the resource prefix readable while
        `enable_external_access=false` denies everything else: local files, other hosts,
        attaching databases and installing extensions. `lock_configuration` then prevents any
        statement from loosening this. The CA bundles in `allowed_paths` are public certificates.

        DuckDB does not resolve `..` in a URL before this check: user SQL can read any object of
        the bucket, not only those under the prefix, which is what anyone can already download
        from this public bucket.
        """
        prefix = settings.PARQUET_BASE_URL.rstrip("/")
        db.execute(f"SET GLOBAL allowed_directories=[{quote_literal(prefix)}]")
        db.execute(f"SET GLOBAL allowed_paths=[{', '.join(map(quote_literal, CA_BUNDLES))}]")
        db.execute("SET GLOBAL enable_external_access=false")
        db.execute("SET GLOBAL lock_configuration=true")

    async def start(self) -> None:
        self._directory = tempfile.TemporaryDirectory(prefix="tabular-duckdb-")
        self._db = await asyncio.to_thread(self._open)
        self._executor = ThreadPoolExecutor(
            max_workers=settings.QUERY_WORKERS, thread_name_prefix="duckdb"
        )
        self._slots = asyncio.Semaphore(settings.QUERY_WORKERS)
        self._exports = asyncio.Semaphore(settings.QUERY_WORKERS)

    async def stop(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._db.close()
        self._directory.cleanup()

    # --- queries -------------------------------------------------------------------

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A connection of its own: in a `with`, closed afterwards with everything it created."""
        return self._db.cursor()

    async def run(
        self,
        cursor: duckdb.DuckDBPyConnection,
        call: Callable[[], T],
        timeout: float | None = None,
    ) -> T:
        """Run a blocking call on the query threads, interrupted after `timeout` seconds."""
        timeout = timeout or settings.QUERY_TIMEOUT
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
            # the interrupted query failed in its thread: read that failure, which asyncio
            # would otherwise log as an error, and Sentry report, for every client going away
            future.exception()
            raise
        finally:
            self._slots.release()

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
        try:
            await asyncio.wait_for(self._exports.acquire(), settings.QUERY_TIMEOUT)
        except TimeoutError:
            raise server_busy(settings.QUERY_TIMEOUT) from None
        try:
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
        finally:
            self._exports.release()

    async def ping(self) -> None:
        """Liveness check, run on the default executor so that busy query threads don't fail it."""

        def run() -> None:
            with self.cursor() as cursor:
                cursor.execute("SELECT 1").fetchall()

        await asyncio.to_thread(run)


engine = DuckDBEngine()
