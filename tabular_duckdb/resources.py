import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, NotRequired, TypedDict

import duckdb
from starlette.exceptions import HTTPException

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine, quote_literal
from tabular_duckdb.errors import QueryException, handle_exception, safe_message
from tabular_duckdb.query import ID_COLUMN, python_type, quote_ident

RESOURCE_ID_PATTERN = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

# key-value metadata where hydra embeds the csv-detective inspection of the source file
INSPECTION_KEY = "csv_detective"


@dataclass
class Resource:
    resource_id: str
    created_at: str | None
    columns: dict[str, str] = field(default_factory=dict)
    # the csv-detective inspection, absent from the files converted before hydra embedded it
    inspection: dict[str, Any] | None = None

    @property
    def query_columns(self) -> dict[str, str]:
        """Columns available in queries, including the synthetic row id."""
        return {ID_COLUMN: "BIGINT", **self.columns}

    @property
    def url_literal(self) -> str:
        """The URL of the parquet file, as a SQL string literal."""
        return quote_literal(settings.object_url(self.resource_id))

    @property
    def source_sql(self) -> str:
        """Subquery exposing the parquet file plus the __id column."""
        return (
            f"(SELECT * EXCLUDE (file_row_number), file_row_number + 1 AS {ID_COLUMN} "
            f"FROM read_parquet({self.url_literal}, file_row_number = true))"
        )


def is_storage_failure(exc: duckdb.Error) -> bool:
    """The storage failed to serve a file, as opposed to a missing file or a client error."""
    if isinstance(exc, duckdb.HTTPException):
        # the HTTP status of the answer, set on the instance by DuckDB
        return getattr(exc, "status_code", 500) >= 500
    return isinstance(exc, duckdb.IOException)


def is_missing_file(exc: duckdb.Error) -> bool:
    # a bucket that does not grant listing answers 403 instead of 404 for a missing object
    return isinstance(exc, duckdb.HTTPException) and getattr(exc, "status_code", None) in (403, 404)


def read_error(exc: duckdb.Error, resource_id: str | None = None) -> Exception:
    """DuckDB cannot read the file: missing resource, failing storage, or unusable parquet."""
    if is_missing_file(exc):
        # also a parquet file deleted while its resource was still in the cache
        return HTTPException(404)
    if is_storage_failure(exc):
        return handle_exception(
            502,
            "Storage error",
            f"The storage failed while reading the parquet file: {safe_message(str(exc))}",
            resource_id,
        )
    return QueryException(
        400,
        "Invalid resource file",
        f"Could not read the parquet file: {safe_message(str(exc))}",
    )


@dataclass(frozen=True)
class CacheEntry:
    # on the time.monotonic() clock
    expires_at: float
    resource: Resource


class ResourceCache:
    """Small TTL cache: the parquet schema of a resource does not change often."""

    def __init__(self) -> None:
        self._entries: dict[str, CacheEntry] = {}
        # the loads in progress: concurrent requests on a resource share its load, and its
        # failure, without waiting on the (remote, possibly slow) loads of other resources
        self._loading: dict[str, asyncio.Task[Resource]] = {}

    async def get(self, resource_id: str) -> Resource:
        if not RESOURCE_ID_PATTERN.fullmatch(resource_id):
            raise HTTPException(404)
        cached = self._entries.get(resource_id)
        if cached and cached.expires_at > time.monotonic():
            return cached.resource
        task = self._loading.get(resource_id)
        if task is None:
            task = asyncio.create_task(self._load_and_cache(resource_id))
            self._loading[resource_id] = task
            task.add_done_callback(lambda _: self._loading.pop(resource_id, None))
        # a client going away must not cancel the load the others are waiting for
        return await asyncio.shield(task)

    async def _load_and_cache(self, resource_id: str) -> Resource:
        resource = await self._load(resource_id)
        now = time.monotonic()
        # an expired entry is only replaced when its resource is requested again: without this
        # purge, every resource ever requested would stay in memory
        self._entries = {
            key: entry for key, entry in self._entries.items() if entry.expires_at > now
        }
        self._entries[resource_id] = CacheEntry(
            expires_at=now + settings.RESOURCE_CACHE_TTL, resource=resource
        )
        return resource

    async def _load(self, resource_id: str) -> Resource:
        url_literal = quote_literal(settings.object_url(resource_id))
        try:
            # a single HEAD, which reads no content: whether the file exists, and its date
            last_modified = await engine.query_value(
                f"SELECT last_modified FROM read_blob({url_literal})"
            )
            described = await engine.query(f"DESCRIBE SELECT * FROM read_parquet({url_literal})")
            embedded = await engine.query_value(
                f"SELECT value FROM parquet_kv_metadata({url_literal}) WHERE decode(key) = ?",
                [INSPECTION_KEY],
            )
        except duckdb.Error as exc:
            raise read_error(exc, resource_id) from exc
        return Resource(
            resource_id=resource_id,
            created_at=last_modified.isoformat() if last_modified else None,
            columns={row[0]: row[1] for row in described.rows},
            inspection=json.loads(embedded) if embedded is not None else None,
        )


resource_cache = ResourceCache()


class ColumnProfile(TypedDict):
    python_type: str
    format: str
    # only in the csv-detective inspection
    score: NotRequired[float]


class ColumnStats(TypedDict):
    # rendered as strings by SUMMARIZE, whatever the column type
    min: str | None
    max: str | None
    mean: float | None
    std: float | None
    nb_distinct: int | None
    nb_missing_values: int | None


def profile_columns(resource: Resource) -> dict[str, ColumnProfile]:
    if resource.inspection:
        return resource.inspection["columns"]
    # without the inspection, the types are guessed from the parquet schema
    return {
        name: ColumnProfile(python_type=python_type(duckdb_type), format=python_type(duckdb_type))
        for name, duckdb_type in resource.columns.items()
    }


async def build_profile(resource: Resource) -> dict:
    """The csv-detective inspection, or a profile derived from the parquet file without it."""
    if resource.inspection:
        return resource.inspection
    columns = profile_columns(resource)
    formats: dict[str, list[str]] = {}
    for name, description in columns.items():
        formats.setdefault(description["format"], []).append(name)

    # the footer has the row count: counting the rows by scanning the file takes longer than
    # QUERY_TIMEOUT on the biggest resources
    try:
        total_lines = await engine.query_value(
            f"SELECT num_rows FROM parquet_file_metadata({resource.url_literal})"
        )
    except duckdb.Error as exc:
        raise read_error(exc, resource.resource_id) from exc

    with_stats = settings.PROFILE_STATS and bool(resource.columns)
    profile = await summarize(resource) if with_stats else {}

    return {
        "header": list(resource.columns.keys()),
        "total_lines": total_lines,
        "columns": columns,
        "formats": formats,
        "profile": profile,
    }


def _as_float(value: Any) -> float | None:
    """SUMMARIZE returns avg/std as strings, and NULL for non-numeric columns."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def summarize(resource: Resource) -> dict[str, ColumnStats]:
    """Per-column statistics, computed by DuckDB's SUMMARIZE."""
    source = f"read_parquet({resource.url_literal})"
    try:
        # the null counts of the footer, exact where SUMMARIZE only gives a rounded percentage;
        # a writer that omits them leaves them unknown (None)
        null_counts = await engine.query(
            "SELECT path_in_schema, sum(stats_null_count) "
            f"FROM parquet_metadata({resource.url_literal}) GROUP BY ALL"
        )
        missing_values: dict[str, int | None] = dict(null_counts.rows)
        # the footer only has null counts for the leaves of a nested column, not for its rows
        nested = [name for name in resource.columns if name not in missing_values]
        summary = await engine.query(f"SUMMARIZE SELECT * FROM {source}")
        if nested:
            counts_sql = ", ".join(f"count(*) - count({quote_ident(name)})" for name in nested)
            counts = await engine.query(f"SELECT {counts_sql} FROM {source}")
            missing_values = {**missing_values, **dict(zip(nested, counts.rows[0]))}
    except (duckdb.Error, QueryException):
        # statistics are a bonus: a file too big to summarize in time still gets its profile
        return {}
    stats: dict[str, ColumnStats] = {}
    for row in (dict(zip(summary.columns, values)) for values in summary.rows):
        name = row["column_name"]
        stats[name] = ColumnStats(
            min=row["min"],
            max=row["max"],
            mean=_as_float(row["avg"]),
            std=_as_float(row["std"]),
            nb_distinct=row["approx_unique"],
            nb_missing_values=missing_values.get(name),
        )
    return stats
