import asyncio
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Any

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine, quote_literal, rows_as_dicts
from tabular_duckdb.errors import QueryException
from tabular_duckdb.query import ID_COLUMN

RESOURCE_ID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# csv-detective python_type, as expected by the swagger builder
PYTHON_TYPES: dict[str, str] = {
    "BOOLEAN": "bool",
    "TINYINT": "int",
    "SMALLINT": "int",
    "INTEGER": "int",
    "BIGINT": "int",
    "HUGEINT": "int",
    "UTINYINT": "int",
    "USMALLINT": "int",
    "UINTEGER": "int",
    "UBIGINT": "int",
    "UHUGEINT": "int",
    "FLOAT": "float",
    "REAL": "float",
    "DOUBLE": "float",
    "DATE": "date",
    "TIME": "string",
    "TIMESTAMP": "datetime",
    "TIMESTAMP WITH TIME ZONE": "datetime",
    "TIMESTAMP_S": "datetime",
    "TIMESTAMP_MS": "datetime",
    "TIMESTAMP_NS": "datetime",
    "INTERVAL": "string",
    "VARCHAR": "string",
    "UUID": "string",
    "BLOB": "string",
    "JSON": "json",
}

JSON_LIKE_PREFIXES = ("STRUCT", "MAP", "UNION", "JSON")


def python_type(duckdb_type: str) -> str:
    """Map a DuckDB column type to the csv-detective python_type vocabulary."""
    normalized = duckdb_type.upper()
    if normalized.endswith("[]") or normalized.startswith(JSON_LIKE_PREFIXES):
        return "json"
    if normalized.startswith("DECIMAL"):
        return "float"
    return PYTHON_TYPES.get(normalized, "string")


@dataclass
class Resource:
    resource_id: str
    created_at: str | None
    total_lines: int
    columns: dict[str, str] = field(default_factory=dict)

    @property
    def object_url(self) -> str:
        return settings.object_url(self.resource_id)

    @property
    def query_columns(self) -> dict[str, str]:
        """Columns available in queries, including the synthetic row id."""
        return {ID_COLUMN: "BIGINT", **self.columns}

    @property
    def source_sql(self) -> str:
        """Subquery exposing the parquet file plus the __id column."""
        return (
            f"(SELECT * EXCLUDE (file_row_number), file_row_number + 1 AS {ID_COLUMN} "
            f"FROM read_parquet({quote_literal(self.object_url)}, file_row_number = true))"
        )


def validate_resource_id(resource_id: str) -> str:
    if not RESOURCE_ID_PATTERN.match(resource_id):
        raise QueryException(404, None, "Resource not found", f"Unknown resource {resource_id}")
    return resource_id


def head(url: str) -> dict[str, str]:
    """HTTP HEAD on the object, used both to check existence and to read its date."""
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=settings.HTTP_TIMEOUT) as response:
            return dict(response.headers)
    except urllib.error.HTTPError as exc:
        # a bucket that does not grant listing answers 403 instead of 404 for a missing object
        if exc.code in (403, 404):
            raise QueryException(
                404, None, "Resource not found", f"No parquet file at {url}"
            ) from exc
        raise QueryException(
            502, None, "Storage error", f"Unexpected HTTP {exc.code} for {url}"
        ) from exc
    except urllib.error.URLError as exc:
        raise QueryException(503, None, "Storage unavailable", f"{url} is unreachable: {exc}")


def _created_at(url: str) -> str | None:
    """Last modification date of the object, when the storage is reachable over HTTP."""
    if not url.startswith(("http://", "https://")):
        # an s3:// base URL is only reachable through DuckDB, we have no date to report
        return None
    last_modified = head(url).get("Last-Modified")
    if not last_modified:
        return None
    try:
        return parsedate_to_datetime(last_modified).isoformat()
    except (TypeError, ValueError):
        return None


def _read_error(resource_id: str, object_url: str, exc: duckdb.Error) -> QueryException:
    """DuckDB cannot read the file: missing resource or genuinely unusable parquet."""
    if isinstance(exc, (duckdb.HTTPException, duckdb.IOException)) and not object_url.startswith(
        ("http://", "https://")
    ):
        # no HEAD was possible on an s3:// URL, so an IO error most likely means "not found"
        return QueryException(
            404, None, "Resource not found", f"No readable parquet file for {resource_id}"
        )
    return QueryException(
        400, None, "Invalid resource file", f"Could not read the parquet file: {exc}"
    )


class ResourceCache:
    """Small TTL cache: the parquet schema of a resource does not change often."""

    def __init__(self) -> None:
        self._entries: dict[str, tuple[float, Resource]] = {}
        # one lock per resource: concurrent requests on a resource load it once, without
        # waiting on the (remote, possibly slow) loads of other resources
        self._locks: dict[str, asyncio.Lock] = {}

    def invalidate(self) -> None:
        self._entries.clear()

    async def get(self, resource_id: str) -> Resource:
        validate_resource_id(resource_id)
        cached = self._entries.get(resource_id)
        now = time.monotonic()
        if cached and cached[0] > now:
            return cached[1]
        lock = self._locks.setdefault(resource_id, asyncio.Lock())
        try:
            async with lock:
                cached = self._entries.get(resource_id)
                if cached and cached[0] > time.monotonic():
                    return cached[1]
                resource = await self._load(resource_id)
                self._entries[resource_id] = (
                    time.monotonic() + settings.RESOURCE_CACHE_TTL,
                    resource,
                )
                return resource
        finally:
            # unknown resource ids must not pile up locks
            if not lock.locked():
                self._locks.pop(resource_id, None)

    async def _load(self, resource_id: str) -> Resource:
        object_url = settings.object_url(resource_id)
        # over HTTP this also tells us whether the resource exists at all
        created_at = await asyncio.to_thread(_created_at, object_url)
        try:
            _, described = await engine.query(
                f"DESCRIBE SELECT * FROM read_parquet({quote_literal(object_url)})"
            )
            total_lines = await engine.query_value(
                f"SELECT count(*) FROM read_parquet({quote_literal(object_url)})"
            )
        except duckdb.Error as exc:
            raise _read_error(resource_id, object_url, exc) from exc
        columns = {row[0]: row[1] for row in described}
        return Resource(
            resource_id=resource_id,
            created_at=created_at,
            total_lines=int(total_lines or 0),
            columns=columns,
        )


resource_cache = ResourceCache()


async def get_resource(resource_id: str) -> Resource:
    return await resource_cache.get(resource_id)


def profile_columns(resource: Resource) -> dict[str, dict[str, Any]]:
    return {
        name: {"python_type": python_type(duckdb_type), "format": python_type(duckdb_type)}
        for name, duckdb_type in resource.columns.items()
    }


async def build_profile(resource: Resource) -> dict:
    """Build a csv-detective-like profile out of the parquet file itself."""
    columns = profile_columns(resource)
    formats: dict[str, list[str]] = {}
    for name, description in columns.items():
        formats.setdefault(description["format"], []).append(name)

    profile: dict[str, dict[str, Any]] = {}
    if settings.PROFILE_STATS and resource.columns:
        profile = await summarize(resource)

    return {
        "header": list(resource.columns.keys()),
        "total_lines": resource.total_lines,
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


async def summarize(resource: Resource) -> dict[str, dict[str, Any]]:
    """Per-column statistics, computed by DuckDB's SUMMARIZE."""
    try:
        columns, rows = await engine.query(f"SUMMARIZE SELECT * FROM {resource.source_sql}")
    except duckdb.Error:
        return {}
    stats: dict[str, dict[str, Any]] = {}
    for row in rows_as_dicts(columns, rows):
        name = row.get("column_name")
        if name in (None, ID_COLUMN):
            continue
        null_percentage = _as_float(row.get("null_percentage")) or 0.0
        stats[name] = {
            "min": row.get("min"),
            "max": row.get("max"),
            "mean": _as_float(row.get("avg")),
            "std": _as_float(row.get("std")),
            "nb_distinct": row.get("approx_unique"),
            "nb_missing_values": round(null_percentage * resource.total_lines / 100),
        }
    return stats
