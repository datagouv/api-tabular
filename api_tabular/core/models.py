"""
Data models for the core module.

This module contains data classes and models used throughout the application.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ResourceMetadata:
    """Represents metadata for a resource."""

    resource_id: str
    created_at: str
    url: str
    dataset_id: str | None = None
    deleted_at: str | None = None
    parsing_table: str | None = None
    csv_detective: dict[str, Any] | None = None


@dataclass
class QueryResult:
    """Represents the result of a data query."""

    data: list[dict[str, Any]]
    meta: dict[str, Any]
    links: dict[str, Any]
    total: int | None = None
