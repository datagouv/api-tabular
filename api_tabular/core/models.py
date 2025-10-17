"""
Data models for the core module.

This module contains data classes and models used throughout the application.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ResourceMetadata:
    """Represents metadata for a resource."""

    resource_id: str
    created_at: str
    url: str
    dataset_id: Optional[str] = None
    deleted_at: Optional[str] = None
    parsing_table: Optional[str] = None
    csv_detective: Optional[Dict[str, Any]] = None


@dataclass
class QueryResult:
    """Represents the result of a data query."""

    data: List[Dict[str, Any]]
    meta: Dict[str, Any]
    links: Dict[str, Any]
    total: Optional[int] = None
