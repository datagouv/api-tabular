"""
Core module for api_tabular.

This module contains the core business logic separated from the API layer.
It provides data access, query building, and exception handling functionality.
"""

from .data_access import DataAccessor
from .exceptions import QueryException, handle_exception
from .models import QueryResult, ResourceMetadata
from .query_builder import QueryBuilder

__all__ = [
    "DataAccessor",
    "QueryBuilder",
    "ResourceMetadata",
    "QueryResult",
    "QueryException",
    "handle_exception",
]
