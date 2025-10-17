from typing import AsyncGenerator

from aiohttp import ClientSession, web

from api_tabular import config
from api_tabular.core.data_access import DataAccessor
from api_tabular.error import handle_exception
from api_tabular.utils import process_total

# Global data accessor instance for backward compatibility
_data_accessor = None


def _get_data_accessor(session: ClientSession) -> DataAccessor:
    """Get or create a DataAccessor instance for the session."""
    global _data_accessor
    if _data_accessor is None:
        _data_accessor = DataAccessor(session)
    return _data_accessor


async def get_resource(session: ClientSession, resource_id: str, columns: list) -> dict:
    """Get resource metadata by resource ID (backward compatible wrapper)."""
    return await _get_data_accessor(session).get_resource(resource_id, columns)


async def get_resource_data(
    session: ClientSession, resource: dict, sql_query: str
) -> tuple[list[dict], int | None]:
    """Get resource data using a SQL query (backward compatible wrapper)."""
    return await _get_data_accessor(session).get_resource_data(resource, sql_query)


async def get_resource_data_streamed(
    session: ClientSession,
    model: dict,
    sql_query: str,
    accept_format: str = "text/csv",
    batch_size: int = config.BATCH_SIZE,
) -> AsyncGenerator[bytes, None]:
    """Get resource data as a stream (backward compatible wrapper)."""
    async for chunk in _get_data_accessor(session).get_resource_data_streamed(
        model, sql_query, accept_format, batch_size
    ):
        yield chunk


async def get_potential_indexes(session: ClientSession, resource_id: str) -> set[str] | None:
    """Get potential indexes for a resource (backward compatible wrapper)."""
    return await _get_data_accessor(session).get_potential_indexes(resource_id)
