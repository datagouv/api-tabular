"""
Data access layer for the core module.

This module contains the core data access logic extracted from query.py.
It provides methods for interacting with the database via PostgREST.
"""

from typing import AsyncGenerator, Optional, Set

from aiohttp import ClientSession, web

from .. import config
from .exceptions import handle_exception


class DataAccessor:
    """Handles data access operations for resources."""

    def __init__(self, session: ClientSession):
        self.session = session

    async def get_resource(self, resource_id: str, columns: list) -> dict:
        """
        Get resource metadata by resource ID.

        Args:
            resource_id: The ID of the resource to retrieve
            columns: List of columns to select

        Returns:
            Dictionary containing resource metadata

        Raises:
            web.HTTPNotFound: If resource is not found
            web.HTTPGone: If resource has been deleted
        """
        # Always include deleted_at and dataset_id for deletion checking, but don't duplicate them
        if "deleted_at" not in columns:
            columns.append("deleted_at")
        if "dataset_id" not in columns:
            columns.append("dataset_id")
        q = f"select={','.join(columns)}&resource_id=eq.{resource_id}&order=created_at.desc"
        url = f"{config.PGREST_ENDPOINT}/tables_index?{q}"
        async with self.session.get(url) as res:
            record = await res.json()
            if not res.ok:
                handle_exception(res.status, "Database error", record, resource_id)
            if not record:
                raise web.HTTPNotFound()
            if record[0].get("deleted_at") is not None:
                deleted_at: str = record[0]["deleted_at"]
                dataset_id: str | None = record[0].get("dataset_id")
                message = f"Resource {resource_id} has been permanently deleted on {deleted_at} by its producer."
                if dataset_id:
                    message += f" You can find more information about this resource at https://www.data.gouv.fr/datasets/{dataset_id}"
                else:
                    message += " Contact the resource producer to get more information."
                raise web.HTTPGone(text=message)
            return record[0]

    async def get_resource_data(
        self, resource: dict, sql_query: str
    ) -> tuple[list[dict], int | None]:
        """
        Get resource data using a SQL query.

        Args:
            resource: Resource metadata dictionary
            sql_query: SQL query string

        Returns:
            Tuple of (data records, total count)
        """
        headers = {"Prefer": "count=exact"}
        url = f"{config.PGREST_ENDPOINT}/{resource['parsing_table']}?{sql_query}"
        skip_total = False
        if any(f".{agg}()" in url for agg in ["count", "max", "min", "sum", "avg"]):
            # the total for aggregated data is wrong, it is always the length of the original table
            skip_total = True
        async with self.session.get(url, headers=headers) as res:
            if not res.ok:
                handle_exception(res.status, "Database error", await res.json(), resource.get("id"))
            record = await res.json()
            from ..utils import process_total

            total = process_total(res) if not skip_total else None
            return record, total

    async def get_resource_data_streamed(
        self,
        model: dict,
        sql_query: str,
        accept_format: str = "text/csv",
        batch_size: int = config.BATCH_SIZE,
    ) -> AsyncGenerator[bytes, None]:
        """
        Get resource data as a stream.

        Args:
            model: Resource metadata dictionary
            sql_query: SQL query string
            accept_format: MIME type for the response
            batch_size: Number of records per batch

        Yields:
            Bytes of data in the specified format
        """
        url = f"{config.PGREST_ENDPOINT}/{model['parsing_table']}?{sql_query}"
        res = await self.session.head(f"{url}&limit=1&", headers={"Prefer": "count=exact"})
        if not res.ok:
            handle_exception(res.status, "Database error", await res.json(), None)
        from ..utils import process_total

        total = process_total(res)
        for i in range(0, total, batch_size):
            async with self.session.get(
                url=f"{url}&limit={batch_size}&offset={i}", headers={"Accept": accept_format}
            ) as res:
                if not res.ok:
                    handle_exception(res.status, "Database error", await res.json(), None)
                async for chunk in res.content.iter_chunked(1024):
                    yield chunk
                yield b"\n"

    async def get_potential_indexes(self, resource_id: str) -> set[str] | None:
        """
        Get potential indexes for a resource.

        Args:
            resource_id: The ID of the resource

        Returns:
            Set of index names or None if no indexes found
        """
        q = f"select=table_indexes&resource_id=eq.{resource_id}"
        url = f"{config.PGREST_ENDPOINT}/resources_exceptions?{q}"
        async with self.session.get(url) as res:
            record = await res.json()
            if not res.ok:
                handle_exception(res.status, "Database error", record, resource_id)
            if not record:
                return None
            # indexes look like {"column_name": "index_type", ...} or None
            indexes: dict = record[0].get("table_indexes", {})
            return set(indexes.keys()) if indexes else None
