import json
from typing import AsyncGenerator

from aiohttp import ClientSession, web

from api_tabular import config
from api_tabular.error import handle_exception
from api_tabular.utils import process_total


async def get_resource(session: ClientSession, resource_id: str, columns: list) -> dict:
    # Always include deleted_at and dataset_id for deletion checking, but don't duplicate them
    if "deleted_at" not in columns:
        columns.append("deleted_at")
    if "dataset_id" not in columns:
        columns.append("dataset_id")
    q = f"select={','.join(columns)}&resource_id=eq.{resource_id}&order=created_at.desc"
    url = f"{config.PGREST_ENDPOINT}/tables_index?{q}"
    async with session.get(url) as res:
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
    session: ClientSession, resource: dict, sql_query: str
) -> tuple[list[dict], int | None]:
    headers = {"Prefer": "count=exact"}
    url = f"{config.PGREST_ENDPOINT}/{resource['parsing_table']}?{sql_query}"
    skip_total = False
    if any(f".{agg}()" in url for agg in ["count", "max", "min", "sum", "avg"]):
        # the total for aggretated data is wrong, it is always the length of the original table
        skip_total = True
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            handle_exception(res.status, "Database error", await res.json(), resource.get("id"))
        record = await res.json()
        total = process_total(res) if not skip_total else None
        return record, total


async def get_resource_data_streamed(
    session: ClientSession,
    model: dict,
    sql_query: str,
    accept_format: str = "text/csv",
    batch_size: int = config.BATCH_SIZE,
) -> AsyncGenerator[bytes, None]:
    headers = {"Accept": accept_format, "Prefer": "count=exact"}
    url = f"{config.PGREST_ENDPOINT}/{model['parsing_table']}?{sql_query}"
    res = await session.head(f"{url}&limit=1&", headers=headers)
    if not res.ok:
        handle_exception(res.status, "Database error", await res.json(), None)
    total = process_total(res)

    is_json = accept_format == "application/json"
    is_first_batch = True
    has_emitted_data = False  # Track if we've emitted any data for JSON

    try:
        for i in range(0, total, batch_size):
            async with session.get(
                url=f"{url}&limit={batch_size}&offset={i}", headers=headers
            ) as res:
                if not res.ok:
                    handle_exception(res.status, "Database error", await res.json(), None)

                if is_json:
                    # For JSON, we need to parse each batch and reconstruct a valid array
                    batch_data = await res.json()
                    if not isinstance(batch_data, list):
                        batch_data = [batch_data]

                    if is_first_batch:
                        # First batch: open the JSON array
                        if batch_data:
                            json_str = json.dumps(batch_data, ensure_ascii=False)
                            yield json_str[:-1].encode("utf-8")  # Remove the final ']'
                            has_emitted_data = True
                        else:
                            yield b"["
                        is_first_batch = False
                    else:
                        # Subsequent batches: add a comma and elements without brackets
                        if batch_data:
                            if has_emitted_data:
                                yield b","
                            json_str = json.dumps(batch_data, ensure_ascii=False)
                            yield json_str[1:-1].encode("utf-8")  # Remove '[' and ']'
                            has_emitted_data = True
                else:
                    # For CSV, we need to handle headers
                    if is_first_batch:
                        # First batch: include everything (headers + data)
                        async for chunk in res.content.iter_chunked(1024):
                            yield chunk
                        is_first_batch = False
                    else:
                        # Subsequent batches: skip the first line (headers)
                        first_line = True
                        buffer = b""
                        async for chunk in res.content.iter_chunked(1024):
                            buffer += chunk
                            while b"\n" in buffer:
                                line, buffer = buffer.split(b"\n", 1)
                                if not first_line:
                                    yield line + b"\n"
                                first_line = False
                        # Emit remaining buffer if there's anything left after the first line
                        if buffer and not first_line:
                            yield buffer
    finally:
        # For JSON, close the array at the end (or emit an empty array if no batches)
        if is_json:
            if is_first_batch:
                yield b"[]"
            else:
                yield b"]"


async def get_potential_indexes(session: ClientSession, resource_id: str) -> set[str] | None:
    q = f"select=table_indexes&resource_id=eq.{resource_id}"
    url = f"{config.PGREST_ENDPOINT}/resources_exceptions?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            handle_exception(res.status, "Database error", record, resource_id)
        if not record:
            return None
        # indexes look like {"column_name": "index_type", ...} or None
        indexes: dict = record[0].get("table_indexes", {})
        return set(indexes.keys()) if indexes else None
