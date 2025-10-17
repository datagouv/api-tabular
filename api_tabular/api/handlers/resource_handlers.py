"""
Resource-related request handlers.
"""

from aiohttp import web

from api_tabular import config
from api_tabular.core.data_access import DataAccessor
from api_tabular.core.query_builder import QueryBuilder
from api_tabular.error import QueryException
from api_tabular.utils import (
    build_link_with_page,
    build_swagger_file,
    url_for,
)


def _get_data_accessor(session) -> DataAccessor:
    """Get DataAccessor instance for the session."""
    return DataAccessor(session)


def _get_query_builder() -> QueryBuilder:
    """Get QueryBuilder instance."""
    return QueryBuilder()


def build_next_page(
    nb_results: int, page_size: int, offset: int, total: int | None, default_next: str
) -> str | None:
    """Build next page URL for pagination."""
    if total is not None:
        # this is for raw or filtering queries
        return default_next if page_size + offset < total else None
    # for aggregation queries, the total is erroneous but we can be somewhat smart for the next page
    if nb_results < page_size:
        return None
    return default_next


async def handle_resource_meta(request):
    """Handle resource metadata requests."""
    resource_id = request.match_info["rid"]
    data_accessor = _get_data_accessor(request.app["csession"])
    resource = await data_accessor.get_resource(resource_id, ["created_at", "url"])
    return web.json_response(
        {
            "created_at": resource["created_at"],
            "url": resource["url"],
            "links": [
                {
                    "href": url_for(request, "profile", rid=resource_id, _external=True),
                    "type": "GET",
                    "rel": "profile",
                },
                {
                    "href": url_for(request, "data", rid=resource_id, _external=True),
                    "type": "GET",
                    "rel": "data",
                },
                {
                    "href": url_for(request, "swagger", rid=resource_id, _external=True),
                    "type": "GET",
                    "rel": "swagger",
                },
            ],
        }
    )


async def handle_resource_profile(request):
    """Handle resource profile requests."""
    resource_id = request.match_info["rid"]
    data_accessor = _get_data_accessor(request.app["csession"])
    resource: dict = await data_accessor.get_resource(resource_id, ["profile:csv_detective"])
    indexes: set | None = await data_accessor.get_potential_indexes(resource_id)
    resource["indexes"] = list(indexes) if isinstance(indexes, set) else None
    return web.json_response(resource)


async def handle_resource_swagger(request):
    """Handle resource Swagger documentation requests."""
    resource_id = request.match_info["rid"]
    data_accessor = _get_data_accessor(request.app["csession"])
    resource: dict = await data_accessor.get_resource(resource_id, ["profile:csv_detective"])
    indexes: set | None = await data_accessor.get_potential_indexes(resource_id)
    columns: dict[str, str] = resource["profile"]["columns"]
    if indexes:
        columns = {col: params for col, params in columns.items() if col in indexes}
    swagger_string = build_swagger_file(columns, resource_id)
    return web.Response(body=swagger_string)


async def handle_resource_data(request):
    """Handle resource data requests."""
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []
    page = int(request.query.get("page", "1"))
    page_size = int(request.query.get("page_size", config.PAGE_SIZE_DEFAULT))

    if page_size > config.PAGE_SIZE_MAX:
        raise QueryException(
            400,
            None,
            "Invalid query string",
            f"Page size exceeds allowed maximum: {config.PAGE_SIZE_MAX}",
        )
    if page > 1:
        offset = page_size * (page - 1)
    else:
        offset = 0

    data_accessor = _get_data_accessor(request.app["csession"])
    query_builder = _get_query_builder()

    indexes: set | None = await data_accessor.get_potential_indexes(resource_id)
    try:
        sql_query = query_builder.build_sql_query_string(
            query_string, resource_id, indexes, page_size, offset
        )
    except ValueError as e:
        raise QueryException(400, None, "Invalid query string", f"Malformed query: {e}")
    except PermissionError as e:
        raise QueryException(403, None, "Unauthorized parameters", str(e))

    resource = await data_accessor.get_resource(resource_id, ["parsing_table"])
    response, total = await data_accessor.get_resource_data(resource, sql_query)

    next = build_link_with_page(request, query_string, page + 1, page_size)
    prev = build_link_with_page(request, query_string, page - 1, page_size)
    body = {
        "data": response,
        "links": {
            "profile": url_for(request, "profile", rid=resource_id, _external=True),
            "swagger": url_for(request, "swagger", rid=resource_id, _external=True),
            "next": build_next_page(
                nb_results=len(response),
                page_size=page_size,
                offset=offset,
                total=total,
                default_next=next,
            ),
            "prev": prev if page > 1 else None,
        },
        "meta": {"page": page, "page_size": page_size},
    }
    if total is not None:
        body["meta"]["total"] = total
    return web.json_response(body)


async def handle_resource_data_csv(request):
    """Handle resource data CSV export requests."""
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []

    data_accessor = _get_data_accessor(request.app["csession"])
    query_builder = _get_query_builder()

    try:
        sql_query = query_builder.build_sql_query_string(query_string, resource_id)
    except ValueError:
        raise QueryException(400, None, "Invalid query string", "Malformed query")
    except PermissionError as e:
        raise QueryException(403, None, "Unauthorized parameters", str(e))

    resource = await data_accessor.get_resource(resource_id, ["parsing_table"])

    response_headers = {
        "Content-Disposition": f'attachment; filename="{resource_id}.csv"',
        "Content-Type": "text/csv",
    }
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    async for chunk in data_accessor.get_resource_data_streamed(resource, sql_query):
        await response.write(chunk)

    await response.write_eof()
    return response


async def handle_resource_data_json(request):
    """Handle resource data JSON export requests."""
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []

    data_accessor = _get_data_accessor(request.app["csession"])
    query_builder = _get_query_builder()

    try:
        sql_query = query_builder.build_sql_query_string(query_string, resource_id)
    except ValueError:
        raise QueryException(400, None, "Invalid query string", "Malformed query")
    except PermissionError as e:
        raise QueryException(403, None, "Unauthorized parameters", str(e))

    resource = await data_accessor.get_resource(resource_id, ["parsing_table"])

    response_headers = {
        "Content-Disposition": f'attachment; filename="{resource_id}.json"',
        "Content-Type": "application/json",
    }
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    async for chunk in data_accessor.get_resource_data_streamed(
        resource,
        sql_query,
        accept_format="application/json",
    ):
        await response.write(chunk)

    await response.write_eof()
    return response
