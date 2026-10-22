import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any

import duckdb
import sentry_sdk
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.datastructures import MutableHeaders
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from tabular_duckdb import __version__
from tabular_duckdb.config import settings
from tabular_duckdb.engine import QueryResult, engine
from tabular_duckdb.errors import (
    QueryException,
    handle_exception,
    invalid_query_string,
    safe_message,
)
from tabular_duckdb.query import ParsedQuery, build_query
from tabular_duckdb.resources import (
    Resource,
    build_profile,
    head,
    profile_columns,
    resource_cache,
)
from tabular_duckdb.serialization import jsonable, rows_to_json
from tabular_duckdb.sql import RESOURCE_TABLE, run_user_sql
from tabular_duckdb.streaming import check_output_size, start_stream, stream_csv, stream_json
from tabular_duckdb.swagger import build_swagger_file

SWAGGER_FILE = Path(__file__).resolve().parent.parent / "tabular_swagger.yaml"


def external_url(path: str) -> str:
    return f"{settings.SCHEME}://{settings.SERVER_NAME}{path}"


def resource_url(resource_id: str, suffix: str = "") -> str:
    return external_url(f"/api/resources/{resource_id}/{suffix}")


def build_link_with_page(path: str, query_string: list[str], page: int, page_size: int) -> str:
    query = [
        argument
        for argument in query_string
        if argument.partition("=")[0] not in ("page", "page_size")
    ]
    query.extend([f"page={page}", f"page_size={page_size}"])
    return external_url(f"{path}?{'&'.join(query)}")


def query_arguments(request: Request) -> list[str]:
    return request.url.query.split("&") if request.url.query else []


def int_argument(request: Request, name: str, default: int) -> int:
    raw = request.query_params.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise invalid_query_string(f"`{name}` must be an integer, got '{raw}'") from None
    if value < 1:
        raise invalid_query_string(f"`{name}` must be greater than 0")
    return value


def parse_query(
    query_string: list[str],
    columns: Mapping[str, str],
    page_size: int | None = None,
    offset: int = 0,
) -> ParsedQuery:
    try:
        return build_query(query_string, columns, page_size, offset)
    except ValueError as exc:
        raise invalid_query_string(f"Malformed query: {exc}") from exc


async def run_query(sql: str, params: list, resource_id: str) -> QueryResult:
    try:
        return await engine.query(sql, params)
    except duckdb.Error as exc:
        raise handle_exception(400, "Database error", safe_message(str(exc)), resource_id) from exc


def check_storage() -> None:
    if not settings.storage_over_http:
        return
    try:
        head(settings.PARQUET_BASE_URL)
    except StarletteHTTPException:
        # 403/404 on the base URL means the storage answered, which is what we check here
        return
    except QueryException as exc:
        if exc.status == 503:
            raise


async def run_count(parsed: ParsedQuery, resource: Resource, resource_id: str) -> int:
    sql, params = parsed.count_sql(resource.source_sql)
    return int((await run_query(sql, params, resource_id)).rows[0][0])


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if settings.SENTRY_DSN:
        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            sample_rate=settings.SENTRY_SAMPLE_RATE,
            release=__version__,
        )
    await engine.start()
    app.state.started_at = datetime.now(UTC)
    app.state.openapi_document = yaml.safe_load(SWAGGER_FILE.read_text(encoding="utf-8"))
    app.state.openapi_document["info"]["version"] = __version__
    try:
        yield
    finally:
        await engine.stop()


app = FastAPI(
    title="Tabular API (DuckDB)",
    version=__version__,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
    "Access-Control-Expose-Headers": "*",
}


class CORSHeadersMiddleware:
    """The CORS headers on every response, and a 204 to every preflight.

    Unlike Starlette's CORSMiddleware, the headers do not depend on the request having an
    Origin: the Nginx cache in front of the API serves a response to every origin.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
        elif scope["method"] == "OPTIONS":
            await Response(status_code=204, headers=CORS_HEADERS)(scope, receive, send)
        else:

            async def send_with_cors(message: Message) -> None:
                if message["type"] == "http.response.start":
                    MutableHeaders(scope=message).update(CORS_HEADERS)
                await send(message)

            await self.app(scope, receive, send_with_cors)


app.add_middleware(CORSHeadersMiddleware)


def get(path: str, **kwargs: Any) -> Callable:
    """GET route also answering HEAD, like every GET route of the PostgREST-based API."""
    return app.api_route(path, methods=["GET", "HEAD"], **kwargs)


@app.exception_handler(QueryException)
async def query_exception_handler(request: Request, exc: QueryException) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content=exc.body())


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> PlainTextResponse:
    # the plain text body of the PostgREST-based API for its HTTP errors, e.g. "404: Not Found"
    return PlainTextResponse(
        f"{exc.status_code}: {HTTPStatus(exc.status_code).phrase}",
        status_code=exc.status_code,
        headers=exc.headers,
    )


@get("/api/resources/{rid}/", name="meta")
async def resource_meta(rid: str) -> JSONResponse:
    resource = await resource_cache.get(rid)
    return JSONResponse(
        {
            "created_at": resource.created_at,
            # redirects to the current URL of the source file
            "url": f"{settings.DATAGOUV_URL.rstrip('/')}/api/1/datasets/r/{rid}",
            "links": [
                {"href": resource_url(rid, "profile/"), "type": "GET", "rel": "profile"},
                {"href": resource_url(rid, "data/"), "type": "GET", "rel": "data"},
                {"href": resource_url(rid, "swagger/"), "type": "GET", "rel": "swagger"},
                {"href": resource_url(rid, "sql/"), "type": "GET", "rel": "sql"},
            ],
        }
    )


@get("/api/resources/{rid}/profile/", name="profile")
async def resource_profile(rid: str) -> JSONResponse:
    resource = await resource_cache.get(rid)
    profile = await build_profile(resource)
    # no database index in this architecture, the whole parquet file is scanned
    return JSONResponse(jsonable({"profile": profile, "indexes": None}))


@get("/api/resources/{rid}/swagger/", name="swagger")
async def resource_swagger(rid: str) -> Response:
    resource = await resource_cache.get(rid)
    swagger = build_swagger_file(profile_columns(resource), rid)
    return PlainTextResponse(swagger)


@get("/api/resources/{rid}/data/", name="data")
async def resource_data(request: Request, rid: str) -> JSONResponse:
    resource = await resource_cache.get(rid)
    query_string = query_arguments(request)
    page = int_argument(request, "page", 1)
    page_size = int_argument(request, "page_size", settings.PAGE_SIZE_DEFAULT)
    if page_size > settings.PAGE_SIZE_MAX:
        raise invalid_query_string(f"Page size exceeds allowed maximum: {settings.PAGE_SIZE_MAX}")
    offset = page_size * (page - 1)

    parsed = parse_query(query_string, resource.query_columns, page_size, offset)
    sql, params = parsed.data_sql(resource.source_sql)
    result = await run_query(sql, params, rid)
    meta: dict[str, int] = {"page": page, "page_size": page_size}
    if parsed.is_aggregated:
        # like the PostgREST-based API, which reported no total for aggregated queries: a full
        # page is the only hint that another one may follow
        has_next = len(result.rows) >= page_size
    else:
        meta["total"] = await run_count(parsed, resource, rid)
        has_next = page_size + offset < meta["total"]

    path = f"/api/resources/{rid}/data/"
    return JSONResponse(
        {
            "data": rows_to_json(result.columns, result.rows),
            "links": {
                "profile": resource_url(rid, "profile/"),
                "swagger": resource_url(rid, "swagger/"),
                "next": (
                    build_link_with_page(path, query_string, page + 1, page_size)
                    if has_next
                    else None
                ),
                "prev": (
                    build_link_with_page(path, query_string, page - 1, page_size)
                    if page > 1
                    else None
                ),
            },
            "meta": meta,
        }
    )


async def stream_resource_data(request: Request, rid: str, format: str) -> StreamingResponse:
    resource = await resource_cache.get(rid)
    parsed = parse_query(query_arguments(request), resource.query_columns)
    check_output_size(await run_count(parsed, resource, rid))

    sql, params = parsed.data_sql(resource.source_sql)
    try:
        chunks = await start_stream(sql, params)
    except duckdb.Error as exc:
        raise handle_exception(400, "Database error", safe_message(str(exc)), rid) from exc
    mime = "application/json" if format == "json" else "text/csv"
    streamer = stream_json if format == "json" else stream_csv
    return StreamingResponse(
        streamer(chunks),
        media_type=mime,
        headers={"Content-Disposition": f'attachment; filename="{rid}.{format}"'},
    )


@get("/api/resources/{rid}/data/csv/", name="csv")
async def resource_data_csv(request: Request, rid: str) -> StreamingResponse:
    return await stream_resource_data(request, rid, format="csv")


@get("/api/resources/{rid}/data/json/", name="json")
async def resource_data_json(request: Request, rid: str) -> StreamingResponse:
    return await stream_resource_data(request, rid, format="json")


@get("/api/resources/{rid}/sql/", name="sql")
async def resource_sql(rid: str, q: str = "") -> JSONResponse:
    resource = await resource_cache.get(rid)
    result = await run_user_sql(resource, q)
    return JSONResponse(
        {
            "data": rows_to_json(result.columns, result.rows),
            "meta": {
                "table": RESOURCE_TABLE,
                "rows": len(result.rows),
                "truncated": result.truncated,
                "row_limit": result.row_limit,
                "execution_ms": result.execution_ms,
            },
        }
    )


@get("/health/")
async def health(request: Request) -> JSONResponse:
    try:
        await engine.ping()
    except duckdb.Error as exc:
        raise QueryException(503, "DB unavailable", f"DuckDB is not available: {exc}") from exc
    await asyncio.to_thread(check_storage)
    return JSONResponse(
        {
            "status": "ok",
            "version": __version__,
            "uptime_since": request.app.state.started_at.isoformat(),
        }
    )


@get("/api/aggregation-exceptions/")
async def aggregation_exceptions() -> JSONResponse:
    # the PostgREST-based API restricted aggregations to indexed tables, which do not exist here
    return JSONResponse({"allowed": True, "exceptions": []})


# the path where aiohttp_swagger served the spec of the PostgREST-based API
OPENAPI_PATH = f"{settings.DOC_PATH}/swagger.json"


@get(OPENAPI_PATH, include_in_schema=False)
async def openapi_document(request: Request) -> JSONResponse:
    return JSONResponse(request.app.state.openapi_document)


@get(settings.DOC_PATH, include_in_schema=False)
async def documentation() -> Response:
    return get_swagger_ui_html(openapi_url=OPENAPI_PATH, title="Tabular API (DuckDB)")


def run() -> None:
    uvicorn.run("tabular_duckdb.main:app", host="0.0.0.0", port=8005)


if __name__ == "__main__":
    run()
