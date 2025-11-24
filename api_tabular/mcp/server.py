#!/usr/bin/env python3
import json
import logging
import os

from aiohttp import web
from aiohttp.web import Request, Response

from api_tabular.mcp import datagouv_api_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HTTPMCPServer:
    """HTTP-based MCP server that runs as a persistent service."""

    def __init__(self):
        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        """Setup HTTP routes for MCP operations."""
        self.app.router.add_post("/mcp", self._mcp_endpoint)
        self.app.router.add_get("/health", self._health_check)

    async def _mcp_endpoint(self, request: Request) -> Response:
        """MCP endpoint handling POST requests (JSON-RPC messages)."""
        if request.method == "POST":
            return await self._handle_post_request(request)
        else:
            return web.Response(status=405, text="Method Not Allowed")

    async def _handle_post_request(self, request: Request) -> Response:
        """Handle POST requests (JSON-RPC messages from client)."""
        try:
            # Require JSON payload but don't enforce Accept header (clients often send */* or omit)
            content_type = request.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                return web.Response(status=400, text="Content-Type must be application/json")

            # Parse JSON-RPC message
            data = await request.json()

            # Handle different JSON-RPC message types
            if "method" in data:
                if data["method"] == "initialize":
                    return await self._handle_initialize_request(data)
                elif data["method"] == "tools/list":
                    return await self._handle_tools_list_request(data)
                elif data["method"] == "tools/call":
                    return await self._handle_tools_call_request(data)
                elif data["method"] == "resources/list":
                    return await self._handle_resources_list_request(data)
                else:
                    return await self._handle_unknown_method(data)
            else:
                return web.Response(status=202, text="Accepted")

        except json.JSONDecodeError:
            return web.Response(status=400, text="Invalid JSON")
        except Exception as e:
            logger.error(f"Error handling POST request: {e}")
            return web.Response(status=500, text="Internal Server Error")

    async def _handle_initialize_request(self, data: dict) -> Response:
        """Handle MCP initialize request."""
        response_data = {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}, "resources": {}},
                "serverInfo": {"name": "api-tabular-mcp", "version": "1.0.0"},
            },
        }
        return web.json_response(response_data)

    async def _handle_unknown_method(self, data: dict) -> Response:
        """Handle unknown JSON-RPC methods."""
        error_response = {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "error": {"code": -32601, "message": "Method not found"},
        }
        return web.json_response(error_response, status=400)

    async def _handle_tools_list_request(self, data: dict) -> Response:
        """Handle tools/list request - return available tools."""
        tools = [
            {
                "name": "search_datasets",
                "description": (
                    "Search for datasets on data.gouv.fr by keywords. "
                    "Returns a list of datasets matching the search query with their metadata, "
                    "including title, description, organization, tags, and resource count. "
                    "Use this to discover datasets before querying their data."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query string (searches in title, description, tags)",
                        },
                        "page": {
                            "type": "integer",
                            "description": "Page number (default: 1)",
                            "default": 1,
                        },
                        "page_size": {
                            "type": "integer",
                            "description": "Number of results per page (default: 20, max: 100)",
                            "default": 20,
                            "minimum": 1,
                            "maximum": 100,
                        },
                    },
                    "required": ["query"],
                },
            }
        ]

        response_data = {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "result": {"tools": tools},
        }
        return web.json_response(response_data)

    async def _execute_search_datasets(self, arguments: dict) -> dict:
        """Execute the search_datasets tool."""
        query = arguments.get("query", "")
        if not query:
            raise ValueError("query parameter is required")

        page = arguments.get("page", 1)
        page_size = arguments.get("page_size", 20)

        result = await datagouv_api_client.search_datasets(
            query=query, page=page, page_size=page_size
        )

        # Format the result as text content
        datasets = result.get("data", [])
        if not datasets:
            content = f"No datasets found for query: '{query}'"
        else:
            content_parts = [
                f"Found {result.get('total', len(datasets))} dataset(s) for query: '{query}'",
                f"Page {result.get('page', 1)} of results:\n",
            ]
            for i, ds in enumerate(datasets, 1):
                content_parts.append(f"{i}. {ds.get('title', 'Untitled')}")
                content_parts.append(f"   ID: {ds.get('id')}")
                if ds.get("description_short"):
                    desc = ds.get("description_short", "")[:200]
                    content_parts.append(f"   Description: {desc}...")
                if ds.get("organization"):
                    content_parts.append(f"   Organization: {ds.get('organization')}")
                if ds.get("tags"):
                    tags = ", ".join(ds.get("tags", [])[:5])
                    content_parts.append(f"   Tags: {tags}")
                content_parts.append(f"   Resources: {ds.get('resources_count', 0)}")
                content_parts.append(f"   URL: {ds.get('url')}")
                content_parts.append("")

            content = "\n".join(content_parts)

        return {
            "content": [
                {
                    "type": "text",
                    "text": content,
                }
            ],
            "isError": False,
        }

    async def _handle_tools_call_request(self, data: dict) -> Response:
        """Handle tools/call request - execute a tool."""
        params = data.get("params", {})
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        try:
            # Route to the appropriate tool handler
            if tool_name == "search_datasets":
                result = await self._execute_search_datasets(arguments)
            else:
                raise ValueError(f"Unknown tool: {tool_name}")

            response_data = {
                "jsonrpc": "2.0",
                "id": data.get("id"),
                "result": result,
            }

        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}", exc_info=True)
            response_data = {
                "jsonrpc": "2.0",
                "id": data.get("id"),
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Error: {str(e)}",
                        }
                    ],
                    "isError": True,
                },
            }

        return web.json_response(response_data)

    async def _handle_resources_list_request(self, data: dict) -> Response:
        """Handle resources/list request - return available resources."""
        # For now, return empty resources list (we focus on tools)
        response_data = {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "result": {"resources": []},
        }
        return web.json_response(response_data)

    async def _health_check(self, request: Request) -> Response:
        """Health check endpoint."""
        return web.json_response(
            {
                "status": "healthy",
                "server": "http-mcp-server",
                "version": "1.0.0",
                "protocol": "mcp",
            }
        )


async def app_factory():
    """App factory for use with adev runserver."""
    server = HTTPMCPServer()

    async def on_startup(app):
        """Log startup information."""
        logger.info("Starting MCP Server")
        logger.info("Endpoints: POST /mcp, GET /health")

    server.app.on_startup.append(on_startup)
    return server.app


def run():
    """Run the application."""
    web.run_app(app_factory(), path=os.environ.get("CSVAPI_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
