#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import uuid

from aiohttp import web
from aiohttp.web import Request, Response
from mcp.types import CallToolResult, TextContent

from api_tabular import config
from api_tabular.mcp import datagouv_api_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_TOOL_DESCRIPTION_LENGTH = 400  # Maximum length for tool descriptions


class HTTPMCPServer:
    """HTTP-based MCP server that runs as a persistent service."""

    def __init__(self):
        self.app = web.Application()
        self.sessions = {}  # Store active sessions
        self._setup_routes()

    def _setup_routes(self):
        """Setup HTTP routes for MCP operations following standards."""
        # New Streamable HTTP transport (standards-compliant)
        self.app.router.add_post("/mcp", self._mcp_endpoint)
        self.app.router.add_get("/mcp", self._mcp_endpoint)

        # Health check (non-standard but useful)
        self.app.router.add_get("/health", self._health_check)

    async def _validate_origin(self, request: Request) -> bool:
        """Validate Origin header to prevent DNS rebinding attacks."""
        origin = request.headers.get("Origin")
        if not origin:
            return True  # Allow requests without Origin header

        # Allow common localhost variants and desktop app schemes
        if origin == "null":
            return True
        if origin.startswith("app://"):
            return True
        if origin.startswith("http://localhost") or origin.startswith("http://127.0.0.1"):
            return True

        # Backward-compatible explicit allowlist (dev UIs)
        allowed_origins = {
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
        }
        return origin in allowed_origins

    async def _mcp_endpoint(self, request: Request) -> Response:
        """Single MCP endpoint handling both POST and GET requests."""
        # Validate Origin header for security
        if not await self._validate_origin(request):
            return web.Response(status=403, text="Origin not allowed")

        if request.method == "POST":
            return await self._handle_post_request(request)
        elif request.method == "GET":
            return await self._handle_get_request(request)
        else:
            return web.Response(status=405, text="Method Not Allowed")

    async def _handle_post_request(self, request: Request) -> Response:
        """Handle POST requests (JSON-RPC messages from client)."""
        try:
            # Require JSON payload but don't enforce Accept header (clients often send */* or omit)
            content_type = request.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                return web.Response(status=400, text="Content-Type must be application/json")

            # Get protocol version
            protocol_version = request.headers.get("MCP-Protocol-Version", "2025-06-18")

            # Get session ID if present
            session_id = request.headers.get("Mcp-Session-Id")

            # Parse JSON-RPC message
            data = await request.json()

            # Handle different JSON-RPC message types
            if "method" in data:
                # This is a JSON-RPC request
                if data["method"] == "initialize":
                    return await self._handle_initialize_request(data, session_id, protocol_version)
                elif data["method"] == "tools/list":
                    return await self._handle_tools_list_request(data, session_id)
                elif data["method"] == "tools/call":
                    return await self._handle_tools_call_request(data, session_id)
                elif data["method"] == "resources/list":
                    return await self._handle_resources_list_request(data, session_id)
                else:
                    return await self._handle_unknown_method(data)
            else:
                # This is a JSON-RPC response or notification
                return web.Response(status=202, text="Accepted")

        except json.JSONDecodeError:
            return web.Response(status=400, text="Invalid JSON")
        except Exception as e:
            logger.error(f"Error handling POST request: {e}")
            return web.Response(status=500, text="Internal Server Error")

    async def _handle_get_request(self, request: Request) -> Response:
        """Handle GET requests (SSE stream from server)."""
        accept_header = request.headers.get("Accept", "")
        if "text/event-stream" not in accept_header:
            return web.Response(status=405, text="Method Not Allowed")

        # Create SSE response
        response = web.StreamResponse()
        response.headers["Content-Type"] = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["Connection"] = "keep-alive"
        response.headers["Access-Control-Allow-Origin"] = "*"

        await response.prepare(request)

        try:
            # Send initial connection event
            await response.write(
                b'data: {"type": "connected", "message": "MCP Server connected"}\n\n'
            )

            # Keep connection alive with heartbeats
            while True:
                await asyncio.sleep(30)
                heartbeat = {
                    "jsonrpc": "2.0",
                    "method": "ping",
                    "params": {"timestamp": int(asyncio.get_event_loop().time())},
                }
                await response.write(f"data: {json.dumps(heartbeat)}\n\n".encode())

        except asyncio.CancelledError:
            logger.info("SSE connection closed by client")
        except Exception as e:
            logger.error(f"SSE error: {e}")
        finally:
            await response.write_eof()

        return response

    async def _handle_initialize_request(
        self, data: dict, session_id: str, protocol_version: str
    ) -> Response:
        """Handle MCP initialize request."""
        # Generate new session ID if not provided
        if not session_id:
            session_id = str(uuid.uuid4())
            self.sessions[session_id] = {"protocol_version": protocol_version, "initialized": True}

        response_data = {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "result": {
                "protocolVersion": protocol_version,
                "capabilities": {"tools": {}, "resources": {}},
                "serverInfo": {"name": "api-tabular-mcp", "version": "1.0.0"},
            },
        }

        response = web.json_response(response_data)
        response.headers["Mcp-Session-Id"] = session_id
        return response

    async def _handle_tools_list_request(self, data: dict, session_id: str) -> Response:
        """Handle tools/list request."""
        if session_id and session_id not in self.sessions:
            return web.Response(status=404, text="Session not found")

        # Build tools dynamically from all configured resources
        import aiohttp

        tools = []
        async with aiohttp.ClientSession() as session:
            for resource_id in config.MCP_AVAILABLE_RESOURCE_IDS:
                try:
                    meta = await datagouv_api_client.get_resource_and_dataset_metadata(
                        resource_id, session=session
                    )
                    res = meta.get("resource", {})
                    ds = meta.get("dataset", {})
                    title = (res.get("title") or resource_id).strip()
                    ds_title = (ds.get("title") or "").strip()
                    # Prefer description_short if available, fallback to description
                    ds_desc_short = (ds.get("description_short") or "").strip()
                    ds_desc = (ds.get("description") or "").strip()
                    ds_text = ds_desc_short if ds_desc_short else ds_desc

                    # Compose description: "[Resource Title]" from dataset "[Dataset Title]": [Description]
                    if ds_title and ds_text:
                        base = f'"{title}" from dataset "{ds_title}": {ds_text}'
                        description = (
                            base[:MAX_TOOL_DESCRIPTION_LENGTH] + "..."
                            if len(base) > MAX_TOOL_DESCRIPTION_LENGTH
                            else base
                        )
                    elif ds_title:
                        description = f'"{title}" from dataset "{ds_title}"'
                    else:
                        description = f'"{title}"'
                except Exception as e:
                    logger.warning(f"Failed to fetch metadata for {resource_id}: {e}")
                    description = f"{resource_id}. Resource from data.gouv.fr"

                tools.append(
                    {
                        "name": f"ask_resource_{resource_id.replace('-', '_')}",
                        "description": description,
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "question": {
                                    "type": "string",
                                    "description": "Natural language question about this resource",
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Maximum number of results to return",
                                    "default": 20,
                                },
                            },
                            "required": ["question"],
                        },
                    }
                )

        response_data = {"jsonrpc": "2.0", "id": data.get("id"), "result": {"tools": tools}}

        return web.json_response(response_data)

    async def _handle_tools_call_request(self, data: dict, session_id: str) -> Response:
        """Handle tools/call request."""
        if session_id and session_id not in self.sessions:
            return web.Response(status=404, text="Session not found")

        params = data.get("params", {})
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        # Match dynamic tools: ask_resource_<resource_id>
        if tool_name and tool_name.startswith("ask_resource_"):
            resource_id = tool_name.replace("ask_resource_", "", 1).replace("_", "-")
            question = arguments.get("question", "")
            limit = int(arguments.get("limit", 20))
            call_result = await self._handle_ask_specific_resource(resource_id, question, limit)
            result = {
                "content": [
                    {"type": content.type, "text": content.text} for content in call_result.content
                ],
                "isError": call_result.isError,
            }
        else:
            result = {
                "content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}],
                "isError": True,
            }

        response_data = {"jsonrpc": "2.0", "id": data.get("id"), "result": result}

        return web.json_response(response_data)

    async def _handle_resources_list_request(self, data: dict, session_id: str) -> Response:
        """Handle resources/list request."""
        if session_id and session_id not in self.sessions:
            return web.Response(status=404, text="Session not found")

        # Build resources list from configured IDs
        resources = []
        ids: list[str] = config.MCP_AVAILABLE_RESOURCE_IDS or []
        for rid in ids:
            resources.append(
                {
                    "uri": f"resource://{rid}",
                    "name": rid,
                    "description": "Configured available resource",
                    "mimeType": "application/json",
                }
            )

        response_data = {"jsonrpc": "2.0", "id": data.get("id"), "result": {"resources": resources}}

        return web.json_response(response_data)

    async def _handle_unknown_method(self, data: dict) -> Response:
        """Handle unknown JSON-RPC methods."""
        error_response = {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "error": {"code": -32601, "message": "Method not found"},
        }
        return web.json_response(error_response, status=400)

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

    async def _handle_ask_specific_resource(
        self, resource_id: str, question: str, limit: int
    ) -> CallToolResult:
        """Handle ask_resource_<id> tool calls (minimal v0)."""
        try:
            text = (
                f"Selected resource: {resource_id}\n"
                f"Question: {question}\n"
                f"Next step: client should read data for resource://{resource_id} and answer."
            )
            return CallToolResult(content=[TextContent(type="text", text=text)])
        except Exception as e:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Error: {str(e)}")],
                isError=True,
            )


async def app_factory():
    """App factory for use with adev runserver."""
    server = HTTPMCPServer()

    async def on_startup(app):
        """Log startup information."""
        logger.info("🚀 Starting Streamable HTTP MCP Server")
        logger.info("📋 Available endpoints:")
        logger.info("   - GET  /health")
        logger.info("🆕 Streamable HTTP transport:")
        logger.info("   - POST /mcp (JSON-RPC messages)")
        logger.info("   - GET  /mcp (SSE stream)")
        logger.info("✅ MCP server started")

    server.app.on_startup.append(on_startup)
    return server.app


def run():
    """Run the application."""
    web.run_app(app_factory(), path=os.environ.get("CSVAPI_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
