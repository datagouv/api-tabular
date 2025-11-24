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
