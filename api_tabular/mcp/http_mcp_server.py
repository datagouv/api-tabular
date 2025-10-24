#!/usr/bin/env python3
"""
HTTP-based MCP server that runs as a persistent service.
This implements the MCP protocol over HTTP for better stability.
"""

import asyncio
import json
import logging
import uuid
from pathlib import Path

from aiohttp import web
from aiohttp.web import Request, Response
from mcp.types import (
    CallToolResult,
    ListResourcesResult,
    ListToolsResult,
    ReadResourceResult,
    Resource,
    TextContent,
    Tool,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HTTPMCPServer:
    """HTTP-based MCP server that runs as a persistent service."""

    def __init__(self):
        self.app = web.Application()
        self.sessions = {}  # Store active sessions
        self._setup_routes()
        self.resources_config_path = Path(__file__).parent / "data" / "mcp_available_resources.json"
        self.pgrest_endpoint = "http://localhost:8081"

    def _setup_routes(self):
        """Setup HTTP routes for MCP operations following standards."""
        # New Streamable HTTP transport (standards-compliant)
        self.app.router.add_post("/mcp", self._mcp_endpoint)
        self.app.router.add_get("/mcp", self._mcp_endpoint)

        # Backward compatibility: Old HTTP+SSE transport endpoints
        self.app.router.add_post("/mcp/initialize", self._initialize)
        self.app.router.add_post("/mcp/tools/list", self._list_tools)
        self.app.router.add_post("/mcp/tools/call", self._call_tool)
        self.app.router.add_post("/mcp/resources/list", self._list_resources)
        self.app.router.add_post("/mcp/resources/read", self._read_resource)
        self.app.router.add_get("/mcp/sse", self._sse_endpoint)

        # Legacy endpoints for compatibility
        self.app.router.add_get("/mcp/tools", self._list_tools_get)
        self.app.router.add_post("/mcp/list_tools", self._list_tools)
        self.app.router.add_post("/mcp/call_tool", self._call_tool)

        # Health check (non-standard but useful)
        self.app.router.add_get("/health", self._health_check)

    async def _validate_origin(self, request: Request) -> bool:
        """Validate Origin header to prevent DNS rebinding attacks."""
        origin = request.headers.get("Origin")
        if origin:
            # For local development, allow localhost origins
            allowed_origins = [
                "http://localhost:3000",
                "http://127.0.0.1:3000",
                "http://localhost:8080",
                "http://127.0.0.1:8080",
            ]
            return origin in allowed_origins
        return True  # Allow requests without Origin header

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
            # Check for required headers
            accept_header = request.headers.get("Accept", "")
            if "application/json" not in accept_header and "text/event-stream" not in accept_header:
                return web.Response(status=400, text="Missing required Accept header")

            # Get protocol version
            protocol_version = request.headers.get("MCP-Protocol-Version", "2025-03-26")

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

    async def _sse_endpoint(self, request: Request) -> Response:
        """SSE endpoint for backward compatibility with old HTTP+SSE transport."""
        response = web.StreamResponse()
        response.headers["Content-Type"] = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["Connection"] = "keep-alive"
        response.headers["Access-Control-Allow-Origin"] = "*"

        await response.prepare(request)

        try:
            # Send endpoint event for old transport compatibility
            endpoint_event = {"type": "endpoint", "endpoint": "/mcp"}
            await response.write(f"data: {json.dumps(endpoint_event)}\n\n".encode())

            # Keep connection alive with heartbeats
            while True:
                await asyncio.sleep(30)
                heartbeat = {"type": "heartbeat", "timestamp": int(asyncio.get_event_loop().time())}
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

        tools = [
            {
                "name": "list_datagouv_resources",
                "description": "Browse all available datasets and resources from data.gouv.fr",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "ask_datagouv_question",
                "description": "Ask natural language questions about available datasets and get data results",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question about the data",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results to return",
                            "default": 20,
                        },
                    },
                    "required": ["question"],
                },
            },
        ]

        response_data = {"jsonrpc": "2.0", "id": data.get("id"), "result": {"tools": tools}}

        return web.json_response(response_data)

    async def _handle_tools_call_request(self, data: dict, session_id: str) -> Response:
        """Handle tools/call request."""
        if session_id and session_id not in self.sessions:
            return web.Response(status=404, text="Session not found")

        params = data.get("params", {})
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if tool_name == "list_datagouv_resources":
            call_result = await self._handle_list_datagouv_resources()
            # Convert CallToolResult to dict format for new transport
            result = {
                "content": [
                    {"type": content.type, "text": content.text} for content in call_result.content
                ],
                "isError": call_result.isError,
            }
        elif tool_name == "ask_datagouv_question":
            question = arguments.get("question", "")
            limit = arguments.get("limit", 20)
            call_result = await self._handle_ask_datagouv_question(question, limit)
            # Convert CallToolResult to dict format for new transport
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

        # Load resources from JSON file
        resources = []
        if self.resources_config_path.exists():
            with self.resources_config_path.open("r", encoding="utf-8") as f:
                resources_data = json.load(f)

            for dataset in resources_data:
                for resource in dataset.get("resources", []):
                    resources.append(
                        {
                            "uri": f"resource://{resource['resource_id']}",
                            "name": resource["name"],
                            "description": f"Resource from dataset: {dataset['name']}",
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

    async def _initialize(self, request: Request) -> Response:
        """MCP initialize endpoint."""
        try:
            data = await request.json()
            logger.info(f"MCP Initialize request: {data}")

            response = {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}, "resources": {}},
                "serverInfo": {"name": "api-tabular-mcp", "version": "1.0.0"},
            }
            return web.json_response(response)
        except Exception as e:
            logger.error(f"Error in initialize: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _list_tools(self, request: Request) -> Response:
        """List available MCP tools."""
        try:
            tools_result = ListToolsResult(
                tools=[
                    Tool(
                        name="list_datagouv_resources",
                        description="Browse all available datasets and resources from data.gouv.fr",
                        inputSchema={"type": "object", "properties": {}, "required": []},
                    ),
                    Tool(
                        name="ask_datagouv_question",
                        description="Ask natural language questions about available datasets and get data results",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "question": {
                                    "type": "string",
                                    "description": "Natural language question about the data",
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Maximum number of results to return",
                                    "default": 20,
                                },
                            },
                            "required": ["question"],
                        },
                    ),
                ]
            )

            # Convert to dict for JSON serialization
            result_dict = {
                "tools": [
                    {
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.inputSchema,
                    }
                    for tool in tools_result.tools
                ]
            }

            return web.json_response(result_dict)
        except Exception as e:
            logger.error(f"Error listing tools: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _list_tools_get(self, request: Request) -> Response:
        """List tools via GET (legacy endpoint)."""
        return await self._list_tools(request)

    async def _call_tool(self, request: Request) -> Response:
        """Handle tool calls."""
        try:
            data = await request.json()
            tool_name = data.get("name")
            arguments = data.get("arguments", {})

            logger.info(f"Tool call: {tool_name} with args: {arguments}")

            if tool_name == "list_datagouv_resources":
                result = await self._handle_list_datagouv_resources()
            elif tool_name == "ask_datagouv_question":
                question = arguments.get("question", "")
                limit = arguments.get("limit", 20)
                result = await self._handle_ask_datagouv_question(question, limit)
            else:
                result = CallToolResult(
                    content=[TextContent(type="text", text=f"Unknown tool: {tool_name}")],
                    isError=True,
                )

            # Convert to dict for JSON serialization
            result_dict = {
                "content": [
                    {"type": content.type, "text": content.text} for content in result.content
                ],
                "isError": result.isError,
            }

            return web.json_response(result_dict)
        except Exception as e:
            logger.error(f"Error calling tool: {e}")
            error_result = {
                "content": [{"type": "text", "text": f"Error: {str(e)}"}],
                "isError": True,
            }
            return web.json_response(error_result, status=500)

    async def _list_resources(self, request: Request) -> Response:
        """List available resources."""
        try:
            # Load resources from JSON file
            if self.resources_config_path.exists():
                with self.resources_config_path.open("r", encoding="utf-8") as f:
                    resources_data = json.load(f)

                resources = []
                for dataset in resources_data:
                    for resource in dataset.get("resources", []):
                        resources.append(
                            Resource(
                                uri=f"resource://{resource['resource_id']}",
                                name=resource["name"],
                                description=f"Resource from dataset: {dataset['name']}",
                                mimeType="application/json",
                            )
                        )

                result = ListResourcesResult(resources=resources)
            else:
                result = ListResourcesResult(resources=[])

            # Convert to dict for JSON serialization
            result_dict = {
                "resources": [
                    {
                        "uri": str(resource.uri),
                        "name": resource.name,
                        "description": resource.description,
                        "mimeType": resource.mimeType,
                    }
                    for resource in result.resources
                ]
            }

            return web.json_response(result_dict)
        except Exception as e:
            logger.error(f"Error listing resources: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _read_resource(self, request: Request) -> Response:
        """Read a specific resource."""
        try:
            data = await request.json()
            uri = data.get("uri")

            if not uri or not uri.startswith("resource://"):
                return web.json_response({"error": "Invalid resource URI"}, status=400)

            resource_id = uri.replace("resource://", "")

            # For now, return a placeholder response
            # In a full implementation, this would fetch the actual resource data
            result = ReadResourceResult(
                contents=[
                    TextContent(
                        type="text", text=f"Resource data for {resource_id} would be loaded here"
                    )
                ]
            )

            # Convert to dict for JSON serialization
            result_dict = {
                "contents": [
                    {"type": content.type, "text": content.text} for content in result.contents
                ]
            }

            return web.json_response(result_dict)
        except Exception as e:
            logger.error(f"Error reading resource: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_list_datagouv_resources(self) -> CallToolResult:
        """Handle list_datagouv_resources tool call."""
        try:
            if self.resources_config_path.exists():
                with self.resources_config_path.open("r", encoding="utf-8") as f:
                    resources_data = json.load(f)

                result_text = json.dumps(resources_data, indent=2, ensure_ascii=False)
            else:
                result_text = "No resources configuration found"

            return CallToolResult(content=[TextContent(type="text", text=result_text)])
        except Exception as e:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Error loading resources: {str(e)}")],
                isError=True,
            )

    async def _handle_ask_datagouv_question(self, question: str, limit: int) -> CallToolResult:
        """Handle ask_datagouv_question tool call."""
        try:
            # This is a simplified implementation
            # In a full implementation, this would use the NLP logic from the original server
            result_text = f"Question: {question}\nLimit: {limit}\n\nThis is a placeholder response. The full NLP and database query functionality would be implemented here."

            return CallToolResult(content=[TextContent(type="text", text=result_text)])
        except Exception as e:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Error processing question: {str(e)}")],
                isError=True,
            )

    async def run(self, host: str = "127.0.0.1", port: int = 8082):
        """Run the HTTP MCP server with security best practices."""
        logger.info(f"🚀 Starting Standards-Compliant MCP Server on http://{host}:{port}")
        logger.info("📋 Available endpoints:")
        logger.info(f"   - GET  http://{host}:{port}/health")
        logger.info("   🆕 New Streamable HTTP transport:")
        logger.info(f"   - POST http://{host}:{port}/mcp (JSON-RPC messages)")
        logger.info(f"   - GET  http://{host}:{port}/mcp (SSE stream)")
        logger.info("   🔄 Backward compatibility (old HTTP+SSE transport):")
        logger.info(f"   - POST http://{host}:{port}/mcp/initialize")
        logger.info(f"   - POST http://{host}:{port}/mcp/tools/list")
        logger.info(f"   - POST http://{host}:{port}/mcp/tools/call")
        logger.info(f"   - GET  http://{host}:{port}/mcp/sse")
        logger.info("🔒 Security features:")
        logger.info("   - Origin header validation")
        logger.info("   - Localhost binding only")
        logger.info("   - Session management")
        logger.info("   - Protocol version support")
        logger.info("🔧 Test the server:")
        logger.info(f"   curl http://{host}:{port}/health")
        logger.info(
            f'   curl -X POST http://{host}:{port}/mcp -H \'Accept: application/json\' -H \'Content-Type: application/json\' -d \'{{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {{}}}}\''
        )

        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()

        logger.info("✅ HTTP MCP server started successfully!")

        # Keep the server running
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down HTTP MCP server...")
        finally:
            await runner.cleanup()


async def main():
    """Main entry point."""
    server = HTTPMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
