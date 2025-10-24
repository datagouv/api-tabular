#!/usr/bin/env python3
"""
HTTP-based MCP server that runs as a persistent service.
This implements the MCP protocol over HTTP for better stability.
"""

import asyncio
import json
import logging
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
        self._setup_routes()
        self.resources_config_path = Path(__file__).parent / "data" / "mcp_available_resources.json"
        self.pgrest_endpoint = "http://localhost:8081"

    def _setup_routes(self):
        """Setup HTTP routes for MCP operations."""
        # Health check
        self.app.router.add_get("/health", self._health_check)

        # MCP protocol endpoints
        self.app.router.add_post("/mcp/initialize", self._initialize)
        self.app.router.add_post("/mcp/tools/list", self._list_tools)
        self.app.router.add_post("/mcp/tools/call", self._call_tool)
        self.app.router.add_post("/mcp/resources/list", self._list_resources)
        self.app.router.add_post("/mcp/resources/read", self._read_resource)

        # Legacy endpoints for compatibility
        self.app.router.add_get("/mcp/tools", self._list_tools_get)
        self.app.router.add_post("/mcp/list_tools", self._list_tools)
        self.app.router.add_post("/mcp/call_tool", self._call_tool)

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

    async def run(self, host: str = "localhost", port: int = 8082):
        """Run the HTTP MCP server."""
        logger.info(f"🚀 Starting HTTP MCP server on http://{host}:{port}")
        logger.info("📋 Available endpoints:")
        logger.info(f"   - GET  http://{host}:{port}/health")
        logger.info(f"   - POST http://{host}:{port}/mcp/initialize")
        logger.info(f"   - POST http://{host}:{port}/mcp/tools/list")
        logger.info(f"   - POST http://{host}:{port}/mcp/tools/call")
        logger.info(f"   - POST http://{host}:{port}/mcp/resources/list")
        logger.info(f"   - POST http://{host}:{port}/mcp/resources/read")
        logger.info("🔧 Test the server:")
        logger.info(f"   curl http://{host}:{port}/health")
        logger.info(
            f"   curl -X POST http://{host}:{port}/mcp/tools/list -H 'Content-Type: application/json' -d '{{}}'"
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
