"""
MCP server implementation for api_tabular.

This server provides MCP tools for accessing tabular data using the core logic
directly, ensuring consistency with the REST API.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolResult,
    ListResourcesResult,
    ListToolsResult,
    Resource,
    ResourceContents,
    TextContent,
    TextResourceContents,
    Tool,
)

from .. import config
from ..core.data_access import DataAccessor
from ..core.query_builder import QueryBuilder


class TabularMCPServer:
    """MCP server for accessing tabular data."""

    def __init__(self):
        self.server = Server("api-tabular-mcp")
        self.data_accessor = None
        self.query_builder = QueryBuilder()
        self.resources_config = self._load_resources_config()
        self._setup_handlers()

    def _load_resources_config(self) -> dict:
        """Load accessible resources configuration."""
        config_path = Path(__file__).parent / "resources.json"
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {"resources": []}
        except json.JSONDecodeError:
            return {"resources": []}

    def _setup_handlers(self):
        """Setup MCP server handlers."""
        self.server.list_tools()(self._list_tools)
        self.server.call_tool()(self._call_tool)
        self.server.list_resources()(self._list_resources)
        self.server.read_resource()(self._read_resource)

    async def _list_tools(self) -> ListToolsResult:
        """List available MCP tools."""
        return ListToolsResult(
            tools=[
                Tool(
                    name="query_tabular_data",
                    description="Query tabular data from a resource with filtering, sorting, and pagination",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "resource_id": {
                                "type": "string",
                                "description": "The resource ID to query",
                            },
                            "filters": {
                                "type": "object",
                                "description": "Filter conditions (e.g., {'score__greater': 0.9})",
                                "additionalProperties": True,
                            },
                            "sort": {
                                "type": "object",
                                "description": "Sort configuration (e.g., {'column': 'score', 'order': 'desc'})",
                                "properties": {
                                    "column": {"type": "string"},
                                    "order": {"type": "string", "enum": ["asc", "desc"]},
                                },
                            },
                            "page": {
                                "type": "integer",
                                "description": "Page number (default: 1)",
                                "minimum": 1,
                            },
                            "page_size": {
                                "type": "integer",
                                "description": "Items per page (default: 20, max: 50)",
                                "minimum": 1,
                                "maximum": 50,
                            },
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Specific columns to return",
                            },
                        },
                        "required": ["resource_id"],
                    },
                ),
                Tool(
                    name="get_resource_info",
                    description="Get metadata and profile information for a resource",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "resource_id": {
                                "type": "string",
                                "description": "The resource ID to get information for",
                            }
                        },
                        "required": ["resource_id"],
                    },
                ),
                Tool(
                    name="list_accessible_resources",
                    description="List all accessible resources (requires external configuration)",
                    inputSchema={"type": "object", "properties": {}},
                ),
            ]
        )

    async def _call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        """Handle tool calls."""
        try:
            if name == "query_tabular_data":
                return await self._query_tabular_data(arguments)
            elif name == "get_resource_info":
                return await self._get_resource_info(arguments)
            elif name == "list_accessible_resources":
                return await self._list_accessible_resources(arguments)
            else:
                return CallToolResult(
                    content=[TextContent(type="text", text=f"Unknown tool: {name}")], isError=True
                )
        except Exception as e:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Error: {str(e)}")], isError=True
            )

    async def _query_tabular_data(self, arguments: dict[str, Any]) -> CallToolResult:
        """Query tabular data from a resource."""
        resource_id = arguments["resource_id"]
        filters = arguments.get("filters", {})
        sort = arguments.get("sort", {})
        page = arguments.get("page", 1)
        page_size = arguments.get("page_size", 20)
        columns = arguments.get("columns", [])

        # Ensure data accessor is initialized
        if not self.data_accessor:
            from aiohttp import ClientSession

            self.data_accessor = DataAccessor(ClientSession())

        try:
            # Get resource metadata
            resource = await self.data_accessor.get_resource(resource_id, ["parsing_table"])

            # Get potential indexes
            indexes = await self.data_accessor.get_potential_indexes(resource_id)

            # Build query string
            query_parts = []

            # Add filters
            for key, value in filters.items():
                query_parts.append(f"{key}={value}")

            # Add sorting
            if sort:
                column = sort.get("column")
                order = sort.get("order", "asc")
                if column:
                    query_parts.append(f"{column}__sort={order}")

            # Add pagination
            offset = (page - 1) * page_size
            query_parts.append(f"page={page}")
            query_parts.append(f"page_size={page_size}")

            # Add column selection
            if columns:
                query_parts.append(f"columns={','.join(columns)}")

            # Build SQL query
            sql_query = self.query_builder.build_sql_query_string(
                query_parts, resource_id, indexes, page_size, offset
            )

            # Execute query
            data, total = await self.data_accessor.get_resource_data(resource, sql_query)

            # Format response
            result = {
                "data": data,
                "meta": {"page": page, "page_size": page_size, "total": total},
                "resource_id": resource_id,
            }

            return CallToolResult(
                content=[TextContent(type="text", text=json.dumps(result, indent=2))]
            )

        except Exception as e:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Query error: {str(e)}")], isError=True
            )

    async def _get_resource_info(self, arguments: dict[str, Any]) -> CallToolResult:
        """Get resource metadata and profile information."""
        resource_id = arguments["resource_id"]

        # Ensure data accessor is initialized
        if not self.data_accessor:
            from aiohttp import ClientSession

            self.data_accessor = DataAccessor(ClientSession())

        try:
            # Get basic resource info
            resource = await self.data_accessor.get_resource(
                resource_id, ["created_at", "url", "profile:csv_detective"]
            )

            # Get potential indexes
            indexes = await self.data_accessor.get_potential_indexes(resource_id)

            result = {
                "resource_id": resource_id,
                "created_at": resource["created_at"],
                "url": resource["url"],
                "profile": resource.get("profile", {}),
                "indexes": list(indexes) if indexes else None,
            }

            return CallToolResult(
                content=[TextContent(type="text", text=json.dumps(result, indent=2))]
            )

        except Exception as e:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Resource info error: {str(e)}")],
                isError=True,
            )

    async def _list_accessible_resources(self, arguments: dict[str, Any]) -> CallToolResult:
        """List accessible resources from configuration."""
        resources = self.resources_config.get("resources", [])
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(resources, indent=2))]
        )

    async def _list_resources(self) -> ListResourcesResult:
        """List available resources."""
        resources = []
        for resource_config in self.resources_config.get("resources", []):
            resources.append(
                Resource(
                    uri=f"resource://{resource_config['resource_id']}",
                    name=resource_config.get("name", resource_config["resource_id"]),
                    description=resource_config.get("description", ""),
                    mimeType="application/json",
                )
            )
        return ListResourcesResult(resources=resources)

    async def _read_resource(self, uri: str) -> ResourceContents:
        """Read a specific resource."""
        # This would typically read resource data
        return TextResourceContents(
            uri=uri, mimeType="application/json", text="Resource content would be here"
        )

    async def run(self):
        """Run the MCP server."""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="api-tabular-mcp",
                    server_version="1.0.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=None, experimental_capabilities={}
                    ),
                ),
            )


def create_server() -> TabularMCPServer:
    """Create and return a new MCP server instance."""
    return TabularMCPServer()


async def main():
    """Main entry point for the MCP server."""
    server = create_server()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
