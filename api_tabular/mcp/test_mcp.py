#!/usr/bin/env python3
"""
Test script for the MCP server.
"""

import asyncio

from api_tabular.mcp.server import create_server


async def test_mcp_server():
    """Test the MCP server functionality."""
    print("Creating MCP server...")
    server = create_server()

    print("Testing list_tools...")
    tools_result = await server._list_tools()
    print(f"Available tools: {[tool.name for tool in tools_result.tools]}")

    print("\nTesting list_accessible_resources...")
    resources_result = await server._list_accessible_resources({})
    print(f"Resources result: {resources_result.content[0].text}")

    print("\nTesting list_resources...")
    resources = await server._list_resources()
    print(f"Available resources: {[r.name for r in resources.resources]}")


if __name__ == "__main__":
    asyncio.run(test_mcp_server())
