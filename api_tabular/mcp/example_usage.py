#!/usr/bin/env python3
"""
Example usage of the MCP server tools.
This demonstrates how to use the MCP tools programmatically.
"""

import asyncio
import json

from api_tabular.mcp.server import create_server


async def example_usage():
    """Demonstrate MCP server usage."""
    print("=== MCP Server Example Usage ===\n")

    # Create server instance
    server = create_server()

    # 1. List available tools
    print("1. Available Tools:")
    tools_result = await server._list_tools()
    for tool in tools_result.tools:
        print(f"   - {tool.name}: {tool.description}")
    print()

    # 2. List accessible resources
    print("2. Accessible Resources:")
    resources_result = await server._list_accessible_resources({})
    resources = json.loads(resources_result.content[0].text)
    for resource in resources:
        print(f"   - {resource['name']} ({resource['resource_id']})")
        print(f"     Description: {resource['description']}")
    print()

    # 3. Get resource info (example with first resource)
    if resources:
        resource_id = resources[0]["resource_id"]
        print(f"3. Resource Info for {resource_id}:")
        info_result = await server._get_resource_info({"resource_id": resource_id})
        if info_result.isError:
            print(f"   Error: {info_result.content[0].text}")
        else:
            info = json.loads(info_result.content[0].text)
            print(f"   Created: {info.get('created_at', 'N/A')}")
            print(f"   URL: {info.get('url', 'N/A')}")
            print(f"   Indexes: {info.get('indexes', 'N/A')}")
    print()

    # 4. Query data (example - this would need a running database)
    print("4. Query Data Example:")
    print("   Note: This requires a running database with the resource data.")
    print("   Example query parameters:")
    example_query = {
        "resource_id": "aaaaaaaa-1111-bbbb-2222-cccccccccccc",
        "filters": {"score__greater": 0.5},
        "sort": {"column": "score", "order": "desc"},
        "page": 1,
        "page_size": 5,
    }
    print(f"   {json.dumps(example_query, indent=2)}")
    print()

    print("=== End Example ===")


if __name__ == "__main__":
    asyncio.run(example_usage())
