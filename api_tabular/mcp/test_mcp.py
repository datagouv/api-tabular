#!/usr/bin/env python3
"""
Test script for the HTTP MCP server.
"""

import asyncio

from aiohttp import ClientSession


async def test_http_mcp_server():
    """Test the HTTP MCP server endpoints."""
    base_url = "http://localhost:8082"

    async with ClientSession() as session:
        print("🧪 Testing HTTP MCP Server")
        print("=" * 50)

        # Test health check
        print("\n1. Testing health check...")
        async with session.get(f"{base_url}/health") as response:
            if response.status == 200:
                data = await response.json()
                print(f"✅ Health check: {data}")
            else:
                print(f"❌ Health check failed: {response.status}")
                return False

        # Test MCP initialize
        print("\n2. Testing MCP initialize...")
        init_data = {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "1.0.0"},
        }
        async with session.post(f"{base_url}/mcp/initialize", json=init_data) as response:
            if response.status == 200:
                data = await response.json()
                print(f"✅ MCP initialize: {data}")
            else:
                print(f"❌ MCP initialize failed: {response.status}")
                return False

        # Test list tools
        print("\n3. Testing list tools...")
        async with session.post(f"{base_url}/mcp/tools/list", json={}) as response:
            if response.status == 200:
                data = await response.json()
                print(f"✅ List tools: {len(data.get('tools', []))} tools found")
                for tool in data.get("tools", []):
                    print(f"   - {tool['name']}: {tool['description']}")
            else:
                print(f"❌ List tools failed: {response.status}")
                return False

        # Test call tool - list_datagouv_resources
        print("\n4. Testing call tool (list_datagouv_resources)...")
        tool_data = {"name": "list_datagouv_resources", "arguments": {}}
        async with session.post(f"{base_url}/mcp/tools/call", json=tool_data) as response:
            if response.status == 200:
                data = await response.json()
                print(f"✅ Call tool: {len(data.get('content', []))} content items")
                if data.get("content"):
                    content = data["content"][0]
                    print(f"   Content type: {content.get('type')}")
                    print(f"   Content length: {len(content.get('text', ''))}")
            else:
                print(f"❌ Call tool failed: {response.status}")
                return False

        # Test call tool - ask_datagouv_question
        print("\n5. Testing call tool (ask_datagouv_question)...")
        tool_data = {
            "name": "ask_datagouv_question",
            "arguments": {"question": "Données météo du métro parisien", "limit": 10},
        }
        async with session.post(f"{base_url}/mcp/tools/call", json=tool_data) as response:
            if response.status == 200:
                data = await response.json()
                print(f"✅ Call tool: {len(data.get('content', []))} content items")
                if data.get("content"):
                    content = data["content"][0]
                    print(f"   Content: {content.get('text', '')[:200]}...")
            else:
                print(f"❌ Call tool failed: {response.status}")
                return False

        # Test list resources
        print("\n6. Testing list resources...")
        async with session.post(f"{base_url}/mcp/resources/list", json={}) as response:
            if response.status == 200:
                data = await response.json()
                print(f"✅ List resources: {len(data.get('resources', []))} resources found")
            else:
                print(f"❌ List resources failed: {response.status}")
                return False

        print("\n🎉 HTTP MCP Server tests completed!")
        return True


async def run_all_tests():
    """Run all test functions."""
    print("🚀 Starting HTTP MCP Server Tests\n")

    # Test HTTP server
    http_success = await test_http_mcp_server()

    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    print(f"   HTTP MCP Server: {'✅ PASSED' if http_success else '❌ FAILED'}")

    if http_success:
        print("\n🎉 All tests completed successfully!")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        return False


if __name__ == "__main__":
    asyncio.run(run_all_tests())
