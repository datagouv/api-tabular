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

    print("\nTesting ask_data_question...")
    # Test with a specific question about weather data (we saw sg-metro-opendata files)
    question = "Données météo du métro parisien"
    print(f"Question: {question}")

    # Debug step by step
    print("\n🔍 Step 1: Extracting keywords...")
    keywords = server._extract_keywords(question)
    print(f"Keywords extracted: {keywords}")

    print("\n🔍 Step 2: Finding matching resource...")
    best_match = server._find_matching_resource(keywords)
    if best_match:
        print("Best match found:")
        print(f"  - Dataset: {best_match['dataset']['name']}")
        print(f"  - Resource: {best_match['resource']['name']}")
        print(f"  - Resource ID: {best_match['resource']['resource_id']}")
        print(f"  - Score: {best_match['score']}")
    else:
        print("No matching resource found")

    print("\n🔍 Step 3: Building query from question...")
    if best_match:
        query_parts = server._build_query_from_question(question, best_match)
        print(f"Query parts: {query_parts}")

    print("\n🔍 Step 4: Determining limit...")
    limit = server._determine_limit(question)
    print(f"Determined limit: {limit}")

    print("\n🔍 Step 5: Running full question...")
    question_result = await server._ask_data_question({"question": question, "limit": 5})
    print(f"Question result: {question_result.content[0].text}")
    print(f"Is error: {question_result.isError}")


if __name__ == "__main__":
    asyncio.run(test_mcp_server())
