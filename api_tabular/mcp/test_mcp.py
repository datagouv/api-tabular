#!/usr/bin/env python3
"""
Test script for the MCP server.
"""

import asyncio

from api_tabular.mcp.server import create_server


async def test_list_tools():
    """Test the list_tools functionality."""
    print("=== Testing list_tools ===")
    server = create_server()

    tools_result = await server._list_tools()
    print(f"Available tools: {[tool.name for tool in tools_result.tools]}")

    for tool in tools_result.tools:
        print(f"  - {tool.name}: {tool.description}")

    print("✅ list_tools test completed\n")


async def test_list_accessible_resources():
    """Test the list_accessible_resources functionality."""
    print("=== Testing list_accessible_resources ===")
    server = create_server()

    resources_result = await server._list_accessible_resources({})
    print(f"Resources result type: {type(resources_result.content[0].text)}")
    print(f"Resources result length: {len(resources_result.content[0].text)}")

    # Show first few characters of the result
    result_text = resources_result.content[0].text
    print(f"First 200 characters: {result_text[:200]}...")

    print("✅ list_accessible_resources test completed\n")


async def test_list_resources():
    """Test the list_resources functionality."""
    print("=== Testing list_resources ===")
    server = create_server()

    resources = await server._list_resources()
    print(f"Available resources count: {len(resources.resources)}")
    print(f"First 5 resources: {[r.name for r in resources.resources[:5]]}")

    for i, resource in enumerate(resources.resources[:3]):
        print(f"  {i+1}. {resource.name} ({resource.uri})")
        print(f"     Description: {resource.description}")
        print(f"     MIME Type: {resource.mimeType}")

    print("✅ list_resources test completed\n")


async def test_ask_data_question():
    """Test the ask_data_question functionality."""
    print("=== Testing ask_data_question ===")
    server = create_server()

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

    print("✅ ask_data_question test completed\n")


async def run_all_tests():
    """Run all test functions."""
    print("🚀 Starting MCP Server Tests\n")

    await test_list_tools()
    await test_list_accessible_resources()
    await test_list_resources()
    await test_ask_data_question()  # Now enabled for testing

    print("🎉 All tests completed!")


if __name__ == "__main__":
    asyncio.run(run_all_tests())
