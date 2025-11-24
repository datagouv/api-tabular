#!/usr/bin/env python3
"""MCP server for data.gouv.fr using FastMCP."""

from mcp.server.fastmcp import FastMCP

from api_tabular.mcp import datagouv_api_client

# Create an MCP server
mcp = FastMCP("data.gouv.fr API", json_response=True)


@mcp.tool()
async def search_datasets(query: str, page: int = 1, page_size: int = 20) -> str:
    """
    Search for datasets on data.gouv.fr by keywords.

    Returns a list of datasets matching the search query with their metadata,
    including title, description, organization, tags, and resource count.
    Use this to discover datasets before querying their data.

    Args:
        query: Search query string (searches in title, description, tags)
        page: Page number (default: 1)
        page_size: Number of results per page (default: 20, max: 100)

    Returns:
        Formatted text with dataset information
    """
    result = await datagouv_api_client.search_datasets(query=query, page=page, page_size=page_size)

    # Format the result as text content
    datasets = result.get("data", [])
    if not datasets:
        return f"No datasets found for query: '{query}'"

    content_parts = [
        f"Found {result.get('total', len(datasets))} dataset(s) for query: '{query}'",
        f"Page {result.get('page', 1)} of results:\n",
    ]
    for i, ds in enumerate(datasets, 1):
        content_parts.append(f"{i}. {ds.get('title', 'Untitled')}")
        content_parts.append(f"   ID: {ds.get('id')}")
        if ds.get("description_short"):
            desc = ds.get("description_short", "")[:200]
            content_parts.append(f"   Description: {desc}...")
        if ds.get("organization"):
            content_parts.append(f"   Organization: {ds.get('organization')}")
        if ds.get("tags"):
            tags = ", ".join(ds.get("tags", [])[:5])
            content_parts.append(f"   Tags: {tags}")
        content_parts.append(f"   Resources: {ds.get('resources_count', 0)}")
        content_parts.append(f"   URL: {ds.get('url')}")
        content_parts.append("")

    return "\n".join(content_parts)


# Run with streamable HTTP transport
if __name__ == "__main__":
    import os
    import sys

    # Get port from environment variable
    if "MCP_PORT" not in os.environ:
        print("Error: MCP_PORT environment variable must be set", file=sys.stderr)
        print("Usage: MCP_PORT=8007 uv run api_tabular/mcp/server.py", file=sys.stderr)
        sys.exit(1)

    try:
        port = int(os.environ["MCP_PORT"])
    except ValueError:
        print(
            f"Error: Invalid MCP_PORT environment variable: {os.environ['MCP_PORT']}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Use uvicorn to run the Starlette app with custom port
    import uvicorn

    uvicorn.run(mcp.streamable_http_app(), host="0.0.0.0", port=port, log_level="info")
