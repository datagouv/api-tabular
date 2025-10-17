# MCP Server for API Tabular

This module provides a Model Context Protocol (MCP) server for accessing tabular data using the same core logic as the REST API.

## Overview

The MCP server exposes three main tools:

1. **`query_tabular_data`** - Query data from a resource with filtering, sorting, and pagination
2. **`get_resource_info`** - Get metadata and profile information for a resource
3. **`list_accessible_resources`** - List all accessible resources from configuration

## Usage

### Running the MCP Server

```bash
# Run the MCP server
uv run python -m api_tabular.mcp.cli

# Or run directly
uv run python api_tabular/mcp/cli.py
```

### Testing the Server

```bash
# Run the test script
uv run python -m api_tabular.mcp.test_mcp
```

## Configuration

The server reads accessible resources from `api_tabular/mcp/resources.json`:

```json
{
  "description": "Configuration file for accessible resources in MCP server",
  "resources": [
    {
      "resource_id": "aaaaaaaa-1111-bbbb-2222-cccccccccccc",
      "name": "Sample Dataset 1",
      "description": "A sample dataset for testing",
      "dataset_id": "sample-dataset-1"
    }
  ]
}
```

## Tools

### query_tabular_data

Query tabular data with advanced filtering and sorting capabilities.

**Parameters:**
- `resource_id` (required): The resource ID to query
- `filters` (optional): Filter conditions (e.g., `{'score__greater': 0.9}`)
- `sort` (optional): Sort configuration (e.g., `{'column': 'score', 'order': 'desc'}`)
- `page` (optional): Page number (default: 1)
- `page_size` (optional): Items per page (default: 20, max: 50)
- `columns` (optional): Specific columns to return

**Example:**
```json
{
  "resource_id": "aaaaaaaa-1111-bbbb-2222-cccccccccccc",
  "filters": {"score__greater": 0.9},
  "sort": {"column": "score", "order": "desc"},
  "page": 1,
  "page_size": 10
}
```

### get_resource_info

Get comprehensive metadata and profile information for a resource.

**Parameters:**
- `resource_id` (required): The resource ID to get information for

**Example:**
```json
{
  "resource_id": "aaaaaaaa-1111-bbbb-2222-cccccccccccc"
}
```

### list_accessible_resources

List all resources available through the MCP server.

**Parameters:** None

## Architecture

The MCP server uses the same core logic as the REST API:

- **`DataAccessor`**: Database operations and data retrieval
- **`QueryBuilder`**: SQL query construction and validation
- **Same error handling**: Consistent with REST API behavior

This ensures that MCP tools and REST API endpoints behave identically when accessing the same data.

## Integration

The MCP server is designed to be used alongside the existing REST API:

- **Shared Core Logic**: Both use `api_tabular.core` modules
- **Consistent Behavior**: Same query processing and error handling
- **Independent Deployment**: Can run separately from the REST API
- **Future Extensibility**: Easy to add more tools or integrate with external APIs

## Development

To add new MCP tools:

1. Add the tool definition to `_list_tools()`
2. Add the tool handler to `_call_tool()`
3. Implement the tool logic using core modules
4. Update this documentation

The server follows the MCP specification and can be used with any MCP-compatible client.
