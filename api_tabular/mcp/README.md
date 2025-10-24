# MCP Server for API Tabular

This module provides a Model Context Protocol (MCP) server for accessing tabular data using the same core logic as the REST API. The server enables natural language queries over French open data resources.

## Overview

The MCP server exposes two main tools:

1. **`list_accessible_resources`** - Browse all available datasets and resources
2. **`ask_data_question`** - Ask natural language questions about available datasets and get data results

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

The server reads accessible resources from `api_tabular/mcp/data/mcp_available_resources.json`:

```json
[
  {
    "dataset_id": "53698f1aa3a729239d203653",
    "name": "Associations Reconnues d'Utilité Publique",
    "resources": [
      {
        "resource_id": "5b43eaff-d97e-4a2c-b37d-0c8e6a79c925",
        "name": "base-arup-pour-dtnum-21.11.24-vdef-vu-baf.xlsx"
      }
    ]
  }
]
```

## Tools

### list_accessible_resources

Browse all available datasets and resources from the configuration.

**Parameters:** None

**Returns:** A JSON array of datasets with their associated resources.

### ask_data_question

Ask natural language questions about available datasets and get data results.

**Parameters:**
- `question` (required): Natural language question about the data
- `limit` (optional): Maximum number of results to return (default: auto-determined, max: 100)

**Example:**
```json
{
  "question": "Quels sont les départements avec le plus d'associations reconnues d'utilité publique ?",
  "limit": 20
}
```

**Features:**
- **Natural Language Processing**: Extracts keywords from French questions
- **Smart Resource Matching**: Finds the best matching dataset based on keywords
- **Intelligent Query Building**: Automatically builds appropriate queries based on question intent
- **Aggregation Support**: Detects when questions ask for aggregations (counts, averages, etc.)
- **Comprehensive Results**: Searches through all pages to provide complete results
- **Auto-determined Limits**: Automatically sets appropriate result limits based on question type

## Architecture

The MCP server uses the same core logic as the REST API:

- **`DataAccessor`**: Database operations and data retrieval
- **`QueryBuilder`**: SQL query construction and validation
- **Natural Language Processing**: Keyword extraction and resource matching
- **Same error handling**: Consistent with REST API behavior

This ensures that MCP tools and REST API endpoints behave identically when accessing the same data.

## Natural Language Processing

The server includes sophisticated NLP capabilities for French text:

- **Keyword Extraction**: Removes French stop words and extracts meaningful terms
- **Resource Matching**: Calculates similarity scores between questions and dataset names
- **Query Intent Detection**: Identifies aggregation, sorting, and filtering needs
- **Auto-limit Determination**: Sets appropriate result limits based on question type

### Supported Question Types

- **Aggregation queries**: "Quels sont les départements avec le plus d'associations ?"
- **Search queries**: "Trouver les associations dans le domaine de l'éducation"
- **Specific lookups**: "Qu'est-ce que l'association X ?"
- **Sorting queries**: "Montrer les départements par ordre décroissant"

## Integration

The MCP server is designed to be used alongside the existing REST API:

- **Shared Core Logic**: Both use `api_tabular.core` modules
- **Consistent Behavior**: Same query processing and error handling
- **Independent Deployment**: Can run separately from the REST API
- **Natural Language Interface**: Provides conversational access to data
- **Future Extensibility**: Easy to add more tools or integrate with external APIs

## Development

To add new MCP tools:

1. Add the tool definition to `_list_tools()`
2. Add the tool handler to `_call_tool()`
3. Implement the tool logic using core modules
4. Update this documentation

The server follows the MCP specification and can be used with any MCP-compatible client.

## Data Sources

The server currently provides access to French open data resources including:

- **Associations Reconnues d'Utilité Publique**: Public utility associations
- **Accidents corporels de la circulation routière**: Road traffic accidents
- **And many more datasets** from the top 100 reuses on data.gouv.fr

All data is sourced from the official French open data platform and processed through the same pipeline as the REST API.
