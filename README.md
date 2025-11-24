![Tabular API](banner.png)

# Tabular API

[![CircleCI](https://circleci.com/gh/datagouv/api-tabular.svg?style=svg)](https://app.circleci.com/pipelines/github/datagouv/api-tabular)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An API service that provides RESTful access to CSV or tabular data converted by [Hydra](https://github.com/datagouv/hydra). This service provides a REST API to access PostgreSQL database tables containing CSV data, offering HTTP querying capabilities, pagination, and data streaming for CSV or tabular resources.

This service is mainly used, developed and maintained by [data.gouv.fr](https://data.gouv.fr) - the France Open Data platform.
The production API is deployed on data.gouv.fr infrastructure at [`https://tabular-api.data.gouv.fr/api`](https://tabular-api.data.gouv.fr/api). See the [product documentation](https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-beta/) (in French) for usage details and the [technical documentation](https://tabular-api.data.gouv.fr/api/doc) for API reference.

## 🛠️ Installation & Setup

### 📋 Requirements

- **Python** >= 3.11, < 3.14
- **[uv](https://docs.astral.sh/uv/)** for dependency management
- **Docker & Docker Compose**

### 🧪 Run with a test database

1. **Start the Infrastructure**

   Start the test CSV database and test PostgREST container:
   ```shell
   docker compose --profile test up -d
   ```
   The `--profile test` flag tells Docker Compose to start the PostgREST and PostgreSQL services for the test CSV database. This starts PostgREST on port 8080, connecting to the test CSV database. You can access the raw PostgREST API on http://localhost:8080.

2. **Launch the main API proxy**

   Install dependencies and start the proxy services:
   ```shell
   uv sync
   uv run adev runserver -p8005 api_tabular/app.py        # Tabular API (port 8005)
   uv run adev runserver -p8006 api_tabular/metrics.py    # Metrics API (port 8006)
   # Optional: MCP server
   uv run api_tabular/mcp/server.py                       # MCP Server (port 8007)
   ```

   The main API provides a controlled layer over PostgREST - exposing PostgREST directly would be too permissive, so this adds a security and access control layer.

3. **Test the API**

   Query the API using a `resource_id`. Several test resources are available in the fake database:

   - **`aaaaaaaa-1111-bbbb-2222-cccccccccccc`** - Main test resource with 1000 rows
   - **`aaaaaaaa-5555-bbbb-6666-cccccccccccc`** - Resource with database indexes
   - **`dddddddd-7777-eeee-8888-ffffffffffff`** - Resource allowed for aggregation
   - **`aaaaaaaa-9999-bbbb-1010-cccccccccccc`** - Resource with indexes and aggregation allowed

### 🏭 Run with a real Hydra database

To use the API with a real database served by [Hydra](https://github.com/datagouv/hydra) instead of the fake test database:

1. **Start the real Hydra CSV database locally:**

   First, you need to have Hydra CSV database running locally. See the [Hydra repository](https://github.com/datagouv/hydra) for instructions on how to set it up. Make sure the Hydra CSV database is accessible on `localhost:5434`.

2. **Start PostgREST pointing to your local Hydra database:**
   ```shell
   docker compose --profile hydra up -d
   ```
   The `--profile hydra` flag tells Docker Compose to start the PostgREST service configured for the real Hydra CSV database (instead of the test services). This starts PostgREST on port 8080, connecting to your local Hydra CSV database.

3. **Configure the API to use it:**
   ```shell
   export PGREST_ENDPOINT="http://localhost:8080"
   ```

4. **Start the API services:**
   ```shell
   uv sync
   uv run adev runserver -p8005 api_tabular/app.py        # Tabular API (port 8005)
   uv run adev runserver -p8006 api_tabular/metrics.py    # Metrics API (port 8006)
   uv run api_tabular/mcp/server.py                       # MCP Server (port 8007)
   ```

5. **Use real resource IDs** from your Hydra database instead of the test IDs.

**Note:** Make sure your Hydra CSV database is accessible and the database schema matches the expected structure. The test database uses the `csvapi` schema, while real Hydra databases typically use the `public` schema.


## 📚 API Documentation

### Resource Endpoints

#### Get Resource Metadata
```http
GET /api/resources/{resource_id}/
```

Returns basic information about the resource including creation date, URL, and available endpoints.

**Example:**
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/
```

**Response:**
```json
{
  "created_at": "2023-04-21T22:54:22.043492+00:00",
  "url": "https://data.gouv.fr/datasets/example/resources/fake.csv",
  "links": [
    {
      "href": "/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/",
      "type": "GET",
      "rel": "profile"
    },
    {
      "href": "/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/",
      "type": "GET",
      "rel": "data"
    },
    {
      "href": "/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/",
      "type": "GET",
      "rel": "swagger"
    }
  ]
}
```

#### Get Resource Profile
```http
GET /api/resources/{resource_id}/profile/
```

Returns the CSV profile information (column types, headers, etc.) generated by [csv-detective](https://github.com/datagouv/csv-detective).

**Example:**
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/
```

**Response:**
```json
{
  "profile": {
    "header": [
        "id",
        "score",
        "decompte",
        "is_true",
        "birth",
        "liste"
    ]
  },
  "...": "..."
}
```

#### Get Resource Data
```http
GET /api/resources/{resource_id}/data/
```

Returns the actual data with support for filtering, sorting, and pagination.

**Example:**
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/
```

**Response:**
```json
{
  "data": [
    {
        "__id": 1,
        "id": " 8c7a6452-9295-4db2-b692-34104574fded",
        "score": 0.708,
        "decompte": 90,
        "is_true": false,
        "birth": "1949-07-16",
        "liste": "[0]"
    },
    ...
  ],
  "links": {
      "profile": "http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/",
      "swagger": "http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/",
      "next": "http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=2&page_size=20",
      "prev": null
  },
  "meta": {
      "page": 1,
      "page_size": 20,
      "total": 1000
  }
}
```

#### Get Resource Data as CSV
```http
GET /api/resources/{resource_id}/data/csv/
```

Streams the data directly as a CSV file for download.

#### Get Resource Data as JSON
```http
GET /api/resources/{resource_id}/data/json/
```

Streams the data directly as a JSON file for download.

#### Get Swagger Documentation
```http
GET /api/resources/{resource_id}/swagger/
```

Returns OpenAPI/Swagger documentation specific to this resource.

### Query Operators

The data endpoint can be queried with the following operators as query string (replacing `column_name` with the name of an actual column), if the column type allows it (see the swagger for each column's allowed parameter):

#### Filtering Operators
```
# exact value
column_name__exact=value

# differs
column_name__differs=value

# contains (for strings only)
column_name__contains=value

# in (value in list)
column_name__in=value1,value2,value3

# less
column_name__less=value

# greater
column_name__greater=value

# strictly less
column_name__strictly_less=value

# strictly greater
column_name__strictly_greater=value
```

#### Sorting
```
# sort by column
column_name__sort=asc
column_name__sort=desc
```

#### Aggregation Operators
> ⚠️ **WARNING**: Aggregation requests are only available for resources that are listed in the `ALLOW_AGGREGATION` list of the config file, which can be seen at the `/api/aggregation-exceptions/` endpoint.

```
# group by values
column_name__groupby

# count values
column_name__count

# mean / average
column_name__avg

# minimum
column_name__min

# maximum
column_name__max

# sum
column_name__sum
```

> **Note**: Passing an aggregation operator (`count`, `avg`, `min`, `max`, `sum`) returns a column that is named `<column_name>__<operator>` (for instance: `?birth__groupby&score__sum` will return a list of dicts with the keys `birth` and `score__sum`).

#### Pagination
```
page=1          # Page number (default: 1)
page_size=20    # Items per page (default: 20, max: 50)
```

#### Column Selection
```
columns=col1,col2,col3    # Select specific columns only
```

### Example Queries

#### Basic Filtering
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?score__greater=0.9&decompte__exact=13
```

**Returns:**
```json
{
  "data": [
    {
      "__id": 52,
      "id": " 5174f26d-d62b-4adb-a43a-c3b6288fa2f6",
      "score": 0.985,
      "decompte": 13,
      "is_true": false,
      "birth": "1980-03-23",
      "liste": "[0]"
    },
    {
      "__id": 543,
      "id": " 8705df7c-8a6a-49e2-9514-cf2fb532525e",
      "score": 0.955,
      "decompte": 13,
      "is_true": true,
      "birth": "1965-02-06",
      "liste": "[0, 1, 2]"
    }
  ],
  "links": {
    "profile": "http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/",
    "swagger": "http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/",
    "next": null,
    "prev": null
  },
  "meta": {
    "page": 1,
    "page_size": 20,
    "total": 2
  }
}
```

#### Aggregation with Filtering
With filters and aggregators (filtering is always done **before** aggregation, no matter the order in the parameters):
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?decompte__groupby&birth__less=1996&score__avg
```

i.e. `decompte` and average of `score` for all rows where `birth<="1996"`, grouped by `decompte`, returns:
```json
{
    "data": [
        {
            "decompte": 55,
            "score__avg": 0.7123333333333334
        },
        {
            "decompte": 27,
            "score__avg": 0.6068888888888889
        },
        {
            "decompte": 23,
            "score__avg": 0.4603333333333334
        },
        ...
    ]
}
```

#### Pagination
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=2&page_size=30
```

#### Column Selection
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?columns=id,score,birth
```

### Metrics API

The metrics service provides similar functionality for system metrics:

```shell
# Get metrics data
curl http://localhost:8006/api/{model}/data/

# Get metrics as CSV
curl http://localhost:8006/api/{model}/data/csv/
```

### Health Check

```shell
# Main API health
curl http://localhost:8005/health/

# Metrics API health
curl http://localhost:8006/health/
```

## ⚙️ Configuration

Configuration is handled through TOML files and environment variables. The default configuration is in `api_tabular/config_default.toml`.

### Key Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `PGREST_ENDPOINT` | `http://localhost:8080` | PostgREST server URL |
| `SERVER_NAME` | `localhost:8005` | Server name for URL generation |
| `SCHEME` | `http` | URL scheme (http/https) |
| `SENTRY_DSN` | `None` | Sentry DSN for error reporting (optional) |
| `PAGE_SIZE_DEFAULT` | `20` | Default page size |
| `PAGE_SIZE_MAX` | `50` | Maximum allowed page size |
| `BATCH_SIZE` | `50000` | Batch size for streaming |
| `DOC_PATH` | `/api/doc` | Swagger documentation path |
| `ALLOW_AGGREGATION` | `["dddddddd-7777-eeee-8888-ffffffffffff", "aaaaaaaa-9999-bbbb-1010-cccccccccccc"]` | List of resource IDs allowed for aggregation |

### Environment Variables

You can override any configuration value using environment variables:

```shell
export PGREST_ENDPOINT="http://my-postgrest:8080"
export PAGE_SIZE_DEFAULT=50
export SENTRY_DSN="https://your-sentry-dsn"
```

### Custom Configuration File

Create a `config.toml` file in the project root or set the `CSVAPI_SETTINGS` environment variable:

```shell
export CSVAPI_SETTINGS="/path/to/your/config.toml"
```

## 🤖 MCP Server

This project includes a Model Context Protocol (MCP) server for natural language access to data.gouv.fr datasets, built with [FastMCP](https://github.com/jlowin/fastmcp) and using Streamable HTTP transport protocol.

### Setup and Configuration

1. **Start the real Hydra CSV database locally:**

   First, you need to have Hydra CSV database running locally. See the [Hydra repository](https://github.com/datagouv/hydra) for instructions on how to set it up. Make sure the Hydra CSV database is accessible on `localhost:5434`.

2. **Start PostgREST pointing to your local Hydra database:**
   ```shell
   docker compose --profile hydra up -d
   ```
   The `--profile hydra` flag tells Docker Compose to start the PostgREST service configured for the real Hydra CSV database. This starts PostgREST on port 8080, connecting to your local Hydra CSV database.

3. **Configure the API endpoint:**
   ```shell
   export PGREST_ENDPOINT="http://localhost:8080"
   ```

4. **Start the HTTP MCP server:**

   The port must be specified via the `MCP_PORT` environment variable:
   ```bash
   MCP_PORT=8007 uv run api_tabular/mcp/server.py
   ```

> Note (production): For production deployments, run behind a TLS reverse proxy. Use environment variables to configure the host and port (e.g., `HOST=0.0.0.0 MCP_PORT=8007 uv run api_tabular/mcp/server.py`). Optionally restrict allowed origins and add token authentication at the proxy level.

### 🚀 Quick Start

1. **Test the server:**
   ```bash
   curl -X POST http://127.0.0.1:8007/mcp -H "Accept: application/json" -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}'
   ```

### 🔧 MCP client configuration

The MCP server configuration depends on your client. Use the appropriate configuration format for your client:

> **Note:** If you want to create or modify datasets/resources, you'll need a data.gouv.fr API key. You can get one from your [profile settings](https://www.data.gouv.fr/fr/account/). Add it to your client configuration as shown in the examples below.

#### Gemini CLI

```bash
gemini mcp add --transport http api-tabular http://127.0.0.1:8007/mcp
```

Alternatively, add the following to your `~/.gemini/settings.json` file:

```json
{
  "mcpServers": {
    "api-tabular": {
      "httpUrl": "http://127.0.0.1:8007/mcp",
      "args": {
        "apiKey": "your-data-gouv-api-key-here"
      }
    }
  }
}
```

#### Claude Desktop

Add the following to your Claude Desktop configuration file (typically `~/Library/Application Support/Claude/claude_desktop_config.json` on macOS, or `%APPDATA%\Claude\claude_desktop_config.json` on Windows):

```json
{
  "mcpServers": {
    "data-gouv": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://127.0.0.1:8007/mcp",
        "--header",
        "X-MCP-Config: {\"apiKey\":\"your-data-gouv-api-key-here\"}"
      ]
    }
  }
}
```

Or if your client supports direct configuration:

```json
{
  "mcpServers": {
    "data-gouv": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://127.0.0.1:8007/mcp"
      ],
      "config": {
        "apiKey": "your-data-gouv-api-key-here"
      }
    }
  }
}
```

#### VS Code

Add the following to your VS Code `settings.json`:

```json
{
  "servers": {
    "data-gouv": {
      "url": "http://127.0.0.1:8007/mcp",
      "type": "http",
      "config": {
        "apiKey": "your-data-gouv-api-key-here"
      }
    }
  }
}
```

#### Windsurf

Add the following to your `~/.codeium/mcp_config.json`:

```json
{
  "mcpServers": {
    "data-gouv": {
      "serverUrl": "http://127.0.0.1:8007/mcp",
      "config": {
        "apiKey": "your-data-gouv-api-key-here"
      }
    }
  }
}
```

#### Cursor

Cursor supports MCP servers through its settings. To configure the server:

1. Open Cursor Settings (Cmd/Ctrl + ,)
2. Search for "MCP" or "Model Context Protocol"
3. Add a new MCP server with the following configuration:

```json
{
  "mcpServers": {
    "data-gouv": {
      "url": "http://127.0.0.1:8007/mcp",
      "transport": "http",
      "config": {
        "apiKey": "your-data-gouv-api-key-here"
      }
    }
  }
}
```

**Note:**
- Replace `http://127.0.0.1:8007/mcp` with your actual server URL if running on a different host or port. For production deployments, use `https://` and configure the appropriate hostname.
- Replace `your-data-gouv-api-key-here` with your actual API key from [data.gouv.fr account settings](https://www.data.gouv.fr/fr/account/).
- The API key is only required for tools that create or modify datasets/resources. Read-only operations (like `search_datasets`) work without an API key.

### 🧭 Test with MCP Inspector

Use the official MCP Inspector to interactively test the server tools and resources.

Prerequisites:
- Node.js with `npx` available

Steps:
1. Start the MCP server (see above):
   ```bash
   uv run api_tabular/mcp/server.py
   ```
2. In another terminal, launch the inspector with the provided config:
   ```bash
   npx @modelcontextprotocol/inspector --config ./api_tabular/mcp/mcp_config.json --server api-tabular
   ```
   - This connects to `http://127.0.0.1:8007/mcp` as defined in `api_tabular/mcp/mcp_config.json`.
   - If the server port changes, update the config file accordingly.

### 🚚 Transport support

This MCP server uses FastMCP and implements the Streamable HTTP transport only.
STDIO and SSE are not supported.

Use Streamable HTTP at `http://127.0.0.1:8007/mcp` in clients (e.g. MCP Inspector).

### 📋 Available Endpoints

**Streamable HTTP transport (standards-compliant):**
- `POST /mcp` - JSON-RPC messages (client → server)

### 🛠️ Available Tools

The MCP server provides tools to interact with data.gouv.fr datasets:

- **`search_datasets`** - Search for datasets on data.gouv.fr by keywords. Returns a list of datasets matching the search query with their metadata, including title, description, organization, tags, and resource count. Use this to discover datasets before querying their data.

  Parameters:
  - `query` (required): Search query string (searches in title, description, tags)
  - `page` (optional, default: 1): Page number
  - `page_size` (optional, default: 20, max: 100): Number of results per page

### 🧪 Test the MCP server

```bash
# Test the HTTP MCP server
uv run python api_tabular/mcp/test_mcp.py
```


## 🧪 Testing

This project uses [pytest](https://pytest.org/) for testing with async support and mocking capabilities. You must have the two test containers running for the tests to run (see [### 🧪 Run with a test database](#-run-with-a-test-database) for setup instructions).

### Running Tests

```shell
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_api.py

# Run tests with verbose output
uv run pytest -v

# Run tests and show print statements
uv run pytest -s
```

### Tests Structure

- **`tests/test_api.py`** - API endpoint tests (actually pings the running API)
- **`tests/test_config.py`** - Configuration loading tests
- **`tests/test_query.py`** - Query building and processing tests
- **`tests/test_swagger.py`** - Swagger documentation tests (actually pings the running API)
- **`tests/test_utils.py`** - Utility function tests
- **`tests/conftest.py`** - Test fixtures and configuration

### CI/CD Testing

Tests are automatically run in CI/CD. See [`.circleci/config.yml`](.circleci/config.yml) for the complete CI/CD configuration.

## 🤝 Contributing

### 🧹 Code Linting and Formatting

This project follows PEP 8 style guidelines using [Ruff](https://astral.sh/ruff/) for linting and formatting. **Either running these commands manually or installing the pre-commit hook is required before submitting contributions.**

```shell
# Lint and sort imports, and format code
uv run ruff check  --select I --fix && uv run ruff format
```

### 🔗 Pre-commit Hooks

This repository uses a [pre-commit](https://pre-commit.com/) hook which lint and format code before each commit. **Installing the pre-commit hook is required for contributions.**

**Install pre-commit hooks:**
```shell
uv run pre-commit install
```
The pre-commit hook that automatically:
- Check YAML syntax
- Fix end-of-file issues
- Remove trailing whitespace
- Check for large files
- Run Ruff linting and formatting

### 🧪 Running Tests

**Pull requests cannot be merged unless all CI/CD tests pass.**
Tests are automatically run on every pull request and push to main branch. See [`.circleci/config.yml`](.circleci/config.yml) for the complete CI/CD configuration, and the [🧪 Testing](#-testing) section above for detailed testing commands.

### 🏷️ Releases and versioning

The release process uses the [`tag_version.sh`](tag_version.sh) script to create git tags, GitHub releases and update [CHANGELOG.md](CHANGELOG.md) automatically. Package version numbers are automatically derived from git tags using [setuptools_scm](https://github.com/pypa/setuptools_scm), so no manual version updates are needed in `pyproject.toml`.

**Prerequisites**: [GitHub CLI](https://cli.github.com/) must be installed and authenticated, and you must be on the main branch with a clean working directory.

```bash
# Create a new release
./tag_version.sh <version>

# Example
./tag_version.sh 2.5.0

# Dry run to see what would happen
./tag_version.sh 2.5.0 --dry-run
```

The script automatically:
- Extracts commits since the last tag and formats them for CHANGELOG.md
- Identifies breaking changes (commits with `!:` in the subject)
- Creates a git tag and pushes it to the remote repository
- Creates a GitHub release with the changelog content

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/datagouv/api-tabular/issues)
- **Discussion**: Use the discussion section at the end of the [production API page](https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-beta/)
- **Contact Form**: [Support form](https://support.data.gouv.fr/)

## 🌐 Production Resources

- **Production API**: [`https://tabular-api.data.gouv.fr/api`](https://tabular-api.data.gouv.fr/api)
- **Product Documentation**: [API tabulaire data.gouv.fr (beta)](https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-beta/) (in French)
- **Technical Documentation**: [Swagger/OpenAPI docs](https://tabular-api.data.gouv.fr/api/doc)
