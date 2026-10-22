![Tabular API](banner.png)

# Tabular API

[![CI](https://github.com/datagouv/api-tabular/actions/workflows/ci.yml/badge.svg)](https://github.com/datagouv/api-tabular/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An API service that provides RESTful access to CSV or tabular data converted by [Hydra](https://github.com/datagouv/hydra). Hydra converts each tabular resource to a `<resource_id>.parquet` file in object storage; this service reads those files directly with an embedded [DuckDB](https://duckdb.org/), and offers HTTP querying capabilities, pagination, and data streaming on top of them.

```
client --> FastAPI (query string -> SQL) --> DuckDB (embedded, httpfs)
                                               --> <PARQUET_BASE_URL>/<resource_id>.parquet
```

Any parquet file of the bucket is queryable as is, with no ingestion and no index table: the resource id in the URL is turned into an object URL. `PARQUET_BASE_URL` defaults to the public OVH bucket `https://hydra.s3.rbx.io.cloud.ovh.net/parquet`, which is read over plain HTTPS with no credentials.

This service is mainly used, developed and maintained by [data.gouv.fr](https://data.gouv.fr) - the France Open Data platform.
The production API is deployed on data.gouv.fr infrastructure at [`https://tabular-api.data.gouv.fr/api`](https://tabular-api.data.gouv.fr/api). See the [product documentation](https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-fr-beta/) (in French) for usage details and the [technical documentation](https://tabular-api.data.gouv.fr/api/doc) for API reference.

## 🛠️ Installation & Setup

### 📋 Requirements

- **Python** >= 3.11, < 3.14
- **[uv](https://docs.astral.sh/uv/)** for dependency management
- **Docker & Docker Compose** (optional)

### 🐳 Run with Docker

```shell
docker compose up -d --build
```

A single container starts: the FastAPI app with DuckDB embedded, on http://localhost:8005.
Documentation: http://localhost:8005/api/doc

```shell
RID=3b6b2281-b9d9-4959-ae9d-c2c166dff118
curl "http://localhost:8005/api/resources/$RID/data/?page_size=5"
```

### 💻 Run without Docker

```shell
uv sync
uv run uvicorn tabular_duckdb.main:app --port 8005 --reload
```

Configuration comes from environment variables or a `.env` file, see [.env.example](.env.example) and the [⚙️ Configuration](#️-configuration) section; the defaults point at the public OVH bucket. Another bucket only needs `PARQUET_BASE_URL`, and `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_ENDPOINT` if it is private (the base URL can then be an `s3://` one).

Any resource converted by Hydra can be queried with its real resource id, as long as its `<resource_id>.parquet` file is in the bucket.

## 📚 API Documentation

| Endpoint | Description |
|----------|-------------|
| `GET /api/resources/{rid}/` | metadata and links |
| `GET /api/resources/{rid}/profile/` | column types and statistics |
| `GET /api/resources/{rid}/swagger/` | OpenAPI spec for this resource, with the filters allowed per column |
| `GET /api/resources/{rid}/data/` | paginated JSON data, with filtering and sorting |
| `GET /api/resources/{rid}/data/csv/` | same, streamed as CSV |
| `GET /api/resources/{rid}/data/json/` | same, streamed as JSON |
| `GET /api/resources/{rid}/sql/?q=...` | read-only SQL query on the resource |
| `GET /health/` | health of DuckDB and reachability of the storage |

### Resource Endpoints

#### Get Resource Metadata
```http
GET /api/resources/{resource_id}/
```

Returns basic information about the resource and the available endpoints. `created_at` is the parquet file's last modification date.

**Response:**
```json
{
  "created_at": "2026-10-19T16:23:08+00:00",
  "url": "https://www.data.gouv.fr/api/1/datasets/r/{resource_id}",
  "links": [
    {"href": "http://localhost:8005/api/resources/{resource_id}/profile/", "type": "GET", "rel": "profile"},
    {"href": "http://localhost:8005/api/resources/{resource_id}/data/", "type": "GET", "rel": "data"},
    {"href": "http://localhost:8005/api/resources/{resource_id}/swagger/", "type": "GET", "rel": "swagger"},
    {"href": "http://localhost:8005/api/resources/{resource_id}/sql/", "type": "GET", "rel": "sql"}
  ]
}
```

#### Get Resource Profile
```http
GET /api/resources/{resource_id}/profile/
```

Returns the column types and statistics, derived from the parquet file (schema, row count and DuckDB `SUMMARIZE`).

#### Get Resource Data
```http
GET /api/resources/{resource_id}/data/
```

Returns the actual data with support for filtering, sorting, and pagination.

**Response:**
```json
{
  "data": [
    {"__id": 1, "...": "..."},
    ...
  ],
  "links": {
    "profile": "http://localhost:8005/api/resources/{resource_id}/profile/",
    "swagger": "http://localhost:8005/api/resources/{resource_id}/swagger/",
    "next": "http://localhost:8005/api/resources/{resource_id}/data/?page=2&page_size=20",
    "prev": null
  },
  "meta": {
    "page": 1,
    "page_size": 20,
    "total": 1027
  }
}
```

Each row carries a synthetic `__id` column (the row number in the parquet file), which is also the default sort.

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

The data endpoint can be queried with the following operators as query string (replacing `column_name` with the name of an actual column), if the column type allows it (see the swagger for each column's allowed parameters):

#### Filtering Operators
```
# exact
column_name__exact=value

# differs
column_name__differs=value

# is `null`
column_name__isnull

# is not `null`
column_name__isnotnull

# contains
column_name__contains=value

# does not contain (value does not contain)
column_name__notcontains=value

# in (value in list)
column_name__in=value1,value2,value3

# notin (value not in list)
column_name__notin=value1,value2,value3

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

> **Note**: text is sorted and compared (`sort`, `less`, `greater`, `strictly_less`, `strictly_greater`, `min`, `max`) in the French order, where accents and case only break ties (`Eléonore` comes before `Elie`). A space or a hyphen sorts before letters (`Anne Marie`, then `Anne-Flore`, then `Annette`). `exact`, `differs` and `in` stay exact matches.

#### Aggregation Operators
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

> **Note**: Passing an aggregation operator (`count`, `avg`, `min`, `max`, `sum`) returns a column that is named `<column_name>__<operator>` (for instance: `?birth__groupby&score__sum` will return a list of dicts with the keys `birth` and `score__sum`). Filtering is always done **before** aggregation, no matter the order in the parameters. An aggregated response has no `meta.total`, and `links.next` is set as long as the page is full. `columns` cannot be set alongside aggregators.

> ⚠️ **WARNING**: columns that contain **JSON** objects (see the `profile` to know which ones do) **do not support filtering nor aggregation** for now, except `isnull` and `isnotnull`.

#### Pagination
```
page=1          # Page number (default: 1)
page_size=20    # Items per page (default: 20, max: 50)
```

#### Column Selection
```
columns=col1,col2,col3    # Select specific columns only
```

#### Nested conditions
Top level parameters are considered joined with the `AND` operator. It is possible to add nested conditions with the following syntax:
```
or=(<column1>__<operator1>.<value1>,<column2>__<operator2>.<value2>,...)
```
> **Note**: the conditions within an `OR` have a `.` instead of a `=` between the operator and the value.

> **Note**: the syntax for `isnull` and `isnotnull` is `<column1>__is(not)null` (no value is needed, like outside of `OR` groups)

This scales as much as needed, but the inner operators should be `or(...)` and `and(...)` (without the `=`), like:
```
decompte__less=10&or=(is_true__exact.true,birth__greater.1980,and(score__strictly_less.5,liste__isnotnull,or(...)))
```
> **Note**: if the column name and/or the value of the filter contains special characters (including dots), the item should be wrapped within double quotes:
```
or=("is true"__exact.true,and(score__strictly_less."0.1","avg.views"__greater."15.5"))
```

### Example Queries

```shell
# filters
curl "http://localhost:8005/api/resources/$RID/data/?Code%20du%20d%C3%A9partement__exact=62"
curl "http://localhost:8005/api/resources/$RID/data/?Nom%20de%20l%27%C3%A9lu__contains=MARTIN"

# nested conditions
curl "http://localhost:8005/api/resources/$RID/data/?or=(\"Code%20du%20d%C3%A9partement\"__exact.62,\"Code%20du%20d%C3%A9partement\"__exact.59)"

# sort, pagination, column selection
curl "http://localhost:8005/api/resources/$RID/data/?Date%20de%20naissance__sort=desc&page=2&page_size=50"
curl "http://localhost:8005/api/resources/$RID/data/?columns=Code%20du%20d%C3%A9partement,Nom%20de%20l%27%C3%A9lu"

# number of elected officials per department, among women
curl "http://localhost:8005/api/resources/$RID/data/?Code%20sexe__exact=F&Code%20du%20d%C3%A9partement__groupby&Nom%20de%20l%27%C3%A9lu__count"
```

### SQL Endpoint

Runs a read-only SQL query, where the resource is the table `resource`.

```shell
curl -G "http://localhost:8005/api/resources/$RID/sql/" \
  --data-urlencode 'q=SELECT "Code du département" AS dep, count(*) AS n FROM resource GROUP BY 1 ORDER BY n DESC LIMIT 5'
```

```json
{
  "data": [{"dep": "62", "n": 13242}, {"dep": "59", "n": 12325}],
  "meta": {"table": "resource", "rows": 5, "truncated": false, "row_limit": 10000, "execution_ms": 84}
}
```

The endpoint is read-only, and the protection comes from DuckDB itself rather than from an analysis of the query:

- the process runs a single DuckDB instance, sandboxed with `enable_external_access=false` and only `PARQUET_BASE_URL` in `allowed_directories`: local files, other hosts, attached databases and extension installation are all out of reach, and `lock_configuration=true` prevents a query from loosening this;
- its database is an empty file opened read-only, so that no query can change anything shared by the others;
- every query runs on a connection of its own, where `resource` is a temporary view: whatever a query creates (temporary tables, macros...) disappears with its connection;
- the query must be a single `SELECT` statement, its result is capped at `SQL_MAX_ROWS` rows and `SQL_MAX_BYTES` bytes of JSON (`meta.truncated` says whether a cap was hit), and it is interrupted after `SQL_TIMEOUT` seconds;
- DuckDB error messages are filtered so that the credentials do not leak.

A query can read any parquet file of the prefix, not only its resource: they are all public data. DuckDB does not resolve `..` in a URL before checking `allowed_directories`, so with a private bucket, a query can read whatever its credentials can read.

### Health Check

```shell
curl http://localhost:8005/health/
```

## ⚙️ Configuration

Configuration is handled through environment variables or a `.env` file (see [.env.example](.env.example)).

| Variable | Default | Description |
|----------|---------|-------------|
| `PARQUET_BASE_URL` | `https://hydra.s3.rbx.io.cloud.ovh.net/parquet` | prefix holding the `<resource_id>.parquet` files |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | empty | only for a private bucket, creates a DuckDB S3 secret |
| `S3_ENDPOINT` / `S3_REGION` / `S3_URL_STYLE` / `S3_USE_SSL` | empty / `us-east-1` / `vhost` / `true` | that secret's settings |
| `SERVER_NAME` / `SCHEME` | `localhost:8005` / `http` | used to build the URLs in responses |
| `DATAGOUV_URL` | `https://www.data.gouv.fr` | instance of the resources: their `url` is its permalink, which redirects to the source file |
| `DUCKDB_METADATA_CACHE` | `false` | cache parquet footers and HTTP metadata (see below) |
| `PAGE_SIZE_DEFAULT` / `PAGE_SIZE_MAX` | `20` / `50` | pagination |
| `BATCH_SIZE` | `50000` | maximum number of rows for the CSV/JSON downloads |
| `DUCKDB_THREADS` / `DUCKDB_MEMORY_LIMIT` | `4` / `2GB` | limits of the DuckDB instance, shared by every query |
| `QUERY_TIMEOUT` / `QUERY_WORKERS` | `30` / `8` | data queries are interrupted after this many seconds, and at most this many run at once (a 503 is returned to the others once the timeout is spent waiting) |
| `RESOURCE_CACHE_TTL` | `300` | how long a parquet schema is cached, in seconds |
| `PROFILE_STATS` | `true` | compute `SUMMARIZE` statistics in `/profile/` |
| `SQL_MAX_ROWS` | `10000` | row cap of the SQL endpoint |
| `SQL_MAX_BYTES` | `10000000` | size cap of the SQL endpoint answers, in bytes of JSON |
| `SQL_TIMEOUT` | `30` | timeout of the SQL endpoint, in seconds |
| `SQL_MAX_LENGTH` | `10000` | maximum length of the `q` parameter |
| `SENTRY_DSN` | empty | error reporting |

Reading over the network costs one footer fetch per query (~120 ms on the OVH bucket). `DUCKDB_METADATA_CACHE=true` removes it almost entirely, but DuckDB offers no way to invalidate that cache, so a parquet file replaced in the bucket keeps being served from the cached metadata until the process restarts. It is therefore off by default, and only worth enabling when the files are immutable.

## 🧪 Testing

This project uses [pytest](https://pytest.org/) for testing. No infrastructure is needed: the tests write their own parquet files and run the API in process against them.

```shell
uv sync
uv run pytest
```

### Tests Structure

- **`tests/test_api.py`** - API endpoint tests, run in process against the local parquet files
- **`tests/test_query.py`** - Query building tests, executed on DuckDB
- **`tests/test_sql_security.py`** - What the SQL endpoint refuses, and what the DuckDB sandbox prevents
- **`tests/conftest.py`** - Test fixtures and configuration

## 🔀 Differences with the PostgREST version

- No ingestion in PostgreSQL, no `tables_index` table: a `<resource_id>.parquet` file in the bucket is enough. Resource metadata is derived from the parquet file itself (schema, row count, statistics) and from the object's `Last-Modified` header (`created_at`).
- `/api/resources/{rid}/` returns as `url` the permalink of the resource on data.gouv.fr (`DATAGOUV_URL`), which redirects to its source file, instead of the URL recorded at conversion time.
- A resource deleted by its producer is no longer answered with a `410 Gone`: deletions are not tracked here, the data is served as long as its parquet file is in the bucket.
- `/profile/` returns `"indexes": null`: there is no database index, the parquet file is scanned, so no column is privileged.
- Aggregations are allowed on every resource and every column: `/api/aggregation-exceptions/` always answers `{"allowed": true, "exceptions": []}`.
- `page` and `page_size` must be positive integers, any other value answers a 400. The PostgREST version served the first page for `page=0` or a negative page, and the whole table, without any limit, for `page_size=0`.
- The metrics API is out of scope.

## 🤝 Contributing

### 🧹 Code Linting and Formatting

This project follows PEP 8 style guidelines using [Ruff](https://astral.sh/ruff/) for linting and formatting. **Either running these commands manually or installing the pre-commit hook is required before submitting contributions.**

```shell
# Lint (including import sorting) and format code
uv run ruff check --fix && uv run ruff format
```

### 🔗 Pre-commit Hooks

This repository uses a [pre-commit](https://pre-commit.com/) hook which lint and format code before each commit. **Installing the pre-commit hook is required for contributions.**

**Install pre-commit hooks:**
```shell
uvx pre-commit install
```
The pre-commit hook that automatically:
- Check YAML syntax
- Fix end-of-file issues
- Remove trailing whitespace
- Check for large files
- Run Ruff linting and formatting

### 🧪 Running Tests

**Pull requests cannot be merged unless all CI/CD tests pass.**
Tests are automatically run on every pull request and push to main branch. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml) for the complete CI/CD configuration, and the [🧪 Testing](#-testing) section above for detailed testing commands.

### 🏷️ Releases and versioning

The release process uses the [`tag_version.sh`](tag_version.sh) script to create git tags, GitHub releases and update [CHANGELOG.md](CHANGELOG.md) automatically.

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

### Publishing to PyPI

`uv publish` uploads the built package. It authenticates with the `UV_PUBLISH_TOKEN` environment variable and sends that value as a PyPI API token, with the username `__token__`. See the [uv environment reference](https://docs.astral.sh/uv/reference/environment/#uv_publish_token).

Set that variable in the environment that runs the publish step:

1. Create an API token on [PyPI](https://pypi.org/manage/account/token/). Limit its scope to this project when you can. The value starts with `pypi-`.
2. Store it as a secret named `UV_PUBLISH_TOKEN` and expose it to the publish step.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/datagouv/api-tabular/issues)
- **Discussion**: Use the discussion section at the end of the [production API page](https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-fr-beta/)
- **Contact Form**: [Support form](https://support.data.gouv.fr/)

## 🌐 Production Resources

- **Production API**: [`https://tabular-api.data.gouv.fr/api`](https://tabular-api.data.gouv.fr/api)
- **Product Documentation**: [API tabulaire data.gouv.fr (beta)](https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-fr-beta/) (in French)
- **Technical Documentation**: [Swagger/OpenAPI docs](https://tabular-api.data.gouv.fr/api/doc)
