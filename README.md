![Tabular API](banner.png)

# Tabular API on Parquet + DuckDB

Same HTTP API as [api-tabular](../api-tabular), but the data comes from
`<resource_id>.parquet` files read directly from object storage and is queried by DuckDB instead
of PostgreSQL + PostgREST.

```
client --> FastAPI (query string -> SQL) --> DuckDB (embedded, httpfs)
                                               --> <PARQUET_BASE_URL>/<resource_id>.parquet
```

Any parquet file of the bucket is queryable as is, with no ingestion and no index table: the
resource id in the URL is turned into an object URL. `PARQUET_BASE_URL` defaults to the public
OVH bucket `https://hydra.s3.rbx.io.cloud.ovh.net/parquet`, which is read over plain HTTPS with
no credentials.

## Run it

```shell
docker compose up -d --build
```

A single container starts: the FastAPI app with DuckDB embedded, on http://localhost:8005.
Documentation: http://localhost:8005/api/doc

```shell
RID=3b6b2281-b9d9-4959-ae9d-c2c166dff118
curl "http://localhost:8005/api/resources/$RID/data/?page_size=5"
```

### Without Docker

```shell
uv sync
uv run uvicorn tabular_duckdb.main:app --port 8005 --reload
```

Configuration comes from environment variables or a `.env` file, see
[.env.example](.env.example); the defaults point at the public OVH bucket. Another bucket only
needs `PARQUET_BASE_URL`, and `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_ENDPOINT` if it is private
(the base URL can then be an `s3://` one).

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/resources/{rid}/` | metadata and links (`created_at` is the parquet file's last modification date) |
| `GET /api/resources/{rid}/profile/` | column types and statistics, derived from the parquet file |
| `GET /api/resources/{rid}/swagger/` | OpenAPI spec for this resource, with the filters allowed per column |
| `GET /api/resources/{rid}/data/` | paginated JSON data, with filtering and sorting |
| `GET /api/resources/{rid}/data/csv/` | same, streamed as CSV |
| `GET /api/resources/{rid}/data/json/` | same, streamed as JSON |
| `GET /api/resources/{rid}/sql/?q=...` | read-only SQL query on the resource |
| `GET /health/` | health of DuckDB and reachability of the storage |

### Filtering, sorting, pagination

Unchanged compared to the PostgREST-based API:

```shell
# filters
curl "http://localhost:8005/api/resources/$RID/data/?Code%20du%20d%C3%A9partement__exact=62"
curl "http://localhost:8005/api/resources/$RID/data/?Nom%20de%20l%27%C3%A9lu__contains=MARTIN"

# sort, pagination, column selection
curl "http://localhost:8005/api/resources/$RID/data/?Date%20de%20naissance__sort=desc&page=2&page_size=50"
curl "http://localhost:8005/api/resources/$RID/data/?columns=Code%20du%20d%C3%A9partement,Nom%20de%20l%27%C3%A9lu"
```

Operators: `exact`, `differs`, `isnull`, `isnotnull`, `contains`, `notcontains`, `in`, `notin`,
`less`, `greater`, `strictly_less`, `strictly_greater`, `sort`, plus the nested `or=(...)` /
`and(...)` groups:

```shell
curl "http://localhost:8005/api/resources/$RID/data/?or=(\"Code%20du%20d%C3%A9partement\"__exact.62,\"Code%20du%20d%C3%A9partement\"__exact.59)"
```

Each row carries a synthetic `__id` column (the row number in the parquet file), which is also
the default sort, so pagination is stable.

### SQL endpoint (aggregations)

Aggregation operators (`__groupby`, `__count`, `__avg`, `__min`, `__max`, `__sum`) no longer
exist on `/data/`: they are replaced by real SQL, where the resource is the table `resource`.

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

The endpoint is read-only, and enforced as such at several levels:

- the query is first serialized by DuckDB's `json_serialize_sql()`, which only accepts `SELECT`:
  `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `COPY`, `EXPORT`, `ATTACH`, `INSTALL`, `LOAD`, `SET`,
  `PRAGMA`, `CALL`, `CREATE SECRET`... are rejected before anything runs;
- exactly one statement is allowed, so nothing can hide behind a `;`;
- no table function is allowed (`read_parquet`, `read_csv`, `read_text`, `glob`,
  `duckdb_secrets()`, `duckdb_settings()`...), so no arbitrary file can be read with the
  server's credentials. Only `generate_series`, `range`, `unnest` and `repeat` are allowed;
- the only table that can be referenced is `resource` (plus your own CTEs). A bare
  `FROM 'https://.../file.parquet'` is parsed as a table name and rejected as well;
- queries run on a dedicated pool of sandboxed DuckDB connections: `enable_external_access=false`
  with only `PARQUET_BASE_URL` in `allowed_directories`, so local files, other prefixes of the
  same bucket and other hosts are all unreachable, extension autoloading is off, and
  `lock_configuration=true` prevents a query from loosening its own sandbox;
- results are capped at `SQL_MAX_ROWS` rows (`meta.truncated` says whether the cap was hit) and
  the query is interrupted after `SQL_TIMEOUT` seconds;
- DuckDB error messages are filtered so that neither the storage path nor the credentials leak.

The same sandbox is applied to the connection serving `/data/`, so the whole process can only
ever read `<PARQUET_BASE_URL>/...`.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PARQUET_BASE_URL` | `https://hydra.s3.rbx.io.cloud.ovh.net/parquet` | prefix holding the `<resource_id>.parquet` files |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | empty | only for a private bucket, creates a DuckDB S3 secret |
| `S3_ENDPOINT` / `S3_REGION` / `S3_URL_STYLE` / `S3_USE_SSL` | empty / `us-east-1` / `vhost` / `true` | that secret's settings |
| `SERVER_NAME` / `SCHEME` | `localhost:8005` / `http` | used to build the URLs in responses |
| `DUCKDB_METADATA_CACHE` | `false` | cache parquet footers and HTTP metadata (see below) |
| `PAGE_SIZE_DEFAULT` / `PAGE_SIZE_MAX` | `20` / `50` | pagination |
| `BATCH_SIZE` | `50000` | maximum number of rows for the CSV/JSON downloads |
| `DUCKDB_THREADS` / `DUCKDB_MEMORY_LIMIT` | `4` / `2GB` | DuckDB limits |
| `RESOURCE_CACHE_TTL` | `300` | how long a parquet schema is cached, in seconds |
| `PROFILE_STATS` | `true` | compute `SUMMARIZE` statistics in `/profile/` |
| `SQL_POOL_SIZE` | `4` | connections dedicated to the SQL endpoint |
| `SQL_MAX_ROWS` | `10000` | row cap of the SQL endpoint |
| `SQL_TIMEOUT` | `30` | timeout of the SQL endpoint, in seconds |
| `SQL_MAX_LENGTH` | `10000` | maximum length of the `q` parameter |
| `SENTRY_DSN` | empty | error reporting |

Reading over the network costs one footer fetch per query (~120 ms on the OVH bucket).
`DUCKDB_METADATA_CACHE=true` removes it almost entirely, but DuckDB offers no way to invalidate
that cache, so a parquet file replaced in the bucket keeps being served from the cached metadata
until the process restarts. It is therefore off by default, and only worth enabling when the
files are immutable.

## Tests

```shell
uv sync
uv run pytest tests/test_query.py tests/test_sql_security.py   # no infrastructure needed
docker compose up -d --build && uv run pytest                  # including integration tests
```

`tests/test_api.py` needs the stack running (it is skipped otherwise) and targets
`TABULAR_API_URL`, `http://localhost:8005` by default.

## Differences with the PostgREST version

- No ingestion in PostgreSQL, no `tables_index` table: a `<resource_id>.parquet` file in the
  bucket is enough. Resource metadata is derived from the parquet file itself (schema, row count,
  statistics) and from the object's `Last-Modified` header (`created_at`).
- `/api/resources/{rid}/` returns `"url": null` (the origin of the file is not tracked here).
- `/profile/` returns `"indexes": null`: there is no database index, the parquet file is scanned,
  so no column is privileged. `/api/aggregation-exceptions/` is gone with the aggregation
  operators.
- Aggregations are done through `/sql/` instead of `__groupby` & friends.
- The metrics API of the original repository is out of scope.
