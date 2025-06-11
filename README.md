# Api-tabular

This connects to [hydra](https://github.com/datagouv/hydra) and serves the converted CSVs as an API.

## Run locally

Start [hydra](https://github.com/datagouv/hydra) via `docker compose`.

Launch this project:

```shell
docker compose up
```

You can now access the raw postgrest API on http://localhost:8080.

Now you can launch the proxy (ie the app):

```shell
poetry install
poetry run adev runserver -p8005 api_tabular/app.py        # Api related to apified CSV files by udata-hydra
poetry run adev runserver -p8005 api_tabular/metrics.py    # Api related to udata's metrics
```

And query postgrest via the proxy using a `resource_id`, cf below. Test resource_id is `aaaaaaaa-1111-bbbb-2222-cccccccccccc`

## API

### Meta informations on resource

```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/
```

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

### Profile (csv-detective output) for a resource

```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/
```

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

### Data for a resource (ie resource API)

```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/
```

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

This endpoint can be queried with the following operators as query string (replacing `column_name` with the name of an actual column), if the column type allows it (see the swagger for each column's allowed parameter):

```
# sort by column
column_name__sort=asc
column_name__sort=desc

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

> /!\ WARNING: aggregation requests are only available for resources that are listed in the `ALLOW_AGGREGATION` list of the config file, which can be seen at the `/api/aggregation-exceptions/` endpoint.

> NB : passing an aggregation operator (`count`, `avg`, `min`, `max`, `sum`) returns a column that is named `<column_name>__<operator>` (for instance: `?birth__groupby&score__sum` will return a list of dicts with the keys `birth` and `score__sum`).

For instance:
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?score__greater=0.9&decompte__exact=13
```
returns
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

With filters and aggregators (filtering is always done **before** aggregation, no matter the order in the parameters):
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?decompte__groupby&birth__less=1996&score__avg
```
i.e. `decompte` and average of `score` for all rows where `birth<="1996"`, grouped by `decompte`, returns
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

Pagination is made through queries with `page` and `page_size`:
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=2&page_size=30
```

You may also specify the columns you want to keep using the `columns` argument:
```shell
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?columns=col1,col2
```


## Contributing

### Pre-commit hook

This repository uses a [pre-commit](https://pre-commit.com/) hook which lint and format code before each commit.
Please install it with:
```shell
poetry run pre-commit install
```

### Lint and format code

To lint, format and sort imports, this repository uses [Ruff](https://astral.sh/ruff/).
You can run the following command to lint and format the code:
```shell
poetry run ruff check --fix && poetry run ruff format
```

### Releases

The release process uses [bump'X](https://github.com/datagouv/bumpx).
