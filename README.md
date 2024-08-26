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

```
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
  "url": "https://data.gouv.fr/datastes/example/resources/fake.csv",
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

This endpoint can be queried with the following operators as query string (replacing `column_name` with the name of an actual column):

```
# sort by column
column_name__sort=asc
column_name__sort=desc

# exacts
column_name__exact=word

# contains (for strings only)
column_name__contains=word

# less
column_name__less=12

# greater
column_name__greater=12

# strictly less
column_name__strictly_less=12

# strictly greater
column_name__strictly_greater=12
```

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

Pagination is made through queries with `page` and `page_size`:
```
curl http://localhost:8005/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=2&page_size=30
```
