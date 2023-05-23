# udata-hydra-csvapi

This connects to [udata-hydra](https://github.com/etalab/udata-hydra) and serves the converted CSVs as an API.

## Run locally

Start [udata-hydra](https://github.com/etalab/udata-hydra) via `docker compose`.

Launch this project:

```shell
docker compose up
```

You can now access the raw postgrest API on http://localhost:8080.

Now you can launch the proxy (ie the app):

```
poetry install
poetry run adev runserver -p8005 udata_hydra_csvapi/app.py
```

And query postgrest via the proxy using a `resource_id`, cf below. Test resource_id is `27d469ff-9908-4b7e-a2e0-9439bb38a395`

## API

### Meta informations on resource

```shell
curl http://localhost:8005/api/resources/27d469ff-9908-4b7e-a2e0-9439bb38a395/
```

```json
{
  "created_at": "2023-02-11T11:44:03.875615+00:00",
  "url": "https://data.toulouse-metropole.fr//explore/dataset/boulodromes/download?format=csv&timezone=Europe/Berlin&use_labels_for_header=false",
  "links": [
    {
      "href": "/api/resources/27d469ff-9908-4b7e-a2e0-9439bb38a395/profile/",
      "type": "GET",
      "rel": "profile"
    },
    {
      "href": "/api/resources/27d469ff-9908-4b7e-a2e0-9439bb38a395/data/",
      "type": "GET",
      "rel": "data"
    }
  ]
}
```

### Profile (csv-detective output) for a resource

```shell
curl http://localhost:8005/api/resources/27d469ff-9908-4b7e-a2e0-9439bb38a395/profile/
```

```json
{
  "profile": {
    "header": [
        "geo_point_2d",
        "geo_shape",
        "ins_nom",
        "ins_complexe_nom_cplmt",
        "ins_codepostal",
        "secteur",
        "..."
    ]
  },
  "...": "..."
}
```

### Data for a resource (ie resource API)

```shell
curl http://localhost:8005/api/resources/27d469ff-9908-4b7e-a2e0-9439bb38a395/data/
```

```json
[
  {
    "__id": 1,
    "geo_point_2d": "43.58061543292057,1.401751073689455",
    "geo_shape": {
      "coordinates": [
        [
          1.401751073689455,
          43.58061543292057
        ]
      ],
      "type": "MultiPoint"
    },
    "ins_nom": "BOULODROME LOU BOSC",
    "ins_complexe_nom_cplmt": "COMPLEXE SPORTIF DU MIRAIL",
    "ins_codepostal": 31100,
    "secteur": "Toulouse Ouest",
    "quartier": 6.3,
    "acces_libre": null,
    "ins_nb_equ": 1,
    "ins_detail_equ": "",
    "ins_complexe_nom": "",
    "ins_adresse": "",
    "ins_commune": "",
    "acces_public_horaires": null,
    "acces_club_scol": null,
    "ins_nom_cplmt": "",
    "ins_id_install": ""
  }
]
```

This endpoint can be queried with the following operators as query string:

```
# sort by column
column_name__sort=asc
column_name__sort=desc

# contains
column_name__contains=word

# exacts
column_name__exact=word

# less
column_name__less=12

# greater
column_name__greater=12
```

Pagination is made throught queries with `page` `and page_size`:
```
curl http://localhost:8005/api/resources/27d469ff-9908-4b7e-a2e0-9439bb38a395/data/?page=2&page_size=30
```
