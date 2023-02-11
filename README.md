# udata-hydra-csvapi

This connects to [udata-hydra](https://github.com/etalab/udata-hydra) and serves the converted CSVs as an API.

## Run locally

Start [udata-hydra](https://github.com/etalab/udata-hydra) via `docker compose`.

Launch this project:

```shell
docker compose up
```

You should have an API on localhost:8080:

```shell
curl http://localhost:8080/1ab0d0a24658cdbb3cbbae41b9d3e579?limit=1
```

```json
[
   {
      "__id" : 1,
      "cad_parcelles" : "840031000BX0169",
      "certification_commune" : false,
      "cle_interop" : "84031_1220_09053",
      "commune_deleguee_insee" : "",
      "commune_deleguee_nom" : "",
      "commune_insee" : "84031",
      "commune_nom" : "Carpentras",
      "date_der_maj" : "2018-11-09",
      "lat" : 44.0566876,
      "lieudit_complement_nom" : "",
      "long" : 5.0597239,
      "numero" : 9053,
      "position" : "segment",
      "source" : "Base Adresse Nationale",
      "suffixe" : "",
      "uid_adresse" : "",
      "voie_nom" : "Avenue du Mont Ventoux",
      "x" : 865019.6,
      "y" : 6330784.1
   }
]
```
