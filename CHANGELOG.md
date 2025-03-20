# Changelog

## 0.2.4 (2025-03-20)

- Allow to request specific columns [#42](https://github.com/datagouv/api-tabular/pull/42) 

## 0.2.3 (2025-03-19)

- Add PostgREST version in health endpoint [#37](https://github.com/datagouv/api-tabular/pull/37)
- Cast env configs as expected [#39](https://github.com/datagouv/api-tabular/pull/39)
- Fix pagination and total for aggregation queries [#41](https://github.com/datagouv/api-tabular/pull/41)

## 0.2.2 (2024-11-28)

- Handle queries with aggregators [#35](https://github.com/datagouv/api-tabular/pull/35)
- Restrain aggregators to list of specific resources [#36](https://github.com/datagouv/api-tabular/pull/36)

## 0.2.1 (2024-11-21)

- Add healthcheck endpoint [#33](https://github.com/datagouv/api-tabular/pull/33)

## 0.2.0 (2024-11-18)

- Add endpoint to stream a CSV response [#5](https://github.com/etalab/api-tabular/pull/5)
- Make URL in links absolute [#7](https://github.com/etalab/api-tabular/pull/7)
- Add health route [#16](https://github.com/etalab/api-tabular/pull/16)
- Add SERVER NAME and SCHEME config [#17](https://github.com/etalab/api-tabular/pull/17)
- Override config with env [#18](https://github.com/etalab/api-tabular/pull/18)
- Improve swagger and add new filters [#23](https://github.com/datagouv/api-tabular/pull/23)
- Fix error in `process_total` using `Content-Range` when the DB response is not valid [#27](https://github.com/datagouv/api-tabular/pull/27)
- Update CI to build in Python 3.11 instead of Python 3.9, update deprecated CI [#29](https://github.com/datagouv/api-tabular/pull/29)
- Better handling of columns with special characters [#30](https://github.com/etalab/api-tabular/pull/30)
- Update deprecated Sentry methods [#28](https://github.com/datagouv/api-tabular/pull/28)

## 0.1.0
