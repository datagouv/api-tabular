from collections import defaultdict
from importlib.metadata import version

import yaml
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from api_tabular import config

TYPE_POSSIBILITIES = {
    "string": [
        "isnull",
        "isnotnull",
        "compare",
        "contains",
        "notcontains",
        "differs",
        "exact",
        "in",
        "notin",
        "sort",
        "groupby",
        "count",
    ],
    "float": [
        "isnull",
        "isnotnull",
        "compare",
        "differs",
        "exact",
        "in",
        "notin",
        "sort",
        "groupby",
        "count",
        "avg",
        "max",
        "min",
        "sum",
    ],
    "int": [
        "isnull",
        "isnotnull",
        "compare",
        "differs",
        "exact",
        "in",
        "notin",
        "sort",
        "groupby",
        "count",
        "avg",
        "max",
        "min",
        "sum",
    ],
    "bool": ["isnull", "isnotnull", "differs", "exact", "sort", "groupby", "count"],
    "date": [
        "isnull",
        "isnotnull",
        "compare",
        "contains",
        "notcontains",
        "differs",
        "exact",
        "in",
        "notin",
        "sort",
        "groupby",
        "count",
    ],
    "datetime": [
        "isnull",
        "isnotnull",
        "compare",
        "contains",
        "notcontains",
        "differs",
        "exact",
        "in",
        "notin",
        "sort",
        "groupby",
        "count",
    ],
    # TODO: JSON needs special treatment for operators to work
    "json": [
        "isnull",
        "isnotnull",
    ],
}

MAP_TYPES = {
    # defaults to "string"
    "bool": "boolean",
    "int": "integer",
    "float": "number",
}

OPERATORS_DESCRIPTIONS = {
    "exact": {
        "name": "{}__exact",
        "description": "Exact match in column: {} ({}__exact=value)",
    },
    "differs": {
        "name": "{}__differs",
        "description": "Differs from in column: {} ({}__differs=value)",
    },
    "isnull": {
        "name": "{}__isnull",
        "description": "Is `NULL` in column: {} ({}__isnull)",
    },
    "isnotnull": {
        "name": "{}__isnotnull",
        "description": "Is not `NULL` in column: {} ({}__isnotnull)",
    },
    "contains": {
        "name": "{}__contains",
        "description": "String contains in column: {} ({}__contains=value)",
    },
    "notcontains": {
        "name": "{}__notcontains",
        "description": "String does not contain in column: {} ({}__notcontains=value)",
    },
    "in": {
        "name": "{}__in",
        "description": "Value in list in column: {} ({}__in=value1,value2,...)",
    },
    "notin": {
        "name": "{}__notin",
        "description": "Value not in list in column: {} ({}__notin=value1,value2,...)",
    },
    "groupby": {
        "name": "{}__groupby",
        "description": "Performs `group by values` operation in column: {}",
        "is_aggregator": True,
    },
    "count": {
        "name": "{}__count",
        "description": "Performs `count values` operation in column: {}",
        "is_aggregator": True,
    },
    "avg": {
        "name": "{}__avg",
        "description": "Performs `mean` operation in column: {}",
        "is_aggregator": True,
    },
    "min": {
        "name": "{}__min",
        "description": "Performs `minimum` operation in column: {}",
        "is_aggregator": True,
    },
    "max": {
        "name": "{}__max",
        "description": "Performs `maximum` operation in column: {}",
        "is_aggregator": True,
    },
    "sum": {
        "name": "{}__sum",
        "description": "Performs `sum` operation in column: {}",
        "is_aggregator": True,
    },
}


def is_aggregation_allowed(resource_id: str):
    return resource_id in config.ALLOW_AGGREGATION


async def get_app_version() -> str:
    """Get the version from the installed package metadata."""
    try:
        return version("udata-hydra-csvapi")
    except Exception:
        return "unknown"


def build_sql_query_string(
    request_arg: list,
    resource_id: str | None = None,
    indexes: set | None = None,
    page_size: int | None = None,
    offset: int = 0,
) -> str:
    sql_query = []
    aggregators = defaultdict(list)
    sorted = False
    for arg in request_arg:
        _split = arg.split("=")
        # filters are expected to have the syntax `<column_name>__<operator>=<value>`
        if len(_split) == 2:
            _filter, _sorted = add_filter(*_split)
            if _filter:
                sorted = sorted or _sorted
                sql_query.append(_filter)
        # aggregators are expected to have the syntax `<column_name>__<operator>`
        # is(not)null also has this syntax but is a filter
        elif len(_split) == 1:
            if _split[0].split("__")[1] in ["isnull", "isnotnull"]:
                _filter, _ = add_filter(_split[0], None)
                sql_query.append(_filter)
            else:
                column, operator = add_aggregator(_split[0], indexes)
                if column:
                    aggregators[operator].append(column)
        else:
            raise ValueError(f"argument '{arg}' could not be parsed")
    if aggregators:
        if resource_id and not is_aggregation_allowed(resource_id):
            raise PermissionError(
                f"Aggregation parameters `{'`, `'.join(aggregators.keys())}` "
                f"are not allowed for resource '{resource_id}'"
            )
        agg_query = "select="
        for operator in aggregators:
            if operator == "groupby":
                agg_query += f"{','.join(aggregators[operator])},"
            else:
                for column in aggregators[operator]:
                    # aggregated columns are named `<column_name>__<operator>`
                    # we pop the heading and trailing " that were added upstream
                    # and put them around the new column name
                    agg_query += f'"{column[1:-1]}__{operator}":{column}.{operator}(),'
        # we pop the trailing comma (it's always there, by construction)
        sql_query.append(agg_query[:-1])
    if page_size:
        sql_query.append(f"limit={page_size}")
    if offset >= 1:
        sql_query.append(f"offset={offset}")
    if not sorted and not aggregators:
        sql_query.append("order=__id.asc")
    q = "&".join(sql_query)
    if q.count("select=") > 1:
        raise ValueError("the argument `columns` cannot be set alongside aggregators")
    return q


def get_column_and_operator(argument: str) -> tuple[str, str]:
    *column_split, comparator = argument.split("__")
    normalized_comparator = comparator.lower()
    # handling headers with "__" and special characters
    # we're escaping the " because they are the encapsulators of the label
    column = '"{}"'.format("__".join(column_split).replace('"', '\\"'))
    return column, normalized_comparator


def add_filter(argument: str, value: str) -> tuple[str | None, bool]:
    if argument in ["page", "page_size"]:  # processed differently
        return None, False
    if argument == "columns":
        return f"select={value}", False
    if "__" in argument:
        column, normalized_comparator = get_column_and_operator(argument)
        if normalized_comparator == "sort":
            return f"order={column}.{value}", True
        elif normalized_comparator == "exact":
            return f"{column}=eq.{value}", False
        elif normalized_comparator == "differs":
            return f"{column}=isdistinct.{value}", False
        elif normalized_comparator == "isnull":
            return f"{column}=is.null", False
        elif normalized_comparator == "isnotnull":
            return f"{column}=not.is.null", False
        elif normalized_comparator == "contains":
            return f"{column}=ilike.*{value}*", False
        elif normalized_comparator == "notcontains":
            return f"{column}=not.ilike.*{value}*", False
        elif normalized_comparator == "in":
            return f"{column}=in.({value})", False
        elif normalized_comparator == "notin":
            return f"{column}=not.in.({value})", False
        elif normalized_comparator == "less":
            return f"{column}=lte.{value}", False
        elif normalized_comparator == "greater":
            return f"{column}=gte.{value}", False
        elif normalized_comparator == "strictly_less":
            return f"{column}=lt.{value}", False
        elif normalized_comparator == "strictly_greater":
            return f"{column}=gt.{value}", False
    raise ValueError(f"argument '{argument}={value}' could not be parsed")


def add_aggregator(argument: str, indexes: set | None) -> tuple[str, str]:
    operator = None
    if "__" in argument:
        column, operator = get_column_and_operator(argument)
        raise_if_not_index(column, indexes)
    if operator in ["avg", "count", "max", "min", "sum", "groupby"]:
        return column, operator
    raise ValueError(f"argument '{argument}' could not be parsed")


def raise_if_not_index(column_name: str, indexes: set | None) -> None:
    if indexes is None:
        return
    # we pop the heading and trailing " that were added upstream
    if column_name[1:-1] not in indexes:
        raise PermissionError(f"{column_name[1:-1]} is not among the allowed columns: {indexes}")


def process_total(res: Response) -> int:
    # the Content-Range looks like this: '0-49/21777'
    # see https://docs.postgrest.org/en/stable/references/api/pagination_count.html
    raw_total = res.headers.get("Content-Range")
    _, str_total = raw_total.split("/")
    return int(str_total)


def external_url(url) -> str:
    return f"{config.SCHEME}://{config.SERVER_NAME}{url}"


def build_link_with_page(request: Request, query_string: str, page: int, page_size: int) -> str:
    q = [string for string in query_string if not string.startswith("page")]
    q.extend([f"page={page}", f"page_size={page_size}"])
    rebuilt_q = "&".join(q)
    return external_url(f"{request.path}?{rebuilt_q}")


def url_for(request: Request, route: str, *args, **kwargs) -> str:
    router = request.app.router
    if kwargs.pop("_external", None):
        return external_url(router[route].url_for(**kwargs))
    return router[route].url_for(**kwargs)


def swagger_parameters(resource_columns: dict, resource_id: str) -> list:
    parameters_list = [
        {
            "name": "page",
            "in": "query",
            "description": "Specific page (page=value)",
            "required": False,
            "schema": {"type": "integer"},
            "example": 1,
        },
        {
            "name": "page_size",
            "in": "query",
            "description": "Number of results per page (page_size=value)",
            "required": False,
            "schema": {"type": "integer"},
            "example": 20,
        },
        {
            "name": "columns",
            "in": "query",
            "description": "Columns to keep in the result (columns=column1,column3,...)",
            "required": False,
            "schema": {"type": "string"},
            # see https://swagger.io/docs/specification/v3_0/serialization/
            "style": "form",
            "explode": False,
        },
    ]
    # expected python types are: string, float, int, bool, date, datetime, json
    # see cast for db here: https://github.com/datagouv/csv-detective/blob/master/csv_detective/output/dataframe.py
    for key, value in resource_columns.items():
        for op in OPERATORS_DESCRIPTIONS:
            if not is_aggregation_allowed(resource_id) and OPERATORS_DESCRIPTIONS[op].get(
                "is_aggregator"
            ):
                continue
            if op in TYPE_POSSIBILITIES[value["python_type"]]:
                parameters_list.extend(
                    [
                        {
                            "name": OPERATORS_DESCRIPTIONS[op]["name"].format(key),
                            "in": "query",
                            "description": (
                                (s := OPERATORS_DESCRIPTIONS[op]["description"]).format(
                                    *[key for _ in range(s.count("{}"))]
                                )
                            ),
                            "required": False,
                            "schema": {"type": "string"},
                        }
                        | (
                            # aggregators and is(not)null don't need a value
                            {
                                "schema": {"type": "boolean"},
                                "allowEmptyValue": True,
                            }
                            if (
                                op in ["isnull", "isnotnull"]
                                or OPERATORS_DESCRIPTIONS[op].get("is_aggregator")
                            )
                            else {}
                        ),
                    ]
                )
        if "sort" in TYPE_POSSIBILITIES[value["python_type"]]:
            parameters_list.extend(
                [
                    {
                        "name": f"{key}__sort",
                        "in": "query",
                        "description": (
                            f"Sort ascending or descending on column: {key} "
                            f"({key}__sort=asc or {key}__sort=desc)"
                        ),
                        "required": False,
                        "schema": {"type": "string"},
                    },
                ]
            )
        if "compare" in TYPE_POSSIBILITIES[value["python_type"]]:
            parameters_list.extend(
                [
                    {
                        "name": f"{key}__less",
                        "in": "query",
                        "description": f"Less than in column: {key} ({key}__less=value)",
                        "required": False,
                        "schema": {"type": "string"},
                    },
                    {
                        "name": f"{key}__greater",
                        "in": "query",
                        "description": f"Greater than in column: {key} ({key}__greater=value)",
                        "required": False,
                        "schema": {"type": "string"},
                    },
                    {
                        "name": f"{key}__strictly_less",
                        "in": "query",
                        "description": f"Strictly less than in column: {key} ({key}__strictly_less=value)",
                        "required": False,
                        "schema": {"type": "string"},
                    },
                    {
                        "name": f"{key}__strictly_greater",
                        "in": "query",
                        "description": f"Strictly greater than in column: {key} ({key}__strictly_greater=value)",
                        "required": False,
                        "schema": {"type": "string"},
                    },
                ]
            )
    return parameters_list


def swagger_component(resource_columns: dict) -> dict:
    resource_prop_dict = {}
    for key, value in resource_columns.items():
        type = MAP_TYPES.get(value["python_type"], "string")
        resource_prop_dict.update({f"{key}": {"type": f"{type}"}})
    component_dict = {
        "schemas": {
            "ResourceData": {
                "type": "object",
                "properties": {
                    "data": {"type": "array", "items": {"$ref": "#/components/schemas/Resource"}},
                    "link": {
                        "type": "object",
                        "properties": {
                            "profile": {
                                "description": "Link to the profile endpoint of the resource",
                                "type": "string",
                            },
                            "next": {
                                "description": "Pagination link to the next page of the resource data",
                                "type": "string",
                            },
                            "prev": {
                                "description": "Pagination link to the previous page of the resource data",
                                "type": "string",
                            },
                        },
                    },
                    "meta": {
                        "type": "object",
                        "properties": {
                            "page": {"description": "Current page", "type": "integer"},
                            "page_size": {
                                "description": "Number of results per page",
                                "type": "integer",
                            },
                            "total": {"description": "Total number of results", "type": "integer"},
                        },
                    },
                },
            },
            "Resource": {"type": "object", "properties": resource_prop_dict},
        }
    }
    return component_dict


def build_swagger_file(resource_columns: dict, rid: str) -> str:
    parameters_list = swagger_parameters(resource_columns, rid)
    component_dict = swagger_component(resource_columns)
    swagger_dict = {
        "openapi": "3.0.3",
        "info": {
            "title": "Tabular API",
            "description": "Retrieve data for a specified resource with optional filtering and sorting.",
            "version": "1.0.0",
        },
        "tags": [
            {
                "name": "Data retrieval",
                "description": "Retrieve data for a specified resource",
            }
        ],
        "paths": {
            f"/api/resources/{rid}/data/": {
                "get": {
                    "description": "Returns resource data based on ID as JSON, each row is a dictionnary.",
                    "summary": "Get resource data from its ID",
                    "operationId": "getResourceDataFromId",
                    "responses": {
                        "200": {
                            "description": "successful operation",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ResourceData"}
                                }
                            },
                        },
                        "400": {"description": "Invalid query string"},
                        "404": {"description": "Resource not found"},
                    },
                },
                "parameters": parameters_list,
            },
            f"/api/resources/{rid}/data/csv/": {
                "get": {
                    "description": "Returns resource data based on ID as a CSV file.",
                    "summary": "Get resource data from its ID in CSV format",
                    "operationId": "getResourceDataFromIdCSV",
                    "responses": {
                        "200": {"description": "successful operation", "content": {"text/csv": {}}},
                        "400": {"description": "Invalid query string"},
                        "404": {"description": "Resource not found"},
                    },
                },
                "parameters": parameters_list,
            },
            f"/api/resources/{rid}/data/json/": {
                "get": {
                    "description": "Returns resource data based on ID as a JSON file.",
                    "summary": "Get resource data from its ID in JSON format",
                    "operationId": "getResourceDataFromIdJSON",
                    "responses": {
                        "200": {
                            "description": "successful operation",
                            "content": {"application/json": {}},
                        },
                        "400": {"description": "Invalid query string"},
                        "404": {"description": "Resource not found"},
                    },
                },
                "parameters": parameters_list,
            },
        },
        "components": component_dict,
    }
    return yaml.dump(swagger_dict, allow_unicode=True)
