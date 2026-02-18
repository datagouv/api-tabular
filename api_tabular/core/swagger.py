from typing import cast

import yaml

from api_tabular.core.utils import is_aggregation_allowed

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
                op_name = cast(str, OPERATORS_DESCRIPTIONS[op]["name"])
                op_description = cast(str, OPERATORS_DESCRIPTIONS[op]["description"])
                parameters_list.extend(
                    [
                        {
                            "name": op_name.format(key),
                            "in": "query",
                            "description": (
                                op_description.format(
                                    *[key for _ in range(op_description.count("{}"))]
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
