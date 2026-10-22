"""Per-resource OpenAPI spec: the filters and sorts available on each column of the resource."""

import yaml

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
    # columns holding nested values only support null checks
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


def swagger_parameters(resource_columns: dict) -> list:
    parameters_list: list[dict] = [
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
        {
            "name": "or",
            "in": "query",
            "description": (
                "Encapsulates filters to be joined with the `OR` operator. Allows to build "
                "complex queries with nested `or(...)` and `and(...)` operators. Inner filters "
                "syntax must be `<column>.<operation>.<value>` (or `<column>.<operation>` for "
                "`is(not)null`), separated with commas."
            ),
            "required": False,
            "schema": {"type": "string"},
        },
    ]
    for key, value in resource_columns.items():
        possibilities = TYPE_POSSIBILITIES[value["python_type"]]
        for op, description in OPERATORS_DESCRIPTIONS.items():
            if op not in possibilities:
                continue
            op_name = description["name"]
            op_description = description["description"]
            parameters_list.append(
                {
                    "name": op_name.format(key),
                    "in": "query",
                    "description": op_description.format(
                        *[key for _ in range(op_description.count("{}"))]
                    ),
                    "required": False,
                    "schema": {"type": "string"},
                }
                | (
                    # aggregators and is(not)null don't need a value
                    {"schema": {"type": "boolean"}, "allowEmptyValue": True}
                    if op in ("isnull", "isnotnull") or description.get("is_aggregator")
                    else {}
                )
            )
        if "sort" in possibilities:
            parameters_list.append(
                {
                    "name": f"{key}__sort",
                    "in": "query",
                    "description": (
                        f"Sort ascending or descending on column: {key} "
                        f"({key}__sort=asc or {key}__sort=desc)"
                    ),
                    "required": False,
                    "schema": {"type": "string"},
                }
            )
        if "compare" in possibilities:
            for suffix, label in (
                ("less", "Less than"),
                ("greater", "Greater than"),
                ("strictly_less", "Strictly less than"),
                ("strictly_greater", "Strictly greater than"),
            ):
                parameters_list.append(
                    {
                        "name": f"{key}__{suffix}",
                        "in": "query",
                        "description": f"{label} in column: {key} ({key}__{suffix}=value)",
                        "required": False,
                        "schema": {"type": "string"},
                    }
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
                                "description": (
                                    "Pagination link to the next page of the resource data"
                                ),
                                "type": "string",
                            },
                            "prev": {
                                "description": (
                                    "Pagination link to the previous page of the resource data"
                                ),
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
            "SqlResult": {
                "type": "object",
                "properties": {
                    "data": {"type": "array", "items": {"type": "object"}},
                    "meta": {
                        "type": "object",
                        "properties": {
                            "rows": {"description": "Number of returned rows", "type": "integer"},
                            "truncated": {
                                "description": "True when the row limit was reached",
                                "type": "boolean",
                            },
                            "row_limit": {"type": "integer"},
                            "execution_ms": {"type": "integer"},
                        },
                    },
                },
            },
        }
    }
    return component_dict


def build_swagger_file(resource_columns: dict, rid: str) -> str:
    parameters_list = swagger_parameters(resource_columns)
    component_dict = swagger_component(resource_columns)
    swagger_dict = {
        "openapi": "3.0.3",
        "info": {
            "title": "Tabular API",
            "description": (
                "Retrieve data for a specified resource with optional filtering and sorting."
            ),
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
                    "description": (
                        "Returns resource data based on ID as JSON, each row is a dictionnary."
                    ),
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
            f"/api/resources/{rid}/sql/": {
                "get": {
                    "description": (
                        "Runs a read-only SQL query on this resource, exposed as the `resource` "
                        "table. Only a single SELECT statement is accepted."
                    ),
                    "summary": "Run a read-only SQL query on the resource",
                    "operationId": "getResourceSql",
                    "responses": {
                        "200": {
                            "description": "successful operation",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/SqlResult"}
                                }
                            },
                        },
                        "400": {"description": "Invalid or rejected SQL query"},
                        "404": {"description": "Resource not found"},
                    },
                },
                "parameters": [
                    {
                        "name": "q",
                        "in": "query",
                        "description": (
                            "The SQL query, for instance: "
                            "SELECT col, count(*) AS n FROM resource GROUP BY col ORDER BY n DESC"
                        ),
                        "required": True,
                        "schema": {"type": "string"},
                    }
                ],
            },
        },
        "components": component_dict,
    }
    return yaml.dump(swagger_dict, allow_unicode=True)
