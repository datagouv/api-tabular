import yaml
from aiohttp.web_request import Request

from api_tabular import config

TYPE_POSSIBILITIES = {
    "string": ["compare", "contains", "differs", "exact", "in", "sort"],
    "float": ["compare", "differs", "exact", "in", "sort"],
    "int": ["compare", "differs", "exact", "in", "sort"],
    "bool": ["differs", "exact", "in", "sort"],
    "date": ["compare", "contains", "differs", "exact", "in", "sort"],
    "datetime": ["compare", "contains", "differs", "exact", "in", "sort"],
    "json": ["contains", "exact", "in"],
}

MAP_TYPES = {
    # defaults to "string"
    "bool": "boolean",
    "int": "integer",
    "float": "number",
}


def build_sql_query_string(
    request_arg: list, page_size: int = None, offset: int = 0
) -> str:
    sql_query = []
    sorted = False
    for arg in request_arg:
        argument, value = arg.split("=")
        if "__" in argument:
            column, comparator = argument.split("__")
            normalized_comparator = comparator.lower()

            if normalized_comparator == "sort":
                if value == "asc":
                    sql_query.append(f"order={column}.asc,__id.asc")
                elif value == "desc":
                    sql_query.append(f"order={column}.desc,__id.asc")
                sorted = True
            elif normalized_comparator == "exact":
                sql_query.append(f"{column}=eq.{value}")
            elif normalized_comparator == "differs":
                sql_query.append(f"{column}=neq.{value}")
            elif normalized_comparator == "contains":
                sql_query.append(f"{column}=ilike.*{value}*")
            elif normalized_comparator == "in":
                sql_query.append(f"{column}=in.{value}")
            elif normalized_comparator == "less":
                sql_query.append(f"{column}=lte.{value}")
            elif normalized_comparator == "greater":
                sql_query.append(f"{column}=gte.{value}")
            elif normalized_comparator == "strictly_less":
                sql_query.append(f"{column}=lt.{value}")
            elif normalized_comparator == "strictly_greater":
                sql_query.append(f"{column}=gt.{value}")
    if page_size:
        sql_query.append(f"limit={page_size}")
    if offset >= 1:
        sql_query.append(f"offset={offset}")
    if not sorted:
        sql_query.append("order=__id.asc")
    return "&".join(sql_query)


def process_total(raw_total: str) -> int:
    # The raw total looks like this: '0-49/21777'
    _, str_total = raw_total.split("/")
    return int(str_total)


def external_url(url):
    return f"{config.SCHEME}://{config.SERVER_NAME}{url}"


def build_link_with_page(request: Request, query_string: str, page: int, page_size: int):
    q = [string for string in query_string if not string.startswith("page")]
    q.extend([f"page={page}", f"page_size={page_size}"])
    rebuilt_q = "&".join(q)
    return external_url(f"{request.path}?{rebuilt_q}")


def url_for(request: Request, route: str, *args, **kwargs):
    router = request.app.router
    if kwargs.pop("_external", None):
        return external_url(router[route].url_for(**kwargs))
    return router[route].url_for(**kwargs)


def swagger_parameters(resource_columns):
    parameters_list = [
        {
            'name': 'page',
            'in': 'query',
            'description': 'Specific page',
            'required': False,
            'schema': {
                'type': 'string'
            }
        },
        {
            'name': 'page_size',
            'in': 'query',
            'description': 'Number of results per page',
            'required': False,
            'schema': {
                'type': 'string'
            }
        },
    ]
    # expected python types are: string, float, int, bool, date, datetime, json
    # see metier_to_python here: https://github.com/datagouv/csv-detective/blob/master/csv_detective/explore_csv.py
    # see cast for db here: https://github.com/datagouv/hydra/blob/main/udata_hydra/analysis/csv.py
    for key, value in resource_columns.items():
        if "exact" in TYPE_POSSIBILITIES[value['python_type']]:
            parameters_list.extend(
                [
                    {
                        'name': f'{key}__exact=value',
                        'in': 'query',
                        'description': f'Exact match in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                ]
            )
        if "differs" in TYPE_POSSIBILITIES[value['python_type']]:
            parameters_list.extend(
                [
                    {
                        'name': f'{key}__differs=value',
                        'in': 'query',
                        'description': f'Differs from in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                ]
            )
        if "in" in TYPE_POSSIBILITIES[value['python_type']]:
            parameters_list.extend(
                [
                    {
                        'name': f'{key}__in=(value1,value2,...)',
                        'in': 'query',
                        'description': f'Value in list in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                ]
            )
        if "sort" in TYPE_POSSIBILITIES[value['python_type']]:
            parameters_list.extend(
                [
                    {
                        'name': f'{key}__sort=asc',
                        'in': 'query',
                        'description': f'Sort ascending on column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                    {
                        'name': f'{key}__sort=desc',
                        'in': 'query',
                        'description': f'Sort descending on column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                ]
            )
        if "contains" in TYPE_POSSIBILITIES[value['python_type']]:
            parameters_list.extend(
                [
                    {
                        'name': f'{key}__contains=value',
                        'in': 'query',
                        'description': f'String contains in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                ]
            )
        if "compare" in TYPE_POSSIBILITIES[value['python_type']]:
            parameters_list.extend(
                [
                    {
                        'name': f'{key}__less=value',
                        'in': 'query',
                        'description': f'Less than in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                    {
                        'name': f'{key}__greater=value',
                        'in': 'query',
                        'description': f'Greater than in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                    {
                        'name': f'{key}__strictly_less=value',
                        'in': 'query',
                        'description': f'Strictly less than in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    },
                    {
                        'name': f'{key}__strictly_greater=value',
                        'in': 'query',
                        'description': f'Strictly greater than in column: {key}',
                        'required': False,
                        'schema': {
                            'type': 'string'
                        }
                    }
                ]
            )
    return parameters_list


def swagger_component(resource_columns):
    resource_prop_dict = {}
    for key, value in resource_columns.items():
        type = MAP_TYPES.get(value['python_type'], 'string')
        resource_prop_dict.update({
            f'{key}': {
                'type': f'{type}'
            }
        })
    component_dict = {
        'schemas': {
            'ResourceData': {
                'type': 'object',
                'properties': {
                    'data': {
                        'type': 'array',
                        'items': {
                            '$ref': '#/components/schemas/Resource'
                        }
                    },
                    'link': {
                        'type': 'object',
                        'properties': {
                            'profile': {
                                'description': 'Link to the profile endpoint of the resource',
                                'type': 'string'
                            },
                            'next': {
                                'description': 'Pagination link to the next page of the resource data',
                                'type': 'string'
                            },
                            'prev': {
                                'description': 'Pagination link to the previous page of the resource data',
                                'type': 'string'
                            }
                        }
                    },
                    'meta': {
                        'type': 'object',
                        'properties': {
                            'page': {
                                'description': 'Current page',
                                'type': 'integer'
                            },
                            'page_size': {
                                'description': 'Number of results per page',
                                'type': 'integer'
                            },
                            'total': {
                                'description': 'Total number of results',
                                'type': 'integer'
                            }
                        }
                    }
                }
            },
            'Resource': {
                'type': 'object',
                'properties': resource_prop_dict
            }
        }
    }
    return component_dict


def build_swagger_file(resource_columns, rid):
    parameters_list = swagger_parameters(resource_columns)
    component_dict = swagger_component(resource_columns)
    swagger_dict = {
        'openapi': '3.0.3',
        'info': {
            'title': 'Resource data API',
            'description': 'Retrieve data for a specified resource with optional filtering and sorting.',
            'version': '1.0.0'
        },
        'tags': [{
            'name': 'Data retrieval',
            'description': 'Retrieve data for a specified resource',
        }],
        'paths': {
            f'/api/resources/{rid}/data/': {
                'get': {
                    'description': 'Returns resource data based on ID as JSON, each row is a dictionnary.',
                    'summary': 'Get resource data from its ID',
                    'operationId': 'getResourceDataFromId',
                    'responses': {
                        '200': {
                            'description': 'successful operation',
                            'content': {
                                'application/json': {
                                    'schema': {
                                        '$ref': '#/components/schemas/ResourceData'
                                    }
                                }
                            }
                        },
                        '400': {
                            'description': 'Invalid query string'
                        },
                        '404': {
                            'description': 'Resource not found'
                        }
                    }
                },
                'parameters': parameters_list
            },
            f'/api/resources/{rid}/data/csv/': {
                'get': {
                    'description': 'Returns resource data based on ID as a CSV file.',
                    'summary': 'Get resource data from its ID in CSV format',
                    'operationId': 'getResourceDataFromIdCSV',
                    'responses': {
                        '200': {
                            'description': 'successful operation',
                            'content': {
                                'text/csv': {}
                            }
                        },
                        '400': {
                            'description': 'Invalid query string'
                        },
                        '404': {
                            'description': 'Resource not found'
                        }
                    }
                },
                'parameters': parameters_list
            }
        },
        'components': component_dict
    }
    return yaml.dump(swagger_dict)
