from aiohttp.web_request import Request

from api_tabular import config


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
            elif normalized_comparator == "contains":
                sql_query.append(f"{column}=ilike.*{value}*")
            elif normalized_comparator == "less":
                sql_query.append(f"{column}=lte.{value}")
            elif normalized_comparator == "greater":
                sql_query.append(f"{column}=gte.{value}")
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
    swagger_string = ''
    for key, value in resource_columns.items():
        swagger_string = swagger_string + f"""
        - name: sort ascending {key}
          in: query
          required: false
          description: {key}__sort=asc.
          schema:
            type: string
        - name: sort descending {key}
          in: query
          required: false
          description: {key}__sort=desc.
          schema:
            type: string"""
        if value['python_type'] == 'string':
            f"""
        - name: exact {key}
          in: query
          required: false
          description: {key}__exact=value.
          schema:
            type: string
        - name: contains {key}
          in: query
          required: false
          description: {key}__contains=value.
          schema:
            type: string"""
        elif value['python_type'] == 'float':
            f"""
        - name: {key} less
          in: query
          required: false
          description: {key}__less=value.
          schema:
            type: string
        - name: {key} greater
          in: query
          required: false
          description: {key}__greater=value.
          schema:
            type: string"""
    return swagger_string


def swagger_component(resource_columns):
    component_string = """
    components:
      schemas:
        Resource:
          type: object
          properties:"""
    for key, value in resource_columns.items():
        type = 'string'
        if value['python_type'] == 'float':
            type = 'integer'
        component_string = component_string + f"""
            {key}:
              type: {type}"""
    return component_string


def build_swagger_file(resource_columns):
    parameters = swagger_parameters(resource_columns)
    components = swagger_component(resource_columns)
    swagger_string = f"""
    openapi: 3.0.3
    info:
      title: Resource data API
      description: Retrieve data for a specified resource with optional filtering and sorting.
      version: 1.0.0
    tags:
      - name: Data retrieval
        description: Retrieve data for a specified resource
    paths:
      /api/resources/{{rid}}/data/:
        get:
          description: Returns resource data based on ID
          summary: Find resource by ID
          operationId: getResourceById
          responses:
            '200':
              description: successful operation
              content:
                application/json:
                  schema:
                    type: array
                    items:
                      $ref: '#/components/schemas/Resource'
            '400':
              description: Invalid query string
            '404':
              description: Resource not found
        parameters:
        - name: rid
          in: path
          description: ID of resource to return
          required: true
          schema:
            type: string
        - name: page
          in: query
          required: false
          description: Specific page.
          schema:
            type: string
        - name: page_size
          in: query
          required: false
          description: Number of results per page.
          schema:
            type: string{parameters}
      /api/resources/{{rid}}/data/csv:
        get:
          description: Returns resource data based on ID as a CSV file
          summary: Find resource by ID in CSV
          operationId: getResourceByIdCSV
          responses:
            '200':
              description: successful operation
              content:
                text/csv: {{}}
            '400':
              description: Invalid query string
            '404':
              description: Resource not found
        parameters:
        - name: rid
          in: path
          description: ID of resource to return
          required: true
          schema:
            type: string
        - name: page
          in: query
          required: false
          description: Specific page.
          schema:
            type: string
        - name: page_size
          in: query
          required: false
          description: Number of results per page.
          schema:
            type: string{parameters}
      /health:
        get:
          description: Ping endpoint to ensure health of metrics service.
          summary: Service's health endpoint
          operationId: getMetricsHealth
          responses:
            '200':
              description: successful operation
    {components}"""
    return swagger_string
