from aiohttp.web_request import Request


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
                    sql_query.append(f"order=__id.asc,{column}.asc")
                elif value == "desc":
                    sql_query.append(f"order=__id.asc,{column}.desc")
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
        sql_query.append(f"order=__id.asc")
    return "&".join(sql_query)


def process_total(raw_total: str) -> int:
    # The raw total looks like this: '0-49/21777'
    _, str_total = raw_total.split("/")
    return int(str_total)


def build_link_with_page(request: Request, query_string: str, page: int, page_size: int):
    q = [string for string in query_string if not string.startswith("page")]
    q.extend([f"page={page}", f"page_size={page_size}"])
    rebuilt_q = "&".join(q)
    return f"{request.scheme}://{request.host}{request.path}?{rebuilt_q}"


def url_for(request: Request, route: str, *args, **kwargs):
    router = request.app.router
    if kwargs.pop("_external", None):
        return f"{request.scheme}://{request.host}{router[route].url_for(**kwargs)}"
    return router[route].url_for(**kwargs)
