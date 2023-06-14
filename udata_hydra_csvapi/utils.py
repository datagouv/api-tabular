from slugify import slugify


def build_sql_query_string(request_arg: str, page_size: int, offset: int) -> str:
    sql_query = []
    for arg in request_arg:
        argument, value = arg.split('=')
        if '__' in argument:
            column, comparator = argument.split('__')
            normalized_column = slugify(column, separator='_', allow_unicode=True)
            normalized_comparator = comparator.lower()
            if normalized_comparator == 'sort':
                if value == 'asc':
                    sql_query.append(f'order={normalized_column}.asc')
                elif value == 'desc':
                    sql_query.append(f'order={normalized_column}.desc')
            elif normalized_comparator == 'exact':
                sql_query.append(f'{normalized_column}=eq.{value}')
            elif normalized_comparator == 'contains':
                sql_query.append(f'{normalized_column}=like.*{value}*')
            elif normalized_comparator == 'less':
                sql_query.append(f'{normalized_column}=lte.{value}')
            elif normalized_comparator == 'greater':
                sql_query.append(f'{normalized_column}=gte.{value}')
    sql_query.append(f'limit={page_size}')
    sql_query.append(f'offset={offset}')
    return '&'.join(sql_query)


def process_total(raw_total: str) -> int:
    # The raw total looks like this: '0-49/21777'
    _, str_total = raw_total.split('/')
    return int(str_total)


def build_link_with_page(path, query_string, page, page_size):
    q = [string for string in query_string if not string.startswith('page')]
    q.extend([f'page={page}', f'page_size={page_size}'])
    rebuilt_q = '&'.join(q)
    return f"{path}?{rebuilt_q}"
