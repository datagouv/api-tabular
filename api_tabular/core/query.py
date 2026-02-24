from collections import defaultdict

from api_tabular.core.utils import is_aggregation_allowed


def build_sql_query_string(
    request_arg: list,
    resource_id: str | None = None,
    indexes: set | None = None,
    page_size: int | None = None,
    offset: int = 0,
) -> str:
    sql_query = []
    or_groups = defaultdict(list)
    aggregators = defaultdict(list)
    sorted = False
    for arg in request_arg:
        _split = arg.split("=")
        fail = True
        # filters are expected to have the syntax `<column_name>__<operator>=<value>`
        if len(_split) == 2:
            # handling when is(not)null is within an OR group
            if _split[0].startswith("or__"):
                # so that we go through the correct if-block
                _split = _split + [None]
            else:
                _filter, _sorted = add_filter(*_split)
                if _filter:
                    sorted = sorted or _sorted
                    sql_query.append(_filter)
                fail = False
        # aggregators are expected to have the syntax `<column_name>__<operator>`
        # is(not)null also has this syntax but is a filter
        if len(_split) == 1:
            if _split[0].split("__")[1] in ["isnull", "isnotnull"]:
                _filter, _ = add_filter(_split[0], None)
                sql_query.append(_filter)
            else:
                column, operator = add_aggregator(_split[0], indexes)
                if column:
                    aggregators[operator].append(column)
            fail = False
        # filters that are within an OR group are expected to have the syntax `or__<group>=<column_name>__<operator>=<value>`
        if len(_split) == 3:
            _or, *ored_filter = _split
            split_or = _or.split("__")
            if len(split_or) != 2 or split_or[0] != "or":
                raise ValueError(f"The OR group `{_or}` is malformed")
            _filter, _ = add_filter(*ored_filter, in_or=True)
            or_groups[split_or[1]].append(_filter)
            fail = False
        if fail:
            raise ValueError(f"argument '{arg}' could not be parsed")
    for group_filters in or_groups.values():
        sql_query.append(f"or=({','.join(group_filters)})")
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


def add_filter(
    argument: str,
    value: str | None,
    *,
    in_or: bool = False,
) -> tuple[str | None, bool]:
    if argument in ["page", "page_size"]:  # processed differently
        if in_or:
            raise ValueError(f"Argument `{argument}` can't be set in an OR group")
        return None, False
    if argument == "columns":
        if in_or:
            raise ValueError(f"Argument `{argument}` can't be set in an OR group")
        return f"select={value}", False
    if "__" in argument:
        column, normalized_comparator = get_column_and_operator(argument)
        # when encapsulated in an OR statement, the syntax is `col.eq.val` instead of `col=eq.val`
        op = "." if in_or else "="
        if normalized_comparator == "sort":
            if in_or:
                raise ValueError(f"Argument `{argument}` can't be set in an OR group")
            return f"order={column}.{value}", True
        elif normalized_comparator == "exact":
            return f"{column}{op}eq.{value}", False
        elif normalized_comparator == "differs":
            return f"{column}{op}isdistinct.{value}", False
        elif normalized_comparator == "isnull":
            return f"{column}{op}is.null", False
        elif normalized_comparator == "isnotnull":
            return f"{column}{op}not.is.null", False
        elif normalized_comparator == "contains":
            return f"{column}{op}ilike.*{value}*", False
        elif normalized_comparator == "notcontains":
            return f"{column}{op}not.ilike.*{value}*", False
        elif normalized_comparator == "in":
            return f"{column}{op}in.({value})", False
        elif normalized_comparator == "notin":
            return f"{column}{op}not.in.({value})", False
        elif normalized_comparator == "less":
            return f"{column}{op}lte.{value}", False
        elif normalized_comparator == "greater":
            return f"{column}{op}gte.{value}", False
        elif normalized_comparator == "strictly_less":
            return f"{column}{op}lt.{value}", False
        elif normalized_comparator == "strictly_greater":
            return f"{column}{op}gt.{value}", False
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
