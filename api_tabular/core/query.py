import re
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
    aggregators = defaultdict(list)
    sorted = False
    for arg in request_arg:
        if arg.startswith("or=("):
            # top level "and=(...)" should not happen
            sql_query.append(parse_operator(query=arg, operator="or", top_level=True))
            continue
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


def add_filter(
    argument: str,
    value: str | None,
    *,
    in_operator: bool = False,
) -> tuple[str | None, bool]:
    if argument in ["page", "page_size"]:  # processed differently
        if in_operator:
            raise ValueError(f"Argument `{argument}` can't be set within an operator")
        return None, False
    if argument == "columns":
        if in_operator:
            raise ValueError(f"Argument `{argument}` can't be set within an operator")
        return f"select={value}", False
    if "__" in argument:
        column, normalized_comparator = get_column_and_operator(argument)
        # when encapsulated in an OR statement, the syntax is `col.eq.val` instead of `col=eq.val`
        op = "." if in_operator else "="
        if normalized_comparator == "sort":
            if in_operator:
                raise ValueError(f"Argument `{argument}` can't be set within an operator")
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


def split_top_level(s: str) -> list[str]:
    # we can't .split(",") as there may be commas within the params (if nested)
    # so we need a custom "split by ',' if ',' not within parentheses"
    parts = []
    current = ""
    depth = 0
    for char in s:
        if char == "(":
            depth += 1
            current += char
        elif char == ")":
            depth -= 1
            current += char
        elif char == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += char
    if current:
        parts.append(current)
    return parts


def find_arg_val(param: str) -> tuple[str, str]:
    if param.count('"') not in {0, 2, 4}:
        raise ValueError(f"argument '{param}' could not be parsed")
    column_operator_pattern = r'^"[^"]*"__[a-z]+'
    value_pattern = r'\."[^"]*"$'
    if param.count('"') == 0:
        # no quote, we expect col__op.val
        _split = param.split(".")
        if len(_split) != 2:
            raise ValueError(f"argument '{param}' could not be parsed")
        return tuple(_split)
    elif param.count('"') == 4:
        # 4 quotes, we expect "col.umn"__op."val.ue"
        col_op = re.findall(column_operator_pattern, param)
        val = re.findall(value_pattern, param)
        if len(col_op) != 1 or len(val) != 1:
            raise ValueError(f"argument '{param}' could not be parsed")
        # we drop the double quotes around the column as they will be added back in get_column_and_operator
        # the dot is included in the value result, we pop it in the result
        return col_op[0].replace('"', ""), val[0][1:]
    else:
        # we have any of "col.umn"__op.val, col__op."val.ue"
        col_op = re.findall(column_operator_pattern, param)
        val = re.findall(value_pattern, param)
        if not col_op:
            # case col__op."val.ue"
            return param.split(".")[0], val[0][1:]
        else:
            # case "col.umn"__op.val
            return col_op[0].replace('"', ""), param.split(".")[-1]


def parse_operator(query: str, operator: str, top_level: bool = False):
    # only the top level operator has an "=", nested operators are "or(...)" and "and(...)"
    if not query.endswith(")"):
        raise ValueError(f"argument '{query}' could not be parsed")
    postgrest_params = []
    # we can safely assume that there will be one result for the regex
    params = split_top_level(
        re.findall(rf"^{operator}{'=' if top_level else ''}\((.*)\)$", query)[0]
    )
    for param in params:
        if param.startswith(("and(", "or(")):
            # recursively adding the nested confitions
            postgrest_params.append(parse_operator(query=param, operator=param.split("(")[0]))
        else:
            if param.endswith(("__isnull", "__isnotnull")):
                postgrest_params.append(
                    add_filter(
                        param.replace('"', ""),
                        None,
                        in_operator=True,
                    )[0]
                )
            else:
                # if a special character is in the column name and/or value, the problematic item should be encapsulated in double quotes
                # so we have 4 possibles cases: col__op.val, "col.umn"__op.val, col__op."val.ue" and "col.umn"__op."val.ue"
                argument, value = find_arg_val(param)
                postgrest_params.append(add_filter(argument, value, in_operator=True)[0])
    return f"{operator}{'=' if top_level else ''}({','.join(postgrest_params)})"


def raise_if_not_index(column_name: str, indexes: set | None) -> None:
    if indexes is None:
        return
    # we pop the heading and trailing " that were added upstream
    if column_name[1:-1] not in indexes:
        raise PermissionError(f"{column_name[1:-1]} is not among the allowed columns: {indexes}")
