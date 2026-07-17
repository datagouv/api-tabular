import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DuckDBQuery:
    """SQL fragments ready to run against a Parquet source aliased as `src`."""

    where_sql: str = ""
    where_params: list = field(default_factory=list)
    order_sql: str = ""
    select_sql: str = "*"
    group_by_sql: str = ""
    is_aggregation: bool = False
    limit: int | None = None
    offset: int = 0


@dataclass
class FilterAction:
    kind: str  # where | order | select
    sql: str = ""
    params: list[Any] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_column_and_operator(argument: str) -> tuple[str, str]:
    *column_split, comparator = argument.split("__")
    column = "__".join(column_split)
    return column, comparator.lower()


def build_duckdb_query(
    request_arg: list[str],
    page_size: int | None = None,
    offset: int = 0,
) -> DuckDBQuery:
    where_parts: list[str] = []
    where_params: list[Any] = []
    order_sql = ""
    select_columns: list[str] | None = None
    aggregators: dict[str, list[str]] = defaultdict(list)
    has_sort = False

    for arg in request_arg:
        if arg.startswith("or=("):
            clause, params = parse_operator(query=arg, operator="or", top_level=True)
            where_parts.append(clause)
            where_params.extend(params)
            continue
        _split = arg.split("=")
        if len(_split) == 2:
            action = add_filter(*_split)
            if action is None:
                continue
            if action.kind == "where":
                where_parts.append(action.sql)
                where_params.extend(action.params)
            elif action.kind == "order":
                order_sql = action.sql
                has_sort = True
            elif action.kind == "select":
                select_columns = action.columns
        elif len(_split) == 1:
            if "__" not in _split[0]:
                raise ValueError(f"argument '{arg}' could not be parsed")
            if _split[0].split("__")[-1] in ["isnull", "isnotnull"]:
                action = add_filter(_split[0], "")
                assert action is not None and action.kind == "where"
                where_parts.append(action.sql)
                where_params.extend(action.params)
            else:
                column, operator = add_aggregator(_split[0])
                aggregators[operator].append(column)
        else:
            raise ValueError(f"argument '{arg}' could not be parsed")

    is_aggregation = bool(aggregators)
    if is_aggregation and select_columns is not None:
        raise ValueError("the argument `columns` cannot be set alongside aggregators")

    select_sql = "*"
    group_by_sql = ""
    if is_aggregation:
        select_parts: list[str] = []
        group_cols: list[str] = []
        for operator, columns in aggregators.items():
            if operator == "groupby":
                for column in columns:
                    ident = quote_ident(column)
                    select_parts.append(ident)
                    group_cols.append(ident)
            else:
                for column in columns:
                    ident = quote_ident(column)
                    alias = quote_ident(f"{column}__{operator}")
                    select_parts.append(f"{operator.upper()}({ident}) AS {alias}")
        select_sql = ", ".join(select_parts)
        if group_cols:
            group_by_sql = ", ".join(group_cols)
    elif select_columns is not None:
        select_sql = ", ".join(quote_ident(c) for c in select_columns)

    if not has_sort and not is_aggregation:
        order_sql = f"ORDER BY {quote_ident('__id')} ASC"

    where_sql = ""
    if where_parts:
        where_sql = "WHERE " + " AND ".join(where_parts)

    return DuckDBQuery(
        where_sql=where_sql,
        where_params=where_params,
        order_sql=order_sql,
        select_sql=select_sql,
        group_by_sql=f"GROUP BY {group_by_sql}" if group_by_sql else "",
        is_aggregation=is_aggregation,
        limit=page_size,
        offset=offset,
    )


def add_filter(
    argument: str,
    value: str,
    *,
    in_operator: bool = False,
) -> FilterAction | None:
    if argument in ["page", "page_size"]:
        if in_operator:
            raise ValueError(f"Argument `{argument}` can't be set within an operator")
        return None
    if argument == "columns":
        if in_operator:
            raise ValueError(f"Argument `{argument}` can't be set within an operator")
        if '"' in value:
            raise ValueError('Forbidden character `"` in a column name')
        return FilterAction(kind="select", columns=value.split(","))
    if "__" not in argument:
        raise ValueError(f"argument '{argument}={value}' could not be parsed")

    column, normalized_comparator = get_column_and_operator(argument)
    ident = quote_ident(column)

    if normalized_comparator == "sort":
        if in_operator:
            raise ValueError(f"Argument `{argument}` can't be set within an operator")
        direction = value.upper()
        if direction not in ("ASC", "DESC"):
            raise ValueError(f"argument '{argument}={value}' could not be parsed")
        return FilterAction(kind="order", sql=f"ORDER BY {ident} {direction}")
    if normalized_comparator == "exact":
        return FilterAction(kind="where", sql=f"{ident} = ?", params=[coerce_value(value)])
    if normalized_comparator == "differs":
        return FilterAction(
            kind="where", sql=f"{ident} IS DISTINCT FROM ?", params=[coerce_value(value)]
        )
    if normalized_comparator == "isnull":
        return FilterAction(kind="where", sql=f"{ident} IS NULL")
    if normalized_comparator == "isnotnull":
        return FilterAction(kind="where", sql=f"{ident} IS NOT NULL")
    if normalized_comparator == "contains":
        return FilterAction(kind="where", sql=f"{ident} ILIKE '%' || ? || '%'", params=[value])
    if normalized_comparator == "notcontains":
        return FilterAction(
            kind="where", sql=f"NOT ({ident} ILIKE '%' || ? || '%')", params=[value]
        )
    if normalized_comparator == "in":
        values = value.split(",")
        placeholders = ", ".join("?" for _ in values)
        return FilterAction(
            kind="where",
            sql=f"{ident} IN ({placeholders})",
            params=[coerce_value(v) for v in values],
        )
    if normalized_comparator == "notin":
        values = value.split(",")
        placeholders = ", ".join("?" for _ in values)
        return FilterAction(
            kind="where",
            sql=f"{ident} NOT IN ({placeholders})",
            params=[coerce_value(v) for v in values],
        )
    if normalized_comparator == "less":
        return FilterAction(kind="where", sql=f"{ident} <= ?", params=[coerce_value(value)])
    if normalized_comparator == "greater":
        return FilterAction(kind="where", sql=f"{ident} >= ?", params=[coerce_value(value)])
    if normalized_comparator == "strictly_less":
        return FilterAction(kind="where", sql=f"{ident} < ?", params=[coerce_value(value)])
    if normalized_comparator == "strictly_greater":
        return FilterAction(kind="where", sql=f"{ident} > ?", params=[coerce_value(value)])
    raise ValueError(f"argument '{argument}={value}' could not be parsed")


def coerce_value(value: str):
    """Coerce query-string values. Keep numbers as strings so DuckDB can cast to the column type."""
    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    return value


def add_aggregator(argument: str) -> tuple[str, str]:
    if "__" not in argument:
        raise ValueError(f"argument '{argument}' could not be parsed")
    column, operator = get_column_and_operator(argument)
    if operator in ["avg", "count", "max", "min", "sum", "groupby"]:
        return column, operator
    raise ValueError(f"argument '{argument}' could not be parsed")


def split_top_level(s: str) -> list[str]:
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
        # col__op.val — value may contain dots (e.g. 0.1)
        if "." not in param:
            raise ValueError(f"argument '{param}' could not be parsed")
        argument, value = param.split(".", 1)
        if "__" not in argument:
            raise ValueError(f"argument '{param}' could not be parsed")
        return argument, value
    if param.count('"') == 4:
        col_op = re.findall(column_operator_pattern, param)
        val = re.findall(value_pattern, param)
        if len(col_op) != 1 or len(val) != 1:
            raise ValueError(f"argument '{param}' could not be parsed")
        return col_op[0].replace('"', ""), val[0][1:]
    col_op = re.findall(column_operator_pattern, param)
    val = re.findall(value_pattern, param)
    if not col_op:
        return param.split(".", 1)[0], val[0][1:]
    return col_op[0].replace('"', ""), param.split(".")[-1]


def parse_operator(query: str, operator: str, top_level: bool = False) -> tuple[str, list]:
    if not query.endswith(")"):
        raise ValueError(f"argument '{query}' could not be parsed")
    params_sql: list[str] = []
    params_values: list = []
    inner = re.findall(rf"^{operator}{'=' if top_level else ''}\((.*)\)$", query)[0]
    for param in split_top_level(inner):
        if param.startswith(("and(", "or(")):
            clause, values = parse_operator(query=param, operator=param.split("(")[0])
            params_sql.append(clause)
            params_values.extend(values)
        elif param.endswith(("__isnull", "__isnotnull")):
            action = add_filter(param.replace('"', ""), "", in_operator=True)
            assert action is not None and action.kind == "where"
            params_sql.append(action.sql)
            params_values.extend(action.params)
        else:
            argument, value = find_arg_val(param)
            if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            action = add_filter(argument, value, in_operator=True)
            assert action is not None and action.kind == "where"
            params_sql.append(action.sql)
            params_values.extend(action.params)
    joiner = f" {operator.upper()} "
    return f"({joiner.join(params_sql)})", params_values
