import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import unquote_plus
from uuid import UUID

from tabular_duckdb.config import settings
from tabular_duckdb.errors import QueryException

# synthetic row identifier, kept for compatibility with the PostgREST-based API
ID_COLUMN = "__id"

# aggregation moved to the dedicated read-only SQL endpoint
AGGREGATORS = ("groupby", "count", "avg", "min", "max", "sum")
AGGREGATION_HINT = (
    "aggregation operators (`groupby`, `count`, `avg`, `min`, `max`, `sum`) are not supported "
    "by this endpoint, use `/api/resources/<resource_id>/sql/?q=...` to run a GROUP BY query"
)

COMPARATORS = {
    "exact": "=",
    "differs": "IS DISTINCT FROM",
    "less": "<=",
    "greater": ">=",
    "strictly_less": "<",
    "strictly_greater": ">",
}
SORT_DIRECTIONS = {"asc": "ASC", "desc": "DESC"}
RESERVED_ARGUMENTS = ("page", "page_size", "columns")

TRUE_VALUES = ("true", "t", "1", "yes", "y")
FALSE_VALUES = ("false", "f", "0", "no", "n")

INTEGER_TYPES = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# characters carrying the structure of an `or=(...)` group; clients may percent-encode them,
# while the values themselves stay encoded until they reach a leaf condition
STRUCTURAL_CHARACTERS = (("%28", "("), ("%29", ")"), ("%2c", ","), ("%2C", ","), ("%22", '"'))


def decode_structure(value: str) -> str:
    for encoded, character in STRUCTURAL_CHARACTERS:
        value = value.replace(encoded, character)
    return value


def build_offset(page: int, page_size: int) -> int:
    if page_size > settings.PAGE_SIZE_MAX:
        raise QueryException(
            400,
            None,
            "Invalid query string",
            f"Page size exceeds allowed maximum: {settings.PAGE_SIZE_MAX}",
        )
    return page_size * (page - 1) if page > 1 else 0


def split_column_operator(argument: str) -> tuple[str, str]:
    """`my__column__exact` -> ("my__column", "exact")"""
    if "__" not in argument:
        raise ValueError(f"argument '{argument}' could not be parsed")
    *column_split, comparator = argument.split("__")
    return "__".join(column_split), comparator.lower()


def resolve_column(name: str, columns: Mapping[str, str]) -> tuple[str, str]:
    """Check the column against the parquet schema and return (quoted name, DuckDB type)."""
    if name not in columns:
        raise ValueError(f"unknown column '{name}'")
    return quote_ident(name), columns[name]


def coerce_value(value: str, duckdb_type: str) -> tuple[Any, bool]:
    """Convert a query string value to a bindable Python value.

    Returns (value, compare_as_varchar): nested values (lists, structs, JSON) and types without
    a Python counterpart are compared through their text rendering. A value that does not fit
    the column type is rejected, like PostgreSQL did, rather than compared as a string, which
    would silently return wrong rows ('9' >= '12.5').
    """
    normalized = duckdb_type.upper()
    if normalized.endswith("[]") or normalized.startswith(("STRUCT", "MAP", "UNION", "JSON")):
        return value, True
    try:
        if normalized in ("VARCHAR", "CHAR", "TEXT", "STRING"):
            return value, False
        if normalized == "UUID":
            UUID(value)
            return value, False
        if normalized == "BOOLEAN":
            lowered = value.strip().lower()
            if lowered in TRUE_VALUES:
                return True, False
            if lowered in FALSE_VALUES:
                return False, False
            raise ValueError(value)
        if normalized.startswith("DECIMAL"):
            return Decimal(value), False
        if normalized in ("FLOAT", "REAL", "DOUBLE"):
            return float(value), False
        if normalized in INTEGER_TYPES:
            return int(value), False
        if normalized == "DATE":
            return date.fromisoformat(value), False
        if normalized.startswith("TIMESTAMP"):
            try:
                return datetime.fromisoformat(value), False
            except ValueError:
                return date.fromisoformat(value), False
        if normalized == "TIME":
            return time.fromisoformat(value), False
    except (ValueError, InvalidOperation) as exc:
        raise ValueError(f"invalid value '{value}' for a column of type {duckdb_type}") from exc
    return value, True


def _comparable(quoted_column: str, as_varchar: bool) -> str:
    return f"CAST({quoted_column} AS VARCHAR)" if as_varchar else quoted_column


def _split_list(value: str) -> list[str]:
    """`a,"b,c",d` -> ["a", "b,c", "d"]"""
    items = []
    for item in split_top_level(value, separators=(",",)):
        item = item.strip()
        if len(item) >= 2 and item.startswith('"') and item.endswith('"'):
            item = item[1:-1]
        items.append(item)
    return items


def build_condition(
    argument: str,
    value: str,
    columns: Mapping[str, str],
    *,
    in_operator: bool = False,
) -> tuple[str, list]:
    """Translate a single `<column>__<operator>[=<value>]` filter into SQL."""
    if argument in RESERVED_ARGUMENTS:
        raise ValueError(f"Argument `{argument}` can't be set within an operator")
    column_name, comparator = split_column_operator(argument)
    if comparator in AGGREGATORS:
        raise ValueError(AGGREGATION_HINT)
    if comparator == "sort":
        raise ValueError(f"Argument `{argument}` can't be set within an operator")

    quoted, duckdb_type = resolve_column(column_name, columns)

    if comparator == "isnull":
        return f"({quoted} IS NULL)", []
    if comparator == "isnotnull":
        return f"({quoted} IS NOT NULL)", []
    if comparator in ("contains", "notcontains"):
        condition = f"(CAST({quoted} AS VARCHAR) ILIKE '%' || ? || '%')"
        return (condition if comparator == "contains" else f"(NOT {condition})"), [value]
    if comparator in ("in", "notin"):
        items = _split_list(value)
        if not items:
            raise ValueError(f"argument '{argument}={value}' could not be parsed")
        coerced = [coerce_value(item, duckdb_type) for item in items]
        as_varchar = any(fallback for _, fallback in coerced)
        params = [item if not as_varchar else raw for (item, _), raw in zip(coerced, items)]
        placeholders = ", ".join("?" for _ in params)
        operator = "IN" if comparator == "in" else "NOT IN"
        return f"({_comparable(quoted, as_varchar)} {operator} ({placeholders}))", params
    if comparator in COMPARATORS:
        coerced, as_varchar = coerce_value(value, duckdb_type)
        return (
            f"({_comparable(quoted, as_varchar)} {COMPARATORS[comparator]} ?)",
            [coerced],
        )
    raise ValueError(f"argument '{argument}={value}' could not be parsed")


def split_top_level(value: str, separators: tuple[str, ...] = (",",)) -> list[str]:
    """Split on separators that are not nested within parentheses or double quotes."""
    parts: list[str] = []
    current = ""
    depth = 0
    quoted = False
    for char in value:
        if char == '"':
            quoted = not quoted
            current += char
        elif char == "(" and not quoted:
            depth += 1
            current += char
        elif char == ")" and not quoted:
            depth -= 1
            current += char
        elif char in separators and depth == 0 and not quoted:
            parts.append(current)
            current = ""
        else:
            current += char
    if current:
        parts.append(current)
    return parts


def find_arg_val(param: str) -> tuple[str, str]:
    """Split a condition of an OR/AND group into (argument, value).

    Handles the four possible shapes: `col__op.val`, `"col.umn"__op.val`,
    `col__op."val.ue"` and `"col.umn"__op."val.ue"`.
    """
    if param.count('"') not in {0, 2, 4}:
        raise ValueError(f"argument '{param}' could not be parsed")
    column_operator_pattern = r'^"[^"]*"__[a-zA-Z_]+'
    value_pattern = r'\."[^"]*"$'
    if param.count('"') == 0:
        _split = param.split(".")
        if len(_split) != 2:
            raise ValueError(f"argument '{param}' could not be parsed")
        return _split[0], _split[1]
    column_operator = re.findall(column_operator_pattern, param)
    value = re.findall(value_pattern, param)
    if param.count('"') == 4:
        if len(column_operator) != 1 or len(value) != 1:
            raise ValueError(f"argument '{param}' could not be parsed")
        return column_operator[0].replace('"', ""), value[0][2:-1]
    if not column_operator:
        # case col__op."val.ue"
        if not value:
            raise ValueError(f"argument '{param}' could not be parsed")
        return param.split(".")[0], value[0][2:-1]
    # case "col.umn"__op.val
    return column_operator[0].replace('"', ""), param.split(".")[-1]


def parse_operator(
    query: str,
    operator: str,
    columns: Mapping[str, str],
    top_level: bool = False,
) -> tuple[str, list]:
    """Translate `or=(a__exact.1,and(b__less.2,c__isnotnull))` into a parenthesized SQL group."""
    if not query.endswith(")"):
        raise ValueError(f"argument '{query}' could not be parsed")
    match = re.findall(rf"^{operator}{'=' if top_level else ''}\((.*)\)$", query)
    if not match:
        raise ValueError(f"argument '{query}' could not be parsed")
    conditions: list[str] = []
    params: list[Any] = []
    for param in split_top_level(match[0]):
        if param.startswith(("and(", "or(")):
            condition, condition_params = parse_operator(param, param.split("(")[0], columns)
        elif param.endswith(("__isnull", "__isnotnull")):
            condition, condition_params = build_condition(
                unquote_plus(param).replace('"', ""), "", columns, in_operator=True
            )
        else:
            argument, value = find_arg_val(param)
            condition, condition_params = build_condition(
                unquote_plus(argument), unquote_plus(value), columns, in_operator=True
            )
        conditions.append(condition)
        params.extend(condition_params)
    if not conditions:
        raise ValueError(f"argument '{query}' could not be parsed")
    joiner = " OR " if operator == "or" else " AND "
    return f"({joiner.join(conditions)})", params


@dataclass
class ParsedQuery:
    """SQL pieces built from the query string, applied on top of a source relation."""

    selected: list[str] | None = None
    conditions: list[str] = field(default_factory=list)
    params: list[Any] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    limit: int | None = None
    offset: int = 0

    def where_clause(self) -> str:
        return f" WHERE {' AND '.join(self.conditions)}" if self.conditions else ""

    def select_clause(self) -> str:
        if self.selected:
            return ", ".join(self.selected)
        quoted_id = quote_ident(ID_COLUMN)
        return f"{quoted_id}, * EXCLUDE ({quoted_id})"

    def data_sql(self, source: str) -> tuple[str, list]:
        order_by = self.order_by or [f"{quote_ident(ID_COLUMN)} ASC"]
        sql = (
            f"SELECT {self.select_clause()} FROM {source}{self.where_clause()}"
            f" ORDER BY {', '.join(order_by)}"
        )
        if self.limit is not None:
            sql += f" LIMIT {int(self.limit)}"
        if self.offset:
            sql += f" OFFSET {int(self.offset)}"
        return sql, list(self.params)

    def count_sql(self, source: str) -> tuple[str, list]:
        return f"SELECT count(*) FROM {source}{self.where_clause()}", list(self.params)


def build_query(
    request_args: list[str],
    columns: Mapping[str, str],
    page_size: int | None = None,
    offset: int = 0,
) -> ParsedQuery:
    """Translate the raw query string arguments into SQL pieces."""
    parsed = ParsedQuery(limit=page_size, offset=offset)
    for raw_arg in request_args:
        if not raw_arg:
            continue
        key, separator, raw_value = raw_arg.partition("=")
        if key.lower() == "or":
            condition, params = parse_operator(
                f"or={decode_structure(raw_value)}", "or", columns, top_level=True
            )
            parsed.conditions.append(condition)
            parsed.params.extend(params)
            continue
        key = unquote_plus(key)
        value = unquote_plus(raw_value)
        if not separator:
            # without a value, only `is(not)null` makes sense
            _, comparator = split_column_operator(key)
            if comparator in AGGREGATORS:
                raise ValueError(AGGREGATION_HINT)
            if comparator not in ("isnull", "isnotnull"):
                raise ValueError(f"argument '{key}' could not be parsed")
            condition, params = build_condition(key, "", columns)
            parsed.conditions.append(condition)
            parsed.params.extend(params)
            continue
        if key in ("page", "page_size"):
            # handled by the caller, which needs them to build the pagination links
            continue
        if key == "columns":
            if '"' in value:
                raise ValueError('Forbidden character `"` in a column name')
            parsed.selected = [
                resolve_column(column, columns)[0] for column in value.split(",") if column
            ]
            if not parsed.selected:
                raise ValueError(f"argument '{raw_arg}' could not be parsed")
            continue
        column_name, comparator = split_column_operator(key)
        if comparator == "sort":
            direction = SORT_DIRECTIONS.get(value.lower())
            if direction is None:
                raise ValueError(f"argument '{key}={value}' could not be parsed")
            quoted, _ = resolve_column(column_name, columns)
            parsed.order_by.append(f"{quoted} {direction}")
            continue
        condition, params = build_condition(key, value, columns)
        parsed.conditions.append(condition)
        parsed.params.extend(params)
    return parsed
