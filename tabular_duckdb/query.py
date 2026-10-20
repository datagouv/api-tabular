import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import unquote_plus
from uuid import UUID

# synthetic row identifier, kept for compatibility with the PostgREST-based API
ID_COLUMN = "__id"

GROUP_BY = "groupby"
AGGREGATE_FUNCTIONS = ("count", "avg", "min", "max", "sum")

TEXT_TYPES = ("VARCHAR", "CHAR", "TEXT", "STRING")
# PostgreSQL ordered text with the linguistic collation of the database, DuckDB orders bytes
# (`É` after `Z`): text is sorted and compared with ICU's French rules instead, where accents
# and case only break ties. Unlike glibc, ICU counts spaces and hyphens ("Anne Marie" comes
# before "Anne-Flore", which comes before "Annette"), a convention that does not move with the
# server's glibc version.
TEXT_COLLATION = "fr"
RANGE_COMPARATORS = ("less", "greater", "strictly_less", "strictly_greater")
ORDERED_AGGREGATES = ("min", "max")

COMPARATORS = {
    "exact": "=",
    "differs": "IS DISTINCT FROM",
    "less": "<=",
    "greater": ">=",
    "strictly_less": "<",
    "strictly_greater": ">",
}
SORT_DIRECTIONS = {"asc": "ASC", "desc": "DESC"}
NULLS_PLACEMENTS = {"nullsfirst": "NULLS FIRST", "nullslast": "NULLS LAST"}
RESERVED_ARGUMENTS = ("page", "page_size", "columns")

TRUE_VALUES = ("true", "t", "1", "yes", "y")
FALSE_VALUES = ("false", "f", "0", "no", "n")

# csv-detective python_type of each DuckDB type, as exposed in the profile and the swagger
PYTHON_TYPES: dict[str, str] = {
    "BOOLEAN": "bool",
    "TINYINT": "int",
    "SMALLINT": "int",
    "INTEGER": "int",
    "BIGINT": "int",
    "HUGEINT": "int",
    "UTINYINT": "int",
    "USMALLINT": "int",
    "UINTEGER": "int",
    "UBIGINT": "int",
    "UHUGEINT": "int",
    "FLOAT": "float",
    "REAL": "float",
    "DOUBLE": "float",
    "DATE": "date",
    "TIME": "string",
    "TIMESTAMP": "datetime",
    "TIMESTAMP WITH TIME ZONE": "datetime",
    "TIMESTAMP_S": "datetime",
    "TIMESTAMP_MS": "datetime",
    "TIMESTAMP_NS": "datetime",
    "INTERVAL": "string",
    "VARCHAR": "string",
    "UUID": "string",
    "BLOB": "string",
    "JSON": "json",
}

JSON_LIKE_PREFIXES = ("STRUCT", "MAP", "UNION", "JSON")


def python_type(duckdb_type: str) -> str:
    """Map a DuckDB column type to the csv-detective python_type vocabulary."""
    normalized = duckdb_type.upper()
    if normalized.endswith("[]") or normalized.startswith(JSON_LIKE_PREFIXES):
        return "json"
    if normalized.startswith("DECIMAL"):
        return "float"
    return PYTHON_TYPES.get(normalized, "string")


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# characters carrying the structure of an `or=(...)` group; clients may percent-encode them,
# while the values themselves stay encoded until they reach a leaf condition
STRUCTURAL_CHARACTERS = (("%28", "("), ("%29", ")"), ("%2c", ","), ("%2C", ","), ("%22", '"'))


def decode_structure(value: str) -> str:
    for encoded, character in STRUCTURAL_CHARACTERS:
        value = value.replace(encoded, character)
    return value


def parse_sort(value: str) -> str:
    """PostgREST ordering: `asc` or `desc`, followed or replaced by `nullsfirst` or `nullslast`."""
    parts = value.lower().split(".")
    direction = SORT_DIRECTIONS.get(parts[0])
    if direction:
        parts.pop(0)
    nulls = NULLS_PLACEMENTS.get(parts[0]) if parts else None
    if nulls:
        parts.pop(0)
    if parts or not (direction or nulls):
        raise ValueError(f"invalid sort value '{value}'")
    direction = direction or "ASC"
    # DuckDB puts NULLs last whatever the direction, PostgreSQL treats them as the largest values
    return f"{direction} {nulls or ('NULLS FIRST' if direction == 'DESC' else 'NULLS LAST')}"


def split_column_operator(argument: str) -> tuple[str, str]:
    """`my__column__exact` -> ("my__column", "exact")"""
    if "__" not in argument:
        raise ValueError(f"argument '{argument}' could not be parsed")
    *column_split, comparator = argument.split("__")
    return "__".join(column_split), comparator.lower()


def is_text(duckdb_type: str) -> bool:
    return duckdb_type.upper() in TEXT_TYPES


def collated(quoted_column: str) -> str:
    return f"{quoted_column} COLLATE {TEXT_COLLATION}"


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
    kind = python_type(duckdb_type)
    if kind == "json":
        return value, True
    try:
        if is_text(duckdb_type):
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
        if kind == "int":
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
    for item in split_top_level(value):
        item = item.strip()
        if len(item) >= 2 and item.startswith('"') and item.endswith('"'):
            item = item[1:-1]
        items.append(item)
    return items


def build_condition(argument: str, value: str, columns: Mapping[str, str]) -> tuple[str, list]:
    """Translate a single `<column>__<operator>[=<value>]` filter into SQL."""
    if argument in RESERVED_ARGUMENTS:
        raise ValueError(f"Argument `{argument}` can't be set within an operator")
    column_name, comparator = split_column_operator(argument)
    if comparator == "sort":
        raise ValueError(f"Argument `{argument}` can't be set within an operator")

    quoted, duckdb_type = resolve_column(column_name, columns)

    if comparator == "isnull":
        return f"({quoted} IS NULL)", []
    if comparator == "isnotnull":
        return f"({quoted} IS NOT NULL)", []
    if comparator in ("contains", "notcontains"):
        condition = f"(CAST({quoted} AS VARCHAR) ILIKE '%' || ? || '%')"
        # `*` is the wildcard of PostgREST's ilike patterns
        pattern = value.replace("*", "%")
        return (condition if comparator == "contains" else f"(NOT {condition})"), [pattern]
    if comparator in ("in", "notin"):
        items = _split_list(value)
        if not items:
            # PostgREST's `in.()`: no row is in an empty list, NULLs included
            return ("(FALSE)" if comparator == "in" else "(TRUE)"), []
        coerced = [coerce_value(item, duckdb_type) for item in items]
        as_varchar = any(fallback for _, fallback in coerced)
        params = [item for item, _ in coerced]
        placeholders = ", ".join("?" for _ in params)
        operator = "IN" if comparator == "in" else "NOT IN"
        return f"({_comparable(quoted, as_varchar)} {operator} ({placeholders}))", params
    if comparator in COMPARATORS:
        coerced, as_varchar = coerce_value(value, duckdb_type)
        compared = _comparable(quoted, as_varchar)
        # equality stays exact, as with the deterministic collations of PostgreSQL
        if comparator in RANGE_COMPARATORS and is_text(duckdb_type):
            compared = collated(compared)
        return f"({compared} {COMPARATORS[comparator]} ?)", [coerced]
    raise ValueError(f"argument '{argument}={value}' could not be parsed")


def split_top_level(value: str) -> list[str]:
    """Split on the commas that are not nested within parentheses or double quotes."""
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
        elif char == "," and depth == 0 and not quoted:
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
                unquote_plus(param).replace('"', ""), "", columns
            )
        else:
            argument, value = find_arg_val(param)
            condition, condition_params = build_condition(
                unquote_plus(argument), unquote_plus(value), columns
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
    # operator -> column names, in the order of the query string, which is the order of the
    # output columns
    aggregators: dict[str, list[str]] = field(default_factory=dict)
    # aggregated columns holding text, whose min and max follow TEXT_COLLATION
    text_columns: set[str] = field(default_factory=set)
    limit: int | None = None
    offset: int = 0

    @property
    def is_aggregated(self) -> bool:
        """Like PostgREST, `groupby` alone only selects its columns: rows are grouped once an
        aggregate function is requested."""
        return any(operator != GROUP_BY for operator in self.aggregators)

    @property
    def group_by(self) -> list[str]:
        return [quote_ident(column) for column in self.aggregators.get(GROUP_BY, [])]

    def where_clause(self) -> str:
        return f" WHERE {' AND '.join(self.conditions)}" if self.conditions else ""

    def select_clause(self) -> str:
        if self.aggregators:
            selected = []
            for operator, column_names in self.aggregators.items():
                for column in column_names:
                    quoted = quote_ident(column)
                    if operator == GROUP_BY:
                        selected.append(quoted)
                        continue
                    if operator in ORDERED_AGGREGATES and column in self.text_columns:
                        quoted = collated(quoted)
                    alias = quote_ident(f"{column}__{operator}")
                    selected.append(f"{operator}({quoted}) AS {alias}")
            return ", ".join(selected)
        if self.selected:
            return ", ".join(self.selected)
        quoted_id = quote_ident(ID_COLUMN)
        return f"{quoted_id}, * EXCLUDE ({quoted_id})"

    def from_clause(self, source: str) -> str:
        sql = f"FROM {source}{self.where_clause()}"
        if self.is_aggregated and self.group_by:
            sql += f" GROUP BY {', '.join(self.group_by)}"
        return sql

    def data_sql(self, source: str) -> tuple[str, list]:
        # the last keys break the ties of the requested sort, so that pagination is
        # deterministic: __id for rows, the grouped columns for groups
        tiebreak = self.group_by if self.is_aggregated else [f"{quote_ident(ID_COLUMN)} ASC"]
        order_by = [*self.order_by, *tiebreak]
        sql = f"SELECT {self.select_clause()} {self.from_clause(source)}"
        if order_by:
            sql += f" ORDER BY {', '.join(order_by)}"
        if self.limit is not None:
            sql += f" LIMIT {int(self.limit)}"
        if self.offset:
            sql += f" OFFSET {int(self.offset)}"
        return sql, list(self.params)

    def count_sql(self, source: str) -> tuple[str, list]:
        if self.is_aggregated:
            return (
                f"SELECT count(*) FROM (SELECT {self.select_clause()} {self.from_clause(source)})",
                list(self.params),
            )
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
        if raw_value.count("=") > 0:
            # like the PostgREST-based API, typically a missing `&` between two arguments
            raise ValueError(f"argument '{unquote_plus(raw_arg)}' could not be parsed")
        key = unquote_plus(key)
        value = unquote_plus(raw_value)
        if not separator:
            # without a value: an aggregator, or `is(not)null`
            column_name, comparator = split_column_operator(key)
            if comparator == GROUP_BY or comparator in AGGREGATE_FUNCTIONS:
                _, duckdb_type = resolve_column(column_name, columns)
                parsed.aggregators.setdefault(comparator, []).append(column_name)
                if is_text(duckdb_type):
                    parsed.text_columns.add(column_name)
                continue
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
        if "__" not in key:
            raise ValueError(f"argument '{key}={value}' could not be parsed")
        column_name, comparator = split_column_operator(key)
        if comparator == "sort":
            quoted, duckdb_type = resolve_column(column_name, columns)
            sorted_by = collated(quoted) if is_text(duckdb_type) else quoted
            try:
                parsed.order_by.append(f"{sorted_by} {parse_sort(value)}")
            except ValueError:
                raise ValueError(f"argument '{key}={value}' could not be parsed") from None
            continue
        condition, params = build_condition(key, value, columns)
        parsed.conditions.append(condition)
        parsed.params.extend(params)
    if parsed.aggregators and parsed.selected is not None:
        raise ValueError("the argument `columns` cannot be set alongside aggregators")
    return parsed
