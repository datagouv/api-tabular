import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import unquote_plus
from uuid import UUID

# synthetic row identifier, kept for compatibility with the PostgREST-based API
ID_COLUMN = "__id"

GROUP_BY = "groupby"
AGGREGATE_FUNCTIONS = ("count", "avg", "min", "max", "sum")

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
# handled by the caller, which needs them to build the pagination links
PAGINATION_ARGUMENTS = ("page", "page_size")
RESERVED_ARGUMENTS = (*PAGINATION_ARGUMENTS, "columns")
MAX_GROUP_DEPTH = 50

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
    # the types come from DESCRIBE, which renders CHAR, TEXT, STRING and VARCHAR(n) as VARCHAR
    return duckdb_type.upper() == "VARCHAR"


def collated(quoted_column: str) -> str:
    return f"{quoted_column} COLLATE {TEXT_COLLATION}"


def column_type(name: str, columns: Mapping[str, str]) -> str:
    """The DuckDB type of a column, which must be in the parquet schema."""
    if name not in columns:
        raise ValueError(f"unknown column '{name}'")
    return columns[name]


def compares_as_varchar(duckdb_type: str) -> bool:
    """Nested values (lists, structs, JSON) and types without a Python counterpart are compared
    through their text rendering."""
    normalized = duckdb_type.upper()
    kind = python_type(duckdb_type)
    if kind == "json":
        return True
    return not (
        is_text(duckdb_type)
        or kind in ("int", "float")
        or normalized in ("UUID", "BOOLEAN", "DATE", "TIME")
        or normalized.startswith(("DECIMAL", "TIMESTAMP"))
    )


def coerce_value(value: str, duckdb_type: str) -> Any:
    """Convert a query string value to a bindable Python value.

    A value that does not fit the column type is rejected, like PostgreSQL did, rather than
    compared as a string, which would silently return wrong rows ('9' >= '12.5').
    """
    if compares_as_varchar(duckdb_type) or is_text(duckdb_type):
        return value
    normalized = duckdb_type.upper()
    kind = python_type(duckdb_type)
    try:
        if normalized == "UUID":
            UUID(value)
            return value
        if normalized == "BOOLEAN":
            lowered = value.strip().lower()
            if lowered in TRUE_VALUES:
                return True
            if lowered in FALSE_VALUES:
                return False
            raise ValueError(value)
        if normalized.startswith("DECIMAL"):
            return Decimal(value)
        if kind == "float":
            return float(value)
        if kind == "int":
            return int(value)
        # datetime.fromisoformat rather than date.fromisoformat, which ignores the two characters
        # after a compact date ('20240101-1' is read as 2024-01-01); it also reads plain dates
        if normalized == "DATE":
            parsed = datetime.fromisoformat(value)
            # a time would be silently dropped
            if parsed.time() != time() or parsed.tzinfo is not None:
                raise ValueError(value)
            return parsed.date()
        if normalized.startswith("TIMESTAMP"):
            return datetime.fromisoformat(value)
        # TIME, the last type compares_as_varchar leaves out
        return time.fromisoformat(value)
    except (ValueError, InvalidOperation) as exc:
        raise ValueError(f"invalid value '{value}' for a column of type {duckdb_type}") from exc


def _comparable(quoted_column: str, duckdb_type: str) -> str:
    return (
        f"CAST({quoted_column} AS VARCHAR)" if compares_as_varchar(duckdb_type) else quoted_column
    )


def _split_list(value: str) -> list[str]:
    """`a,"b,c",d` -> ["a", "b,c", "d"]"""
    items = []
    for item in split_top_level(value):
        item = item.strip()
        if len(item) >= 2 and item.startswith('"') and item.endswith('"'):
            item = item[1:-1]
        items.append(item)
    return items


@dataclass(frozen=True)
class Statement:
    """SQL and the values of its `?` placeholders."""

    sql: str
    params: list[Any]


def build_condition(argument: str, value: str, columns: Mapping[str, str]) -> Statement:
    """Translate a single `<column>__<operator>[=<value>]` filter into SQL."""
    if argument in RESERVED_ARGUMENTS:
        raise ValueError(f"Argument `{argument}` can't be set within an operator")
    column_name, comparator = split_column_operator(argument)
    if comparator == "sort":
        raise ValueError(f"Argument `{argument}` can't be set within an operator")

    duckdb_type = column_type(column_name, columns)
    quoted = quote_ident(column_name)

    if comparator == "isnull":
        return Statement(f"({quoted} IS NULL)", [])
    if comparator == "isnotnull":
        return Statement(f"({quoted} IS NOT NULL)", [])
    if comparator in ("contains", "notcontains"):
        condition = f"(CAST({quoted} AS VARCHAR) ILIKE '%' || ? || '%')"
        # `*` is the wildcard of PostgREST's ilike patterns
        pattern = value.replace("*", "%")
        return Statement(condition if comparator == "contains" else f"(NOT {condition})", [pattern])
    if comparator in ("in", "notin"):
        items = _split_list(value)
        if not items:
            # PostgREST's `in.()`: no row is in an empty list, NULLs included
            return Statement("(FALSE)" if comparator == "in" else "(TRUE)", [])
        params = [coerce_value(item, duckdb_type) for item in items]
        placeholders = ", ".join("?" for _ in params)
        operator = "IN" if comparator == "in" else "NOT IN"
        compared = _comparable(quoted, duckdb_type)
        return Statement(f"({compared} {operator} ({placeholders}))", params)
    if comparator in COMPARATORS:
        coerced = coerce_value(value, duckdb_type)
        compared = _comparable(quoted, duckdb_type)
        # equality stays exact, as with the deterministic collations of PostgreSQL
        if comparator in RANGE_COMPARATORS and is_text(duckdb_type):
            compared = collated(compared)
        return Statement(f"({compared} {COMPARATORS[comparator]} ?)", [coerced])
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
    `col__op."val.ue"` and `"col.umn"__op."val.ue"`. The operator is validated by
    build_condition, like outside of the groups.
    """
    quoted_column = re.match(r'^"([^"]*)"__', param)
    if quoted_column:
        column_prefix = f"{quoted_column.group(1)}__"
        operator_value = param[quoted_column.end() :]
    else:
        column_prefix, operator_value = "", param
    operator, dot, value = operator_value.partition(".")
    quoted_value = re.fullmatch(r'"([^"]*)"', value)
    # an unquoted value cannot hold a dot, which separates the terms
    if not dot or '"' in operator or not (quoted_value or not re.search(r'[".]', value)):
        raise ValueError(f"argument '{param}' could not be parsed")
    return column_prefix + operator, quoted_value.group(1) if quoted_value else value


def parse_operator(
    query: str,
    operator: str,
    columns: Mapping[str, str],
    top_level: bool = False,
    depth: int = 1,
) -> Statement:
    """Translate `or=(a__exact.1,and(b__less.2,c__isnotnull))` into a parenthesized SQL group."""
    # each level is parsed on the event loop and rescans the group: without a bound, a 5 KB
    # query blocks the process, then overflows the recursion
    if depth > MAX_GROUP_DEPTH:
        raise ValueError(f"groups cannot be nested more than {MAX_GROUP_DEPTH} levels deep")
    match = re.findall(rf"^{operator}{'=' if top_level else ''}\((.*)\)$", query)
    if not match:
        raise ValueError(f"argument '{query}' could not be parsed")
    conditions: list[Statement] = []
    for param in split_top_level(match[0]):
        if param.startswith(("and(", "or(")):
            conditions.append(parse_operator(param, param.split("(")[0], columns, depth=depth + 1))
        elif param.endswith(("__isnull", "__isnotnull")):
            conditions.append(build_condition(unquote_plus(param).replace('"', ""), "", columns))
        else:
            argument, value = find_arg_val(param)
            conditions.append(build_condition(unquote_plus(argument), unquote_plus(value), columns))
    if not conditions:
        raise ValueError(f"argument '{query}' could not be parsed")
    joiner = " OR " if operator == "or" else " AND "
    return Statement(
        f"({joiner.join(condition.sql for condition in conditions)})",
        [param for condition in conditions for param in condition.params],
    )


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

    def add_condition(self, condition: Statement) -> None:
        self.conditions.append(condition.sql)
        self.params.extend(condition.params)

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

    def data_sql(self, source: str) -> Statement:
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
        return Statement(sql, list(self.params))

    def count_sql(self, source: str) -> Statement:
        if self.is_aggregated:
            return Statement(
                f"SELECT count(*) FROM (SELECT {self.select_clause()} {self.from_clause(source)})",
                list(self.params),
            )
        return Statement(f"SELECT count(*) FROM {source}{self.where_clause()}", list(self.params))


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
            parsed.add_condition(
                parse_operator(f"or={decode_structure(raw_value)}", "or", columns, top_level=True)
            )
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
                duckdb_type = column_type(column_name, columns)
                parsed.aggregators.setdefault(comparator, []).append(column_name)
                if is_text(duckdb_type):
                    parsed.text_columns.add(column_name)
                continue
            if comparator not in ("isnull", "isnotnull"):
                raise ValueError(f"argument '{key}' could not be parsed")
            parsed.add_condition(build_condition(key, "", columns))
            continue
        if key in PAGINATION_ARGUMENTS:
            continue
        if key == "columns":
            if '"' in value:
                raise ValueError('Forbidden character `"` in a column name')
            names = [column for column in value.split(",") if column]
            for name in names:
                column_type(name, columns)
            parsed.selected = [quote_ident(name) for name in names]
            if not parsed.selected:
                raise ValueError(f"argument '{raw_arg}' could not be parsed")
            continue
        if "__" not in key:
            raise ValueError(f"argument '{key}={value}' could not be parsed")
        column_name, comparator = split_column_operator(key)
        if comparator == "sort":
            quoted = quote_ident(column_name)
            sorted_by = collated(quoted) if is_text(column_type(column_name, columns)) else quoted
            try:
                parsed.order_by.append(f"{sorted_by} {parse_sort(value)}")
            except ValueError:
                raise ValueError(f"argument '{key}={value}' could not be parsed") from None
            continue
        parsed.add_condition(build_condition(key, value, columns))
    if parsed.aggregators and parsed.selected is not None:
        raise ValueError("the argument `columns` cannot be set alongside aggregators")
    return parsed
