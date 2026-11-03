"""Read-only SQL endpoint: user-provided queries on the resource, exposed as the `resource` table.

The query is parsed by sqlglot, and only runs when every node of its tree is of an allowed kind
and it reads no other table than the resource and its own CTEs. What runs is the SQL regenerated
from that tree, never the client's text: a construct that DuckDB would read differently from
sqlglot cannot slip through. The allowed kinds serve basic processing of the data (aggregates for
a chart, rounding, dates), without `repeat`, `string_agg`, `list`, `||`, `range` nor recursive
CTEs, which would let a short query build a value or a number of rows of any size. The answer is
capped at SQL_MAX_ROWS rows, and the query interrupted after SQL_TIMEOUT seconds.
"""

import asyncio
from dataclasses import dataclass

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, SqlglotError

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine, fetch
from tabular_duckdb.errors import QueryException, safe_message
from tabular_duckdb.resources import Resource, is_storage_failure, read_error
from tabular_duckdb.serialization import rows_to_json

# the resource is always exposed under this table name
RESOURCE_TABLE = "resource"

ALLOWED_NODES = (
    # structure of a query
    exp.Select,
    exp.From,
    exp.Join,
    exp.Where,
    exp.Group,
    exp.Having,
    exp.Order,
    exp.Ordered,
    exp.Limit,
    exp.Offset,
    exp.Distinct,
    exp.Subquery,
    exp.With,
    exp.CTE,
    exp.Table,
    exp.TableAlias,
    exp.Alias,
    # values
    exp.Column,
    exp.Identifier,
    exp.Star,
    exp.Literal,
    exp.Null,
    exp.Boolean,
    exp.Paren,
    # the unit of date_trunc and EXTRACT
    exp.Var,
    exp.Cast,
    exp.DataType,
    # logic and comparisons
    exp.And,
    exp.Or,
    exp.Not,
    exp.EQ,
    exp.NEQ,
    exp.LT,
    exp.LTE,
    exp.GT,
    exp.GTE,
    exp.In,
    exp.Between,
    exp.Is,
    exp.Like,
    exp.ILike,
    exp.Case,
    exp.If,
    exp.Coalesce,
    exp.Nullif,
    # aggregates
    exp.Count,
    exp.Sum,
    exp.Avg,
    exp.Min,
    exp.Max,
    exp.Median,
    # arithmetic
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.IntDiv,
    exp.Mod,
    exp.Neg,
    exp.Round,
    exp.Abs,
    exp.Floor,
    exp.Ceil,
    # text
    exp.Lower,
    exp.Upper,
    exp.Length,
    # dates
    exp.Year,
    exp.Month,
    exp.Day,
    exp.TimestampTrunc,
    exp.DateTrunc,
    exp.Extract,
    # strftime
    exp.TimeToStr,
)

# functions sqlglot does not model, which it parses as anonymous calls
ALLOWED_ANONYMOUS_FUNCTIONS = frozenset({"date_part"})


def invalid_query(detail: str) -> QueryException:
    return QueryException(400, "Invalid SQL query", detail)


def is_forbidden(node: exp.Expr) -> bool:
    """Whether the node is refused, its children then left unvisited."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower() not in ALLOWED_ANONYMOUS_FUNCTIONS
    return not isinstance(node, ALLOWED_NODES)


def parse(user_sql: str) -> exp.Query:
    try:
        # a trailing `;` gives an empty statement
        statements = [stmt for stmt in sqlglot.parse(user_sql, dialect="duckdb") if stmt]
    except ParseError as exc:
        error = exc.errors[0]
        raise invalid_query(
            f"Could not parse the query: {error['description']} "
            f"(line {error['line']}, column {error['col']})"
        ) from exc
    except (SqlglotError, RecursionError) as exc:
        raise invalid_query(f"Could not parse the query: {exc}") from exc
    # a UNION is a query too, refused by ALLOWED_NODES with the part at fault
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise invalid_query("Only a single SELECT query is allowed")
    return statements[0]


def check_tables(tree: exp.Query) -> None:
    """Only the resource and the CTEs of the query can be read: an unqualified name could
    otherwise resolve to a catalog view of DuckDB (`pg_settings`...).

    A recursive CTE, which would fabricate rows, needs a UNION, which ALLOWED_NODES refuses."""
    ctes = tree.ctes
    if len(list(tree.find_all(exp.CTE))) != len(ctes):
        raise invalid_query("CTEs are only allowed at the start of the query")
    names = [cte.alias_or_name.lower() for cte in ctes]
    for table in tree.find_all(exp.Table):
        cte = table.find_ancestor(exp.CTE)
        # a CTE only sees the ones before it, the main query sees them all
        visible = names[: next(i for i, other in enumerate(ctes) if other is cte)] if cte else names
        if (
            table.db
            or table.catalog
            or not isinstance(table.this, exp.Identifier)
            or table.name.lower() not in (RESOURCE_TABLE, *visible)
        ):
            raise invalid_query(
                f"Unknown table `{table.sql(dialect='duckdb')}`: "
                f"the resource is the `{RESOURCE_TABLE}` table"
            )


def compile_query(user_sql: str, limit: int) -> str:
    """The SQL to run for the query of the client, or an `invalid_query` saying why it is refused."""
    tree = parse(user_sql)
    try:
        forbidden = [node for node in tree.walk(prune=is_forbidden) if is_forbidden(node)]
    except RecursionError as exc:
        raise invalid_query("The query is nested too deeply") from exc
    if forbidden:
        # the outermost refused nodes, as SQL, which tells a client (or an LLM) what to rewrite
        snippets = dict.fromkeys(
            node.sql(dialect="duckdb", comments=False)[:100] for node in forbidden
        )
        raise invalid_query(f"Not allowed: {', '.join(snippets)}")
    check_tables(tree)
    wrapped = sqlglot.select("*").from_(tree.subquery()).limit(limit)
    return wrapped.sql(dialect="duckdb", comments=False)


@dataclass(frozen=True)
class SqlResult:
    # JSON-serializable rows
    rows: list[dict]
    truncated: bool
    execution_ms: int


async def run_user_sql(resource: Resource, user_sql: str) -> SqlResult:
    if not user_sql.strip():
        raise invalid_query("The `q` parameter is required and cannot be empty")
    if len(user_sql) > settings.SQL_MAX_LENGTH:
        raise invalid_query(f"The query exceeds {settings.SQL_MAX_LENGTH} characters")
    row_limit = settings.SQL_MAX_ROWS
    query = compile_query(user_sql, row_limit + 1)
    view_sql = f"CREATE TEMP VIEW {RESOURCE_TABLE} AS SELECT * FROM {resource.source_sql}"

    with engine.cursor() as cursor:

        def view_then_query() -> list[dict]:
            try:
                cursor.execute(view_sql)
            except duckdb.InterruptException:
                raise
            except duckdb.Error as exc:
                # the view reads the parquet footer: the file itself is what fails here
                raise read_error(exc, resource.resource_id) from exc
            result = fetch(cursor, query)
            return rows_to_json(result.columns, result.rows)

        started = asyncio.get_running_loop().time()
        try:
            rows = await engine.run(cursor, view_then_query, settings.SQL_TIMEOUT)
        except duckdb.Error as exc:
            # the storage failing while the query reads the file is not the client's error
            if is_storage_failure(exc):
                raise read_error(exc, resource.resource_id) from exc
            raise invalid_query(safe_message(str(exc))) from exc
        execution_ms = round((asyncio.get_running_loop().time() - started) * 1000)

    return SqlResult(
        rows=rows[:row_limit],
        truncated=len(rows) > row_limit,
        execution_ms=execution_ms,
    )
