import pytest

from tabular_duckdb.query import PYTHON_TYPES, python_type
from tabular_duckdb.swagger import TYPE_POSSIBILITIES, swagger_parameters

GLOBAL_PARAMETERS = {"page", "page_size", "columns", "or"}
COMPARE = {"less", "greater", "strictly_less", "strictly_greater"}
STRING_LIKE = {
    "isnull",
    "isnotnull",
    "contains",
    "notcontains",
    "differs",
    "exact",
    "in",
    "notin",
    "sort",
} | COMPARE
GROUPING = {"groupby", "count"}
NUMERIC_AGGREGATES = GROUPING | {"avg", "min", "max", "sum"}
NUMERIC = {"isnull", "isnotnull", "differs", "exact", "in", "notin", "sort"} | COMPARE
NULL_CHECKS = {"isnull", "isnotnull"}


@pytest.mark.parametrize(
    "duckdb_type",
    [
        *PYTHON_TYPES,
        "DECIMAL(10,2)",
        "INTEGER[]",
        "STRUCT(a INTEGER)",
        "MAP(VARCHAR, INTEGER)",
        "UNION(a INTEGER)",
        "GEOMETRY",
    ],
)
def test_every_column_type_has_swagger_operators(duckdb_type):
    """A type without operators would make /swagger/ fail for every resource holding one."""
    assert python_type(duckdb_type) in TYPE_POSSIBILITIES


@pytest.mark.parametrize(
    "python_type, operators, aggregators",
    [
        ("string", STRING_LIKE, GROUPING),
        ("date", STRING_LIKE, GROUPING),
        ("datetime", STRING_LIKE, GROUPING),
        ("int", NUMERIC, NUMERIC_AGGREGATES),
        ("float", NUMERIC, NUMERIC_AGGREGATES),
        ("bool", {"isnull", "isnotnull", "differs", "exact", "sort"}, GROUPING),
        ("json", NULL_CHECKS, set()),
    ],
)
def test_swagger_parameters_per_type(python_type, operators, aggregators):
    parameters = swagger_parameters({"col": {"python_type": python_type}})
    assert {p["name"] for p in parameters} == GLOBAL_PARAMETERS | {
        f"col__{op}" for op in operators | aggregators
    }
    # is(not)null and aggregators do not take a value
    assert {p["name"] for p in parameters if p.get("allowEmptyValue")} == {
        f"col__{op}" for op in NULL_CHECKS | aggregators
    }
