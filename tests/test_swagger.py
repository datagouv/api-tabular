import pytest

from api_tabular.query import PYTHON_TYPES, python_type
from api_tabular.resources import ColumnProfile
from api_tabular.swagger import TYPE_POSSIBILITIES, swagger_parameters

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


# the python_type values of csv-detective, served as is from the inspection hydra embeds
CSV_DETECTIVE_TYPES = ["string", "int", "float", "bool", "date", "datetime", "json", "binary"]


@pytest.mark.parametrize("python_type", CSV_DETECTIVE_TYPES)
def test_every_csv_detective_type_has_swagger_operators(python_type):
    assert swagger_parameters({"col": ColumnProfile(python_type=python_type, format=python_type)})


@pytest.mark.parametrize(
    "python_type, operators, aggregators",
    [
        ("string", STRING_LIKE, GROUPING),
        # compared through its text rendering, like a BLOB column without the inspection
        ("binary", STRING_LIKE, GROUPING),
        ("date", STRING_LIKE, GROUPING),
        ("datetime", STRING_LIKE, GROUPING),
        ("int", NUMERIC, NUMERIC_AGGREGATES),
        ("float", NUMERIC, NUMERIC_AGGREGATES),
        ("bool", {"isnull", "isnotnull", "differs", "exact", "sort"}, GROUPING),
        ("json", NULL_CHECKS, set()),
    ],
)
def test_swagger_parameters_per_type(python_type, operators, aggregators):
    parameters = swagger_parameters(
        {"col": ColumnProfile(python_type=python_type, format=python_type)}
    )
    assert {p["name"] for p in parameters} == GLOBAL_PARAMETERS | {
        f"col__{op}" for op in operators | aggregators
    }
    # is(not)null and aggregators do not take a value
    assert {p["name"] for p in parameters if p.get("allowEmptyValue")} == {
        f"col__{op}" for op in NULL_CHECKS | aggregators
    }
