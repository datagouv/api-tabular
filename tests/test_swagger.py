import pytest

from tabular_duckdb.swagger import swagger_parameters

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


@pytest.mark.parametrize(
    "python_type, operators",
    [
        ("string", STRING_LIKE),
        ("date", STRING_LIKE),
        ("datetime", STRING_LIKE),
        ("int", {"isnull", "isnotnull", "differs", "exact", "in", "notin", "sort"} | COMPARE),
        ("float", {"isnull", "isnotnull", "differs", "exact", "in", "notin", "sort"} | COMPARE),
        ("bool", {"isnull", "isnotnull", "differs", "exact", "sort"}),
        ("json", {"isnull", "isnotnull"}),
    ],
)
def test_swagger_parameters_per_type(python_type, operators):
    parameters = swagger_parameters({"col": {"python_type": python_type}})
    assert {p["name"] for p in parameters} == GLOBAL_PARAMETERS | {f"col__{op}" for op in operators}
    # is(not)null do not take a value
    assert {p["name"] for p in parameters if p.get("allowEmptyValue")} == {
        "col__isnull",
        "col__isnotnull",
    }
