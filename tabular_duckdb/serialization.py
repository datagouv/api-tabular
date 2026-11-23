import json
import math
from collections.abc import Collection
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


def jsonable(value: Any) -> Any:
    """Convert DuckDB values into JSON-serializable ones."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        # NaN and infinities are not valid JSON
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        # escaped rather than replaced: two different invalid values (or map keys) stay apart
        return value.decode("utf-8", errors="backslashreplace")
    if isinstance(value, dict):
        return {str(jsonable(key)): jsonable(item) for key, item in value.items()}
    # lists come as lists, fixed-size arrays as tuples
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    # UUID, timedelta…
    return str(value)


def json_value(value: Any) -> Any:
    """A value of a column typed json by csv-detective, which hydra stores as text."""
    if not isinstance(value, str):
        return jsonable(value)
    try:
        return json.loads(value)
    except ValueError:
        # csv-detective types a column from the values it samples: one may not parse
        return value


def rows_to_json(
    columns: list[str], rows: list[tuple], json_columns: Collection[str]
) -> list[dict]:
    converters = [json_value if column in json_columns else jsonable for column in columns]
    return [
        {column: convert(value) for column, convert, value in zip(columns, converters, row)}
        for row in rows
    ]


def as_csv_value(value: Any) -> str:
    """Render a DuckDB value for a CSV cell."""
    if value is None:
        return ""
    if isinstance(value, float) and not math.isfinite(value):
        # the text form of PostgreSQL, served by the PostgREST version
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    converted = jsonable(value)
    if isinstance(converted, bool):
        return "true" if converted else "false"
    if isinstance(converted, (list, dict)):
        return json.dumps(converted, ensure_ascii=False)
    return str(converted)
