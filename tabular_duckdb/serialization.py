import json
import math
import uuid
from datetime import date, datetime, time, timedelta
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
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    return str(value)


def rows_to_json(columns: list[str], rows: list[tuple]) -> list[dict]:
    return [{column: jsonable(value) for column, value in zip(columns, row)} for row in rows]


def as_csv_value(value: Any) -> str:
    """Render a DuckDB value for a CSV cell."""
    if value is None:
        return ""
    converted = jsonable(value)
    if isinstance(converted, bool):
        return "true" if converted else "false"
    if isinstance(converted, (list, dict)):
        return json.dumps(converted, ensure_ascii=False)
    return str(converted)
