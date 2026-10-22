import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pytest
from fastapi.testclient import TestClient

from tabular_duckdb.config import settings
from tabular_duckdb.engine import DuckDBEngine
from tabular_duckdb.main import app

RESOURCE_ID = "aaaaaaaa-1111-bbbb-2222-cccccccccccc"
UNKNOWN_RESOURCE_ID = "00000000-0000-0000-0000-000000000000"
TOTAL_LINES = 60

# row i (1-based, which is also its __id) is fully determined by i, so that the expected
# results of a query can be written down in the tests
RESOURCE_SQL = f"""
SELECT
    'id-' || i AS id,
    (i % 10) / 10 AS score,
    (i % 7)::BIGINT AS decompte,
    i % 2 = 0 AS is_true,
    (DATE '1950-01-01' + to_years(i::INTEGER))::DATE AS birth,
    CASE WHEN i % 5 = 0 THEN NULL ELSE 'Élu ' || i END AS "Nom de l'élu",
    ['a', 'b'] AS liste
FROM range(1, {TOTAL_LINES + 1}) AS t(i)
"""
HEADER = ["id", "score", "decompte", "is_true", "birth", "Nom de l'élu", "liste"]

# a resource converted by hydra, which embeds the csv-detective inspection in the parquet
# key-value metadata; json columns are stored as strings, like hydra does
INSPECTED_RESOURCE_ID = "aaaaaaaa-5555-bbbb-6666-cccccccccccc"
INSPECTED_RESOURCE_SQL = """
SELECT * FROM (VALUES ('62', '[1, 2]', 3), ('59', NULL, 4)) AS t(code, liste, n)
"""
INSPECTION = {
    "encoding": "utf-8",
    "separator": ";",
    "header_row_idx": 0,
    "header": ["code", "liste", "n"],
    "columns": {
        "code": {"python_type": "string", "format": "code_departement", "score": 1.0},
        "liste": {"python_type": "json", "format": "json", "score": 1.0},
        "n": {"python_type": "int", "format": "int", "score": 1.0},
    },
    "formats": {"code_departement": ["code"], "json": ["liste"], "int": ["n"]},
    "categorical": ["code"],
    "total_lines": 2,
    "nb_duplicates": 0,
    "profile": {
        "code": {
            "tops": [{"value": "62", "count": 1}, {"value": "59", "count": 1}],
            "nb_distinct": 2,
            "nb_missing_values": 0,
        },
        "liste": {
            "tops": [{"value": "[1, 2]", "count": 1}],
            "nb_distinct": 1,
            "nb_missing_values": 1,
        },
        "n": {
            "min": 3,
            "max": 4,
            "mean": 3.5,
            "std": 0.7,
            "nb_distinct": 2,
            "nb_missing_values": 0,
        },
    },
}

# PostgreSQL float columns accept NaN and infinities, and hydra copies them to the parquet
FLOATS_RESOURCE_ID = "aaaaaaaa-7777-bbbb-8888-cccccccccccc"
FLOATS_RESOURCE_SQL = """
SELECT * FROM (
    VALUES ('nan'::DOUBLE), ('inf'::DOUBLE), ('-inf'::DOUBLE), (NULL), (1.5)
) AS t(value)
"""

# two NULLs among many rows, SUMMARIZE rounds their percentage down to 0; written in row groups
# of RARE_NULL_ROW_GROUP_SIZE rows so that the two NULLs land in different row groups
RARE_NULL_RESOURCE_ID = "aaaaaaaa-9999-bbbb-0000-cccccccccccc"
RARE_NULL_TOTAL_LINES = 30001
RARE_NULL_ROW_GROUP_SIZE = 10000
RARE_NULL_RESOURCE_SQL = f"""
SELECT CASE WHEN i IN (0, 25000) THEN NULL ELSE i END AS value
FROM range({RARE_NULL_TOTAL_LINES}) AS t(i)
"""

# not a parquet file, whose DuckDB error message holds the path of the file
CORRUPT_RESOURCE_ID = "aaaaaaaa-dead-bbbb-beef-cccccccccccc"

# files with a column of the name the API uses for the row id, or reads the row numbers from
ID_COLUMN_RESOURCE_ID = "aaaaaaaa-1d1d-bbbb-1d1d-cccccccccccc"
ID_COLUMN_RESOURCE_SQL = "SELECT 'own-' || i AS __id, i AS value FROM range(3) AS t(i)"
ROW_NUMBER_COLUMN_RESOURCE_ID = "aaaaaaaa-4040-bbbb-4040-cccccccccccc"
ROW_NUMBER_COLUMN_RESOURCE_SQL = "SELECT i AS File_Row_Number FROM range(3) AS t(i)"


@pytest.fixture(scope="session")
def parquet_dir(tmp_path_factory) -> str:
    directory = tmp_path_factory.mktemp("parquet")
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY ({RESOURCE_SQL}) TO '{directory}/{RESOURCE_ID}.parquet' (FORMAT parquet)"
        )
        inspection = json.dumps(INSPECTION).replace("'", "''")
        conn.execute(
            f"COPY ({INSPECTED_RESOURCE_SQL}) TO '{directory}/{INSPECTED_RESOURCE_ID}.parquet' "
            f"(FORMAT parquet, KV_METADATA {{csv_detective: '{inspection}'}})"
        )
        conn.execute(
            f"COPY ({FLOATS_RESOURCE_SQL}) TO '{directory}/{FLOATS_RESOURCE_ID}.parquet' "
            "(FORMAT parquet)"
        )
        conn.execute(
            f"COPY ({RARE_NULL_RESOURCE_SQL}) TO '{directory}/{RARE_NULL_RESOURCE_ID}.parquet' "
            f"(FORMAT parquet, ROW_GROUP_SIZE {RARE_NULL_ROW_GROUP_SIZE})"
        )
        conn.execute(
            f"COPY ({ID_COLUMN_RESOURCE_SQL}) TO '{directory}/{ID_COLUMN_RESOURCE_ID}.parquet'"
        )
        conn.execute(
            f"COPY ({ROW_NUMBER_COLUMN_RESOURCE_SQL}) "
            f"TO '{directory}/{ROW_NUMBER_COLUMN_RESOURCE_ID}.parquet'"
        )
    (directory / f"{CORRUPT_RESOURCE_ID}.parquet").write_bytes(b"not a parquet")
    return str(directory)


@pytest.fixture(scope="session")
def client(http_parquet_dir):
    """The API, run in process against the parquet files of `parquet_dir`, served over HTTP."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(settings, "PARQUET_BASE_URL", http_parquet_dir)
        patch.setattr(settings, "SERVER_NAME", "testserver")
        with TestClient(app) as test_client:
            yield test_client


@pytest.fixture
def base_url() -> str:
    return "http://testserver"


LAST_MODIFIED = "Mon, 01 Jan 2024 12:00:00 GMT"


class StorageHandler(BaseHTTPRequestHandler):
    """Answers `/<status>/...` with that status, and a Last-Modified date to a HEAD on 200.

    A GET gets the same status, like an object storage, which httpfs falls back to on a 403 HEAD,
    except under `/200/`, where it fails: an object that exists but whose reads fail.
    """

    def _status(self) -> int:
        return int(self.path.strip("/").split("/")[0])

    def do_HEAD(self) -> None:
        status = self._status()
        self.send_response(status)
        if status == 200:
            self.send_header("Last-Modified", LAST_MODIFIED)
        self.end_headers()

    def do_GET(self) -> None:
        status = self._status()
        self.send_response(503 if status == 200 else status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture(scope="session")
def storage():
    """Base URL of an HTTP storage answering the status written in the path."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), StorageHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


class ParquetFilesHandler(BaseHTTPRequestHandler):
    """Serves the parquet files of `directory` like an object storage: HEAD, and ranged GETs."""

    directory: Path

    def _file(self) -> Path | None:
        path = self.directory / self.path.strip("/")
        return path if path.is_file() else None

    def do_HEAD(self) -> None:
        path = self._file()
        if path is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(path.stat().st_size))
        self.send_header("Last-Modified", LAST_MODIFIED)
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self) -> None:
        path = self._file()
        if path is None:
            self.send_response(404)
            self.end_headers()
            return
        content = path.read_bytes()
        start, end = 0, len(content) - 1
        ranged = self.headers.get("Range", "").removeprefix("bytes=")
        if ranged:
            first, _, last = ranged.partition("-")
            start, end = int(first), min(int(last), end) if last else end
        self.send_response(206 if ranged else 200)
        self.send_header("Content-Length", str(end - start + 1))
        if ranged:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(content)}")
        self.end_headers()
        self.wfile.write(content[start : end + 1])

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture(scope="session")
def http_parquet_dir(parquet_dir):
    """Base URL serving `parquet_dir` over HTTP, the way the bucket serves the parquet files."""
    handler = type("Handler", (ParquetFilesHandler,), {"directory": Path(parquet_dir)})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


@pytest.fixture
def closed_port() -> int:
    """A local port where nothing listens: a request let through fails to connect."""
    with socket.create_server(("127.0.0.1", 0)) as server:
        return server.getsockname()[1]


@pytest.fixture
async def engine_on(monkeypatch):
    """Start an engine of its own on a storage: its sandbox only opens the prefix set at start.

    The settings patched before the call (QUERY_WORKERS, QUERY_TIMEOUT...) apply to it too."""
    engines: list[DuckDBEngine] = []

    async def start(base_url: str) -> DuckDBEngine:
        monkeypatch.setattr(settings, "PARQUET_BASE_URL", base_url)
        engine = DuckDBEngine()
        await engine.start()
        engines.append(engine)
        return engine

    yield start
    for engine in engines:
        await engine.stop()


@pytest.fixture
def silent_storage():
    """Base URL of a storage accepting connections but never answering."""
    with socket.create_server(("127.0.0.1", 0)) as server:
        yield f"http://127.0.0.1:{server.getsockname()[1]}"


@pytest.fixture
def duckdb_connection():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()
