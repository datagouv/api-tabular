import tempfile
from pathlib import Path

from pydantic import PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration, overridable through environment variables or a .env file."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # where the parquet files live, read over HTTP(S) without credentials: a resource is read at
    # <PARQUET_BASE_URL>/<resource_id>.parquet
    PARQUET_BASE_URL: str = "https://hydra.s3.rbx.io.cloud.ovh.net/parquet"
    # of the HEAD that /health/ sends to the storage (DuckDB reads use QUERY_TIMEOUT_SECONDS)
    HEALTH_TIMEOUT_SECONDS: int = 20

    # the data.gouv.fr instance whose resources are served, for the permalink to their file
    DATAGOUV_URL: str = "https://www.data.gouv.fr"

    # public URLs built in the responses
    SERVER_NAME: str = "localhost:8005"
    SCHEME: str = "http"
    DOC_PATH: str = "/api/doc"

    # pagination and streaming
    PAGE_SIZE_DEFAULT: int = 20
    PAGE_SIZE_MAX: int = 50
    BATCH_SIZE: int = 50000
    STREAM_CHUNK_SIZE: int = 1000

    # DuckDB, for the single instance shared by every query
    DUCKDB_THREADS: int = 4
    DUCKDB_MEMORY_LIMIT: str = "2GB"
    # where queries larger than DUCKDB_MEMORY_LIMIT spill: DuckDB defaults to `.tmp` in the
    # working directory, which the user of the image cannot write to
    DUCKDB_TEMP_DIRECTORY: str = str(Path(tempfile.gettempdir()) / "duckdb")
    # caches the HTTP metadata of the files, which saves a HEAD per query, but a parquet file
    # replaced in the bucket fails to read until restart (the footers are cached either way)
    DUCKDB_ENABLE_HTTP_METADATA_CACHE: bool = False
    # every query is interrupted after QUERY_TIMEOUT_SECONDS, at most QUERY_WORKERS of them run
    # at once, and at most QUERY_WORKERS CSV/JSON exports are open at once
    QUERY_TIMEOUT_SECONDS: int = 30
    QUERY_WORKERS: PositiveInt = 8
    RESOURCE_CACHE_TTL_SECONDS: int = 300
    PROFILE_STATS: bool = True

    SENTRY_DSN: str = ""
    SENTRY_SAMPLE_RATE: float = 1.0

    def object_url(self, resource_id: str) -> str:
        return f"{self.PARQUET_BASE_URL.rstrip('/')}/{resource_id}.parquet"


settings = Settings()
