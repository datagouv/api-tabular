from pydantic import PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration, overridable through environment variables or a .env file."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # where the parquet files live: a resource is read at <PARQUET_BASE_URL>/<resource_id>.parquet
    PARQUET_BASE_URL: str = "https://hydra.s3.rbx.io.cloud.ovh.net/parquet"
    HTTP_TIMEOUT: int = 20

    # only needed for a private bucket: when set, a DuckDB S3 secret is created and the
    # base URL can be an s3:// one
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_ENDPOINT: str = ""
    S3_REGION: str = "us-east-1"
    S3_URL_STYLE: str = "vhost"
    S3_USE_SSL: bool = True

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
    # caches the parquet footers and HTTP metadata: much faster on repeated queries, but a
    # replaced parquet file keeps being served from the cached metadata until restart
    DUCKDB_METADATA_CACHE: bool = False
    # every query of the data endpoints is interrupted after QUERY_TIMEOUT seconds, and at most
    # QUERY_WORKERS of them run at once
    QUERY_TIMEOUT: int = 30
    QUERY_WORKERS: PositiveInt = 8
    RESOURCE_CACHE_TTL: int = 300
    PROFILE_STATS: bool = True

    # read-only SQL endpoint, whose queries share the QUERY_WORKERS threads
    SQL_MAX_ROWS: PositiveInt = 10000
    # of JSON in the answer
    SQL_MAX_BYTES: PositiveInt = 10_000_000
    SQL_TIMEOUT: int = 30
    SQL_MAX_LENGTH: int = 10000

    SENTRY_DSN: str = ""
    SENTRY_SAMPLE_RATE: float = 1.0

    @property
    def storage_over_http(self) -> bool:
        """An s3:// base URL is only reachable through DuckDB, not with a plain HTTP request."""
        return self.PARQUET_BASE_URL.startswith(("http://", "https://"))

    def object_url(self, resource_id: str) -> str:
        return f"{self.PARQUET_BASE_URL.rstrip('/')}/{resource_id}.parquet"


settings = Settings()
