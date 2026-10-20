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

    # public URLs built in the responses
    SERVER_NAME: str = "localhost:8005"
    SCHEME: str = "http"
    DOC_PATH: str = "/api/doc"

    # pagination and streaming
    PAGE_SIZE_DEFAULT: int = 20
    PAGE_SIZE_MAX: int = 50
    BATCH_SIZE: int = 50000
    STREAM_CHUNK_SIZE: int = 1000

    # DuckDB
    DUCKDB_THREADS: int = 4
    DUCKDB_MEMORY_LIMIT: str = "2GB"
    # caches the parquet footers and HTTP metadata: much faster on repeated queries, but a
    # replaced parquet file keeps being served from the cached metadata until restart
    DUCKDB_METADATA_CACHE: bool = False
    # every query of the data endpoints is interrupted after QUERY_TIMEOUT seconds, and at most
    # QUERY_WORKERS of them run at once
    QUERY_TIMEOUT: int = 30
    QUERY_WORKERS: int = 8
    RESOURCE_CACHE_TTL: int = 300
    PROFILE_STATS: bool = True

    # read-only SQL endpoint
    SQL_POOL_SIZE: int = 4
    SQL_MAX_ROWS: int = 10000
    SQL_TIMEOUT: int = 30
    SQL_MAX_LENGTH: int = 10000

    SENTRY_DSN: str = ""
    SENTRY_SAMPLE_RATE: float = 1.0

    @property
    def has_s3_credentials(self) -> bool:
        return bool(self.S3_ACCESS_KEY and self.S3_SECRET_KEY)

    def object_url(self, resource_id: str) -> str:
        return f"{self.PARQUET_BASE_URL.rstrip('/')}/{resource_id}.parquet"


settings = Settings()
