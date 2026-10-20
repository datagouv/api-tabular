FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

# install dependencies first, so that code changes don't invalidate the layer
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# the version comes from git tags, which are not in the build context: the CI passes it
ARG VERSION=0.0.0
COPY tabular_duckdb ./tabular_duckdb
RUN SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION} uv sync --frozen --no-dev

COPY tabular_swagger.yaml ./

# download the httpfs extension at build time: at runtime we only LOAD it
RUN uv run --no-sync python -c "from tabular_duckdb.engine import install_extensions; install_extensions()"

ENV PYTHONUNBUFFERED=1
EXPOSE 8005

CMD ["uv", "run", "--no-sync", "uvicorn", "tabular_duckdb.main:app", "--host", "0.0.0.0", "--port", "8005"]
