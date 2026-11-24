FROM astral/uv:python3.14-trixie-slim

# the API reads remote files through DuckDB extensions: not as root
RUN groupadd --system datagouv && \
    useradd --system --gid datagouv --create-home datagouv

WORKDIR /app

# install dependencies first, so that code changes don't invalidate the layer
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# the version comes from git tags, which are not in the build context: the CI passes it
ARG VERSION=0.0.0
COPY api_tabular ./api_tabular
RUN SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION} uv sync --frozen --no-dev

COPY tabular_swagger.yaml ./

USER datagouv

# download the httpfs extension at build time, into the home of the user that will LOAD it
RUN uv run --no-sync python -c "import duckdb; duckdb.execute('INSTALL httpfs')"

ENV PYTHONUNBUFFERED=1
EXPOSE 8005

CMD ["uv", "run", "--no-sync", "uvicorn", "api_tabular.main:app", "--host", "0.0.0.0", "--port", "8005"]
