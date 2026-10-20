FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# install dependencies first, so that code changes don't invalidate the layer
COPY pyproject.toml README.md ./
COPY tabular_duckdb ./tabular_duckdb
RUN uv sync --no-dev

COPY tabular_swagger.yaml ./

# download the httpfs extension at build time: at runtime we only LOAD it
RUN uv run python -c "from tabular_duckdb.engine import install_extensions; install_extensions()"

ENV PYTHONUNBUFFERED=1
EXPOSE 8005

CMD ["uv", "run", "uvicorn", "tabular_duckdb.main:app", "--host", "0.0.0.0", "--port", "8005"]
