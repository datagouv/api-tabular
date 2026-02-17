FROM astral/uv:python3.11-trixie-slim

# install needed apt packages
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# create user & group
RUN groupadd --system datagouv && \
    useradd --system --gid datagouv --create-home datagouv

# install
WORKDIR /home/datagouv
ADD . /home/datagouv/
RUN uv sync --frozen
RUN chown -R datagouv:datagouv /home/datagouv

# run
USER datagouv
# Use `python -m gunicorn` instead of `gunicorn` due to uv issue #15246: https://github.com/astral-sh/uv/issues/15246
ENTRYPOINT ["uv", "run", "python", "-m", "gunicorn"]
# Gunicorn config: 2 workers for ~1 vCPU allocation, aiohttp.GunicornWebWorker for async support, default timeouts suitable for async workers
CMD ["api_tabular.tabular.app:app_factory", "--bind", "0.0.0.0:8005", "--worker-class", "aiohttp.GunicornWebWorker", "--workers", "2", "--access-logfile", "-"]
