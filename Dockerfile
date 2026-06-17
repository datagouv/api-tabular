FROM astral/uv:python3.14-trixie-slim

# Which Gunicorn app to run
# Overridden at build time by the CI so we can use one Dockerfile for both Tabular API and Metrics API
ARG APP_MODULE=api_tabular.tabular.app:app_factory

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
RUN chown -R datagouv:datagouv /home/datagouv
USER datagouv
RUN uv sync --frozen

# run (ENV from ARG so shell can expand APP_MODULE at runtime)
ENV APP_MODULE=${APP_MODULE}
# Use `python -m gunicorn` instead of `gunicorn` due to uv issue #15246: https://github.com/astral-sh/uv/issues/15246
# Shell so APP_MODULE is expanded; bind to 8005 (map to different host ports when running both containers)
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["uv run python -m gunicorn $APP_MODULE --bind 0.0.0.0:8005 --worker-class aiohttp.GunicornWebWorker --workers 2 --access-logfile -"]
