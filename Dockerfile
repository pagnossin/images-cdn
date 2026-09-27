# syntax=docker/dockerfile:1
FROM nats:2-alpine AS nats

FROM python:3.13-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /srv
COPY requirements.txt .
RUN pip install -r requirements.txt

FROM base AS test
COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt
COPY . .
CMD ["pytest", "-v"]

# roles: node / gateway (docker compose) or all-in-one (single container, online demo)
FROM base AS runtime
COPY --from=nats /usr/local/bin/nats-server /usr/local/bin/nats-server
COPY cdn ./cdn
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh \
    && useradd --create-home --uid 10001 app \
    && mkdir -p /data && chown app /data
USER app
EXPOSE 8000 8080
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["all-in-one"]
