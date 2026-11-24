# syntax=docker/dockerfile:1
#
# One image for the application. The `test` target adds the test dependencies
# and the test suite on top of the same base; it is only built by
# `docker compose --profile test`.

FROM python:3.14-slim AS base
ENV UV_PYTHON=python3.14 \
    UV_LINK_MODE=copy \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /uvx /bin/
WORKDIR /app
# uv installs into /app/.venv; its binaries must be what CMD and `compose run` execute.
ENV PATH="/app/.venv/bin:${PATH}"
COPY pyproject.toml uv.lock ./

# Application with its runtime dependencies, without development packages.
FROM base AS runtime
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 mtl
USER mtl

EXPOSE 8000
# curl only: a healthcheck runs often and must not start a Python interpreter.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["curl", "-fsS", "-o", "/dev/null", "http://127.0.0.1:8000/health"]
CMD ["uvicorn", "app.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

# Test image: runtime and development dependencies, plus the test suite.
FROM base AS test
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-install-project
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
COPY tests ./tests
CMD ["pytest"]
