# syntax=docker/dockerfile:1
ARG PYTHON_IMAGE=python:3.13.15-slim-bookworm

FROM ${PYTHON_IMAGE} AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DWE_API_HOST=0.0.0.0 \
    DWE_API_PORT=8000
WORKDIR /app
RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY --from=build /app/.venv /app/.venv
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 \
    CMD ["python", "-c", "import json, os, urllib.request; r = urllib.request.build_opener(urllib.request.ProxyHandler({})).open('http://127.0.0.1:' + os.environ.get('DWE_API_PORT', '8000') + '/health/live', timeout=2); assert r.status == 200 and json.load(r) == {'status': 'ok'}"]
ENTRYPOINT ["engine"]
CMD ["api"]
