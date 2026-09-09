FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY trpc_service ./trpc_service
RUN pip install --no-cache-dir ".[postgres,wecom,telemetry]"

COPY examples/config/tenants.yaml ./examples/config/tenants.yaml
EXPOSE 8080
CMD ["trpc_service", "serve", "--config", "examples/config/tenants.yaml", "--host", "0.0.0.0", "--port", "8080"]
