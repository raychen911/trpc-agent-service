# Steps 1-4 development guide

The first four implementation steps provide the tested service foundation. The current repository
has since completed the real model, IM, multi-backend, distributed Worker, and Kubernetes layers;
see the root README for the complete runtime.

## Create the portable project environment

```sh
cd /path/to/trpc-agent-service
sh bootstrap.sh
```

`pyproject.toml` installs the compatible published `trpc-agent-py` package from PyPI; no sibling
SDK repository or named Conda environment is required. The script creates or updates `.venv` with
`uv` and works on Linux/macOS and Windows Git Bash or WSL.

## Run the checks

```sh
sh test.sh
uv run --frozen python -m trpc_service._cli init-db
uv run --frozen python -m trpc_service._cli show-config
```

## Start the service

The default environment is `test`, so the foundation can start before external credentials are
available:

```sh
sh start.sh
```

Then open:

- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/ready`
- `http://127.0.0.1:8000/docs`

For development, copy `.env.example` to `.env` and supply the referenced secret environment
variables. Development and production reject test/mock model providers and missing secret
references. Real model execution is now implemented in the current codebase.

## Implemented in these steps

- Python packaging with the published `trpc-agent-py` dependency.
- FastAPI application lifecycle, liveness and database readiness.
- `test`, `development` and `production` settings with fail-closed validation.
- Twelve tenant-scoped SQLite/PostgreSQL tables, including Knowledge, Artifact and Execution Outbox.
- Repositories for tenants, apps, bindings, inbound idempotency, sessions, events, Memory, Summary,
  Knowledge, Artifact, Execution Outbox and Audit Log.
- SQL unique constraints and atomic optimistic session updates.
