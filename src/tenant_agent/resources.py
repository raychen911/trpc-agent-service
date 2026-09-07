"""Locate release resources in either a source checkout or an installed wheel."""

from pathlib import Path


def resource_root() -> Path:
    checkout = Path(__file__).resolve().parents[2]
    if (checkout / "alembic.ini").is_file():
        return checkout
    bundled = Path(__file__).resolve().parent / "_resources"
    if not (bundled / "alembic.ini").is_file():
        raise RuntimeError("installation is missing bundled migration resources")
    return bundled


def default_bootstrap_path() -> Path:
    local = Path("config/tenants.example.yaml")
    return local if local.is_file() else resource_root() / local
