import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def restore(backup: Path, destination: Path, manifest: Path, force: bool) -> None:
    if not force:
        raise RuntimeError("restore requires --force after stopping all writers")
    expected = json.loads(manifest.read_text(encoding="utf-8"))["sha256"]
    actual = hashlib.sha256(backup.read_bytes()).hexdigest()
    if actual != expected:
        raise OSError("backup checksum does not match manifest")
    if destination.exists():
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(destination, destination.with_suffix(f".pre-restore-{timestamp}.db"))
    temporary = destination.with_suffix(destination.suffix + ".restore.tmp")
    shutil.copy2(backup, temporary)
    temporary.replace(destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("backup", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    restore(args.backup, args.destination, args.manifest, args.force)
