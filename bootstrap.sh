#!/usr/bin/env sh
set -eu

usage() {
    printf '%s\n' "Usage: sh bootstrap.sh [--system-certs] [--certificate FILE] [--refresh]"
}

use_system_certs=false
certificate_path=""
refresh=false

while [ "$#" -gt 0 ]; do
    case "$1" in
        --system-certs)
            use_system_certs=true
            ;;
        --certificate)
            shift
            if [ "$#" -eq 0 ]; then
                printf '%s\n' "--certificate requires a PEM file path" >&2
                exit 2
            fi
            certificate_path=$1
            ;;
        --refresh)
            refresh=true
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf 'Unknown option: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

if [ "$use_system_certs" = true ] && [ -n "$certificate_path" ]; then
    printf '%s\n' "Use either --system-certs or --certificate, not both" >&2
    exit 2
fi

if ! command -v uv >/dev/null 2>&1; then
    printf '%s\n' "uv is not installed. See docs/environment-setup.md" >&2
    exit 1
fi

case $0 in
    */*) script_dir=${0%/*} ;;
    *) script_dir=. ;;
esac
project_dir=$(CDPATH= cd -- "$script_dir" && pwd)
cd "$project_dir"

UV_CACHE_DIR=${UV_CACHE_DIR:-"$project_dir/.uv-cache"}
UV_LINK_MODE=${UV_LINK_MODE:-copy}
export UV_CACHE_DIR UV_LINK_MODE

if [ -n "$certificate_path" ]; then
    if [ ! -f "$certificate_path" ]; then
        printf 'Certificate file does not exist: %s\n' "$certificate_path" >&2
        exit 1
    fi
    SSL_CERT_FILE=$certificate_path
    export SSL_CERT_FILE
fi

set -- sync --extra dev --locked
if [ "$refresh" = true ]; then
    set -- "$@" --refresh
fi
if [ "$use_system_certs" = true ]; then
    set -- --system-certs "$@"
fi

uv "$@"
uv run --no-sync python -c "import trpc_agent_sdk, trpc_service; print('tRPC-Agent SDK OK:', trpc_agent_sdk.__file__); print('service OK:', trpc_service.__file__)"
