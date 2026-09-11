#!/usr/bin/env sh
set -eu
yapf --in-place --recursive trpc_service tests
