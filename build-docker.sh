#!/usr/bin/env bash
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
revision=$(git -C "$repo_dir" rev-parse HEAD)

exec docker build \
    --build-arg "BUILD_REVISION=$revision" \
    "$@" \
    "$repo_dir"
