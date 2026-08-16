#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  make dev-down
}

trap cleanup EXIT

make dev-up
./harness/scripts/gateway-smoke.sh
make dev-reset
./harness/scripts/reconciler-smoke.sh
