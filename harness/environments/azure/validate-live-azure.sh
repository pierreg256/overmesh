#!/usr/bin/env bash
set -euo pipefail

./harness/environments/azure/validate-storage-authorization.sh
./harness/environments/azure/validate-gateway-authorization.sh

echo "Overmesh live Azure posture and authorization gates passed."
