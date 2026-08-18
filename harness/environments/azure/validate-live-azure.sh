#!/usr/bin/env bash
set -euo pipefail

./harness/environments/azure/validate-storage-authorization.sh
./harness/environments/azure/validate-gateway-authorization.sh
./harness/environments/azure/validate-client-compatibility.sh

echo "Overmesh live Azure posture, authorization, and client compatibility gates passed."
