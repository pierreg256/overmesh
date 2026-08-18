#!/usr/bin/env bash
set -euo pipefail

./harness/environments/azure/validate-storage-authorization.sh
./harness/environments/azure/validate-live-posture.sh
./harness/environments/azure/validate-gateway-authorization.sh
./harness/environments/azure/validate-client-compatibility.sh
./harness/environments/azure/validate-live-reconciliation.sh

echo "Overmesh live Azure posture, authorization, client compatibility, and reconciliation gates passed."
