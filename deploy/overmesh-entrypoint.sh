#!/bin/sh
set -eu

runtime_dir=/tmp/overmesh
install -d -m 700 "$runtime_dir"

: "${OVERMESH_CONFIG_BASE64:?OVERMESH_CONFIG_BASE64 is required}"
: "${OVERMESH_RING_BASE64:?OVERMESH_RING_BASE64 is required}"
: "${OVERMESH_RING_SIGNATURE_BASE64:?OVERMESH_RING_SIGNATURE_BASE64 is required}"

printf '%s' "$OVERMESH_CONFIG_BASE64" | base64 -d > "$runtime_dir/config.yaml"
printf '%s' "$OVERMESH_RING_BASE64" | base64 -d > "$runtime_dir/ring.yaml"
printf '%s' "$OVERMESH_RING_SIGNATURE_BASE64" | base64 -d > "$runtime_dir/ring.sig"
chmod 400 "$runtime_dir/config.yaml" "$runtime_dir/ring.yaml" "$runtime_dir/ring.sig"

unset OVERMESH_CONFIG_BASE64 OVERMESH_RING_BASE64 OVERMESH_RING_SIGNATURE_BASE64
exec "$@"
