#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  import-ghcr-images-to-acr.sh \
    --acr-name NAME \
    --tag TAG \
    --commit-sha FULL_GITHUB_SHA \
    [--source-owner OWNER]

Imports the public GHCR Gateway and Reconciler images into ACR by immutable
digest, then creates the requested release and source-commit tags.
EOF
}

acr_name=
tag=
commit_sha=
source_owner=pierreg256

while (($#)); do
  case "$1" in
    --acr-name)
      acr_name=${2:-}
      shift 2
      ;;
    --tag)
      tag=${2:-}
      shift 2
      ;;
    --commit-sha)
      commit_sha=${2:-}
      shift 2
      ;;
    --source-owner)
      source_owner=${2:-}
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ $acr_name =~ ^[a-z0-9]{5,50}$ ]] || {
  echo "--acr-name must be a lowercase Azure Container Registry name." >&2
  exit 2
}
[[ $tag =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || {
  echo "--tag is not a valid container image tag." >&2
  exit 2
}
[[ $commit_sha =~ ^[0-9a-fA-F]{40}$ ]] || {
  echo "--commit-sha must be a full 40-character Git commit SHA." >&2
  exit 2
}
[[ $source_owner =~ ^[A-Za-z0-9][A-Za-z0-9-]{0,38}$ ]] || {
  echo "--source-owner is not a valid GitHub owner." >&2
  exit 2
}

command -v az >/dev/null || {
  echo "Azure CLI is required." >&2
  exit 1
}
command -v jq >/dev/null || {
  echo "jq is required." >&2
  exit 1
}
docker buildx version >/dev/null 2>&1 || {
  echo "Docker Buildx is required to resolve public GHCR digests." >&2
  exit 1
}

short_sha=${commit_sha:0:12}
source_owner=$(printf '%s' "$source_owner" | LC_ALL=C tr '[:upper:]' '[:lower:]')

for image in overmesh-gateway overmesh-reconciler; do
  source="ghcr.io/${source_owner}/${image}:${tag}"
  digest=$(
    docker buildx imagetools inspect "$source" --format '{{json .Manifest}}' |
      jq -er '.digest // .Digest'
  )
  [[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "GHCR returned an invalid digest for $source." >&2
    exit 1
  }

  echo "Importing ${source_owner}/${image}@${digest} into ${acr_name}..."
  az acr import \
    --name "$acr_name" \
    --source "ghcr.io/${source_owner}/${image}@${digest}" \
    --image "${image}:${tag}" \
    --image "${image}:sha-${short_sha}" \
    --force \
    --only-show-errors \
    --output none
  printf '%s\t%s\n' "$image" "$digest"
done
