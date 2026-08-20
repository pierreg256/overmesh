#!/usr/bin/env python3
"""Build canonical redacted live evidence before it is signed."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

ARM_RESOURCE = re.compile(
    r"/subscriptions/[0-9a-fA-F-]{36}/resourceGroups/[^/\"'\s]+"
    r"/providers/[^\"'\s,}]+",
    re.IGNORECASE,
)
SUBSCRIPTION_PATH = re.compile(
    r"/subscriptions/([0-9a-fA-F-]{36})",
    re.IGNORECASE,
)
RESOURCE_GROUP_PATH = re.compile(
    r"/resourceGroups/([^/\"'\s]+)",
    re.IGNORECASE,
)
STORAGE_ACCOUNT_PATH = re.compile(
    r"/storageAccounts/([^/\"'\s]+)",
    re.IGNORECASE,
)
GUID = re.compile(
    r"(?<![0-9a-fA-F])"
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"(?![0-9a-fA-F])"
)
HOST_PATTERNS = [
    (re.compile(r"[a-zA-Z0-9.-]+\.azurefd\.net", re.IGNORECASE), "fd"),
    (re.compile(r"[a-zA-Z0-9.-]+\.vault\.azure\.net", re.IGNORECASE), "kv"),
    (
        re.compile(r"[a-zA-Z0-9.-]+\.blob\.core\.windows\.net", re.IGNORECASE),
        "st",
    ),
    (
        re.compile(r"[a-zA-Z0-9.-]+\.azurecontainerapps\.io", re.IGNORECASE),
        "aca",
    ),
    (re.compile(r"[a-zA-Z0-9.-]+\.azurecr\.io", re.IGNORECASE), "acr"),
]


def pseudonym(kind: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"{kind}-{digest}"


def redact_text(value: str) -> str:
    value = ARM_RESOURCE.sub(lambda match: pseudonym("arm", match.group(0)), value)
    for pattern, kind in HOST_PATTERNS:
        value = pattern.sub(
            lambda match, prefix=kind: pseudonym(prefix, match.group(0).lower()),
            value,
        )
    value = SUBSCRIPTION_PATH.sub(
        lambda match: pseudonym("sub", match.group(1)),
        value,
    )
    value = RESOURCE_GROUP_PATH.sub(
        lambda match: f"/resourceGroups/{pseudonym('rg', match.group(1))}",
        value,
    )
    value = STORAGE_ACCOUNT_PATH.sub(
        lambda match: f"/storageAccounts/{pseudonym('st', match.group(1))}",
        value,
    )
    return GUID.sub(lambda match: pseudonym("id", match.group(0)), value)


def redact_json(value: object, key: str | None = None) -> object:
    if isinstance(value, dict):
        return {
            name: redact_json(child, name)
            for name, child in value.items()
        }
    if isinstance(value, list):
        return [redact_json(child, key) for child in value]
    if not isinstance(value, str):
        return value
    normalized_key = (key or "").lower()
    if normalized_key in {"subscription", "subscriptionid"}:
        return pseudonym("sub", value)
    if normalized_key in {"tenant", "tenantid"}:
        return pseudonym("tenant", value)
    if normalized_key in {"resourcegroup", "resourcegroupname"}:
        return pseudonym("rg", value)
    if normalized_key in {
        "principalid",
        "objectid",
        "clientid",
        "managedidentityclientid",
    }:
        return pseudonym("id", value)
    return redact_text(value)


def redact_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.suffix == ".pem":
        shutil.copyfile(source, destination)
        return
    text = source.read_text(encoding="utf-8")
    if source.suffix == ".json":
        payload = redact_json(json.loads(text))
        destination.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return
    if source.suffix == ".jsonl":
        lines = [
            json.dumps(redact_json(json.loads(line)), sort_keys=True)
            for line in text.splitlines()
            if line.strip()
        ]
        destination.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return
    destination.write_text(redact_text(text), encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_source(value: str) -> tuple[str, Path]:
    name, separator, path = value.partition("=")
    if not separator or not name or not path:
        raise argparse.ArgumentTypeError("sources must use GATE=PATH")
    return name, Path(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-bundle", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--bundle-name")
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        type=parse_source,
        metavar="GATE=PATH",
    )
    parser.add_argument(
        "--public-key",
        action="append",
        default=[],
        type=Path,
    )
    arguments = parser.parse_args()

    output = arguments.output_directory
    output.mkdir(parents=True, exist_ok=True)
    bundle = redact_json(
        json.loads(arguments.raw_bundle.read_text(encoding="utf-8"))
    )
    if not isinstance(bundle, dict):
        raise SystemExit("the raw bundle must contain a JSON object")
    gates = bundle.get("gates")
    if gates is not None and not isinstance(gates, dict):
        raise SystemExit("the raw bundle gates field must contain an object")
    if arguments.source and gates is None:
        gates = {}
        bundle["gates"] = gates

    for gate, source in arguments.source:
        if not isinstance(gates, dict):
            raise SystemExit("evidence sources require a gates object")
        destination = output / source.name
        redact_file(source, destination)
        gate_evidence = gates.setdefault(gate, {})
        if not isinstance(gate_evidence, dict):
            raise SystemExit(f"gate {gate!r} must contain an object")
        gate_evidence["result"] = "passed"
        gate_evidence["evidenceSha256"] = sha256(destination)

    for public_key in arguments.public_key:
        redact_file(public_key, output / public_key.name)

    bundle["generatedAt"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    bundle["redaction"] = {
        "apiVersion": "evidence.overmesh.io/redaction/v1",
        "algorithm": "sha256-truncated-16",
        "canonicalForm": "redacted-before-signing",
        "rawBundleSha256": sha256(arguments.raw_bundle),
    }
    bundle_path = output / (arguments.bundle_name or arguments.raw_bundle.name)
    bundle_path.write_text(
        json.dumps(bundle, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
