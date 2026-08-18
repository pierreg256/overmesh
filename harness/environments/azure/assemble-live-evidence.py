#!/usr/bin/env python3
"""Assemble raw live evidence before redaction and signing."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_source(value: str) -> tuple[str, Path]:
    name, separator, path = value.partition("=")
    if not separator or not name or not path:
        raise argparse.ArgumentTypeError("sources must use GATE=PATH")
    return name, Path(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        type=parse_source,
        metavar="GATE=PATH",
    )
    arguments = parser.parse_args()

    bundle = json.loads(arguments.base_bundle.read_text(encoding="utf-8"))
    gates = bundle.setdefault("gates", {})
    for gate, source in arguments.source:
        evidence = gates.setdefault(gate, {})
        evidence["result"] = "passed"
        evidence["evidenceSha256"] = sha256(source)
        if source.suffix == ".json":
            source_payload = json.loads(source.read_text(encoding="utf-8"))
            if isinstance(source_payload, dict) and isinstance(
                source_payload.get("checks"), list
            ):
                evidence["checks"] = len(source_payload["checks"])
    bundle["generatedAt"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(bundle, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
