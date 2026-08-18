#!/usr/bin/env python3
from __future__ import annotations

import base64
import contextlib
import contextvars
import hashlib
import json
import os
import platform
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

from azure.core.pipeline.policies import SansIOHTTPPolicy
from azure.identity import ManagedIdentityCredential, __version__ as identity_version
from azure.storage.blob import BlobBlock, BlobServiceClient, __version__ as blob_version


CURRENT_REQUEST_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_request_id", default=None
)


class ClientRequestIdPolicy(SansIOHTTPPolicy):
    def on_request(self, request: Any) -> None:
        request_id = CURRENT_REQUEST_ID.get()
        if request_id:
            request.http_request.headers["x-ms-client-request-id"] = request_id


@contextlib.contextmanager
def request_id_scope(request_id: str):
    token = CURRENT_REQUEST_ID.set(request_id)
    try:
        yield
    finally:
        CURRENT_REQUEST_ID.reset(token)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def request_id(run_id: str, suffix: str) -> str:
    return f"py-{run_id}-{suffix}"[:128]


def write_result(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def main() -> int:
    result_path = os.environ["OVERMESH_CLIENT_COMPAT_RESULT_PATH"]
    endpoint = os.environ["OVERMESH_CLIENT_COMPAT_ENDPOINT"].rstrip("/")
    container = os.environ["OVERMESH_CLIENT_COMPAT_CONTAINER"]
    prefix = os.environ["OVERMESH_CLIENT_COMPAT_PREFIX"].strip("/")
    run_id = os.environ["OVERMESH_CLIENT_COMPAT_RUN_ID"]
    commit = os.environ["OVERMESH_CLIENT_COMPAT_COMMIT"]
    project_version = os.environ["OVERMESH_CLIENT_COMPAT_PROJECT_VERSION"]
    managed_identity_client_id = os.environ["OVERMESH_CLIENT_COMPAT_MI_CLIENT_ID"]

    operations: list[dict[str, Any]] = []
    status = "passed"
    error: str | None = None
    traceback_text: str | None = None

    simple_blob = f"{prefix}/simple.txt"
    block_blob = f"{prefix}/block.bin"
    simple_bytes = (
        f"client=azure-sdk-python\nrun={run_id}\nblob=simple\n".encode("utf-8")
    )
    block_part_one = f"client=azure-sdk-python|run={run_id}|block=1|".encode("utf-8")
    block_part_two = f"client=azure-sdk-python|run={run_id}|block=2|".encode("utf-8")
    block_bytes = block_part_one + block_part_two

    def add_operation(name: str, result: str, **details: Any) -> None:
        operations.append(
            {
                "name": name,
                "result": result,
                "timestamp_utc": utc_now(),
                **details,
            }
        )

    credential = ManagedIdentityCredential(client_id=managed_identity_client_id)
    service = BlobServiceClient(
        account_url=endpoint,
        credential=credential,
        per_call_policies=[ClientRequestIdPolicy()],
    )
    container_client = service.get_container_client(container)
    simple_client = container_client.get_blob_client(simple_blob)
    block_client = container_client.get_blob_client(block_blob)
    simple_deleted = False
    block_deleted = False
    cleanup_errors: list[str] = []

    try:
        put_request_id = request_id(run_id, "put-blob")
        with request_id_scope(put_request_id):
            simple_client.upload_blob(simple_bytes, overwrite=False)
        add_operation(
            "put_blob",
            "passed",
            blob=simple_blob,
            size_bytes=len(simple_bytes),
            sha256=sha256_hex(simple_bytes),
            request_id=put_request_id,
            request_id_mode="explicit-x-ms-client-request-id",
        )

        block_ids = [
            base64.b64encode(b"block-0001").decode("ascii"),
            base64.b64encode(b"block-0002").decode("ascii"),
        ]
        with request_id_scope(request_id(run_id, "put-block-1")):
            block_client.stage_block(block_ids[0], block_part_one)
        with request_id_scope(request_id(run_id, "put-block-2")):
            block_client.stage_block(block_ids[1], block_part_two)
        add_operation(
            "put_block",
            "passed",
            blob=block_blob,
            block_count=2,
            decoded_block_id_length=len(base64.b64decode(block_ids[0])),
            request_id_mode="explicit-x-ms-client-request-id",
        )

        commit_request_id = request_id(run_id, "put-block-list")
        with request_id_scope(commit_request_id):
            block_client.commit_block_list([BlobBlock(block_id) for block_id in block_ids])
        add_operation(
            "put_block_list",
            "passed",
            blob=block_blob,
            size_bytes=len(block_bytes),
            sha256=sha256_hex(block_bytes),
            request_id=commit_request_id,
            request_id_mode="explicit-x-ms-client-request-id",
        )

        committed_blocks, uncommitted_blocks = block_client.get_block_list(
            block_list_type="all"
        )
        if len(committed_blocks) != 2:
            raise RuntimeError("Committed block count did not match the staged block count.")
        add_operation(
            "get_block_list",
            "passed",
            blob=block_blob,
            committed_block_count=len(committed_blocks),
            uncommitted_block_count=len(uncommitted_blocks),
        )

        downloaded_simple = simple_client.download_blob().readall()
        if downloaded_simple != simple_bytes:
            raise RuntimeError("Downloaded simple blob bytes did not match the uploaded payload.")
        add_operation(
            "get_blob",
            "passed",
            blob=simple_blob,
            sha256=sha256_hex(downloaded_simple),
        )

        downloaded_block = block_client.download_blob().readall()
        if downloaded_block != block_bytes:
            raise RuntimeError("Downloaded block blob bytes did not match the committed payload.")
        add_operation(
            "get_blob_large",
            "passed",
            blob=block_blob,
            sha256=sha256_hex(downloaded_block),
        )

        properties = block_client.get_blob_properties()
        content_length = getattr(properties, "size", None)
        if content_length != len(block_bytes):
            raise RuntimeError(
                f"Blob properties reported content length {content_length}, expected {len(block_bytes)}."
            )
        add_operation(
            "head_blob",
            "passed",
            blob=block_blob,
            content_length=content_length,
        )

        listed_names: list[str] = []
        page_count = 0
        for page in container_client.list_blobs(
            name_starts_with=f"{prefix}/", results_per_page=1
        ).by_page():
            page_count += 1
            for item in page:
                listed_names.append(item.name)
        if simple_blob not in listed_names or block_blob not in listed_names:
            raise RuntimeError("Prefix listing did not contain every expected canary blob.")
        if page_count < 2:
            raise RuntimeError("Expected paged listing to span at least two pages.")
        add_operation(
            "list_blobs",
            "passed",
            blob_count=len(listed_names),
            page_count=page_count,
            blobs=sorted(listed_names),
        )

        with request_id_scope(request_id(run_id, "delete-simple")):
            simple_client.delete_blob()
        simple_deleted = True
        add_operation(
            "delete_blob",
            "passed",
            blob=simple_blob,
            request_id_mode="explicit-x-ms-client-request-id",
        )

        with request_id_scope(request_id(run_id, "delete-block")):
            block_client.delete_blob()
        block_deleted = True
        add_operation(
            "delete_blob_large",
            "passed",
            blob=block_blob,
            request_id_mode="explicit-x-ms-client-request-id",
        )
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error = f"{type(exc).__name__}: {exc}"
        traceback_text = traceback.format_exc()
    finally:
        if not simple_deleted:
            try:
                with request_id_scope(request_id(run_id, "cleanup-simple")):
                    simple_client.delete_blob()
            except Exception as exc:  # noqa: BLE001
                cleanup_errors.append(f"simple cleanup: {type(exc).__name__}: {exc}")
        if not block_deleted:
            try:
                with request_id_scope(request_id(run_id, "cleanup-block")):
                    block_client.delete_blob()
            except Exception as exc:  # noqa: BLE001
                cleanup_errors.append(f"block cleanup: {type(exc).__name__}: {exc}")

        if cleanup_errors and status == "passed":
            status = "failed"
            error = "; ".join(cleanup_errors)

        payload: dict[str, Any] = {
            "client": "azure-sdk-python",
            "result": status,
            "endpoint": endpoint,
            "container": container,
            "prefix": prefix,
            "timestamp_utc": utc_now(),
            "commit": commit,
            "project_version": project_version,
            "tool_versions": {
                "python": platform.python_version(),
                "azure_identity": identity_version,
                "azure_storage_blob": blob_version,
            },
            "operations": operations,
        }
        if error:
            payload["error"] = error
        if traceback_text:
            payload["traceback"] = traceback_text.splitlines()
        write_result(result_path, payload)

    return 0 if status == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
