import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import process from "node:process";
import { createRequire } from "node:module";

import { ManagedIdentityCredential } from "@azure/identity";
import {
  BaseRequestPolicy,
  BlobServiceClient,
  newPipeline,
} from "@azure/storage-blob";

const require = createRequire(import.meta.url);
const storagePackage = require("@azure/storage-blob/package.json");
const identityPackage = require("@azure/identity/package.json");

let currentRequestId = null;

function utcNow() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

function sha256Hex(buffer) {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

function requestId(runId, suffix) {
  return `node-${runId}-${suffix}`.slice(0, 128);
}

async function withRequestId(id, operation) {
  const previous = currentRequestId;
  currentRequestId = id;
  try {
    return await operation();
  } finally {
    currentRequestId = previous;
  }
}

async function streamToBuffer(readable) {
  if (!readable) {
    return Buffer.alloc(0);
  }
  const chunks = [];
  for await (const chunk of readable) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

async function writeResult(path, payload) {
  await fs.writeFile(path, JSON.stringify(payload, null, 2));
}

const resultPath = process.env.OVERMESH_CLIENT_COMPAT_RESULT_PATH;
const endpoint = process.env.OVERMESH_CLIENT_COMPAT_ENDPOINT.replace(/\/+$/, "");
const container = process.env.OVERMESH_CLIENT_COMPAT_CONTAINER;
const prefix = process.env.OVERMESH_CLIENT_COMPAT_PREFIX.replace(/^\/+|\/+$/g, "");
const runId = process.env.OVERMESH_CLIENT_COMPAT_RUN_ID;
const commit = process.env.OVERMESH_CLIENT_COMPAT_COMMIT;
const projectVersion = process.env.OVERMESH_CLIENT_COMPAT_PROJECT_VERSION;
const managedIdentityClientId = process.env.OVERMESH_CLIENT_COMPAT_MI_CLIENT_ID;

const operations = [];
const simpleBlob = `${prefix}/simple.txt`;
const blockBlob = `${prefix}/block.bin`;
const simpleBytes = Buffer.from(
  `client=azure-sdk-node\nrun=${runId}\nblob=simple\n`,
  "utf8",
);
const blockPartOne = Buffer.from(
  `client=azure-sdk-node|run=${runId}|block=1|`,
  "utf8",
);
const blockPartTwo = Buffer.from(
  `client=azure-sdk-node|run=${runId}|block=2|`,
  "utf8",
);
const blockBytes = Buffer.concat([blockPartOne, blockPartTwo]);

function addOperation(name, result, details = {}) {
  operations.push({
    name,
    result,
    timestamp_utc: utcNow(),
    ...details,
  });
}

const credential = new ManagedIdentityCredential({ clientId: managedIdentityClientId });
const pipeline = newPipeline(credential);
class ClientRequestIdPolicy extends BaseRequestPolicy {
  sendRequest(request) {
    if (currentRequestId) {
      request.headers.set("x-ms-client-request-id", currentRequestId);
    }
    return this._nextPolicy.sendRequest(request);
  }
}
pipeline.factories.unshift({
  create(nextPolicy, options) {
    return new ClientRequestIdPolicy(nextPolicy, options);
  },
});

const serviceClient = new BlobServiceClient(endpoint, pipeline);
const containerClient = serviceClient.getContainerClient(container);
const simpleClient = containerClient.getBlockBlobClient(simpleBlob);
const blockClient = containerClient.getBlockBlobClient(blockBlob);

let status = "passed";
let error;
let trace;
let simpleDeleted = false;
let blockDeleted = false;
const cleanupErrors = [];

try {
  const putRequestId = requestId(runId, "put-blob");
  await withRequestId(putRequestId, () => simpleClient.uploadData(simpleBytes));
  addOperation("put_blob", "passed", {
    blob: simpleBlob,
    size_bytes: simpleBytes.length,
    sha256: sha256Hex(simpleBytes),
    request_id: putRequestId,
    request_id_mode: "explicit-x-ms-client-request-id",
  });

  const blockIds = [
    Buffer.from("block-0001", "utf8").toString("base64"),
    Buffer.from("block-0002", "utf8").toString("base64"),
  ];
  await withRequestId(requestId(runId, "put-block-1"), () =>
    blockClient.stageBlock(blockIds[0], blockPartOne, blockPartOne.length),
  );
  await withRequestId(requestId(runId, "put-block-2"), () =>
    blockClient.stageBlock(blockIds[1], blockPartTwo, blockPartTwo.length),
  );
  addOperation("put_block", "passed", {
    blob: blockBlob,
    block_count: 2,
    decoded_block_id_length: Buffer.from(blockIds[0], "base64").length,
    request_id_mode: "explicit-x-ms-client-request-id",
  });

  const commitRequestId = requestId(runId, "put-block-list");
  await withRequestId(commitRequestId, () => blockClient.commitBlockList(blockIds));
  addOperation("put_block_list", "passed", {
    blob: blockBlob,
    size_bytes: blockBytes.length,
    sha256: sha256Hex(blockBytes),
    request_id: commitRequestId,
    request_id_mode: "explicit-x-ms-client-request-id",
  });

  const blockList = await blockClient.getBlockList("all");
  if ((blockList.committedBlocks ?? []).length !== 2) {
    throw new Error("Committed block count did not match the staged block count.");
  }
  addOperation("get_block_list", "passed", {
    blob: blockBlob,
    committed_block_count: (blockList.committedBlocks ?? []).length,
    uncommitted_block_count: (blockList.uncommittedBlocks ?? []).length,
  });

  const simpleDownload = await simpleClient.download();
  const simpleDownloadedBytes = await streamToBuffer(simpleDownload.readableStreamBody);
  if (!simpleDownloadedBytes.equals(simpleBytes)) {
    throw new Error("Downloaded simple blob bytes did not match the uploaded payload.");
  }
  addOperation("get_blob", "passed", {
    blob: simpleBlob,
    sha256: sha256Hex(simpleDownloadedBytes),
  });

  const blockDownload = await blockClient.download();
  const blockDownloadedBytes = await streamToBuffer(blockDownload.readableStreamBody);
  if (!blockDownloadedBytes.equals(blockBytes)) {
    throw new Error("Downloaded block blob bytes did not match the committed payload.");
  }
  addOperation("get_blob_large", "passed", {
    blob: blockBlob,
    sha256: sha256Hex(blockDownloadedBytes),
  });

  const properties = await blockClient.getProperties();
  if (properties.contentLength !== blockBytes.length) {
    throw new Error(
      `Blob properties reported content length ${properties.contentLength}, expected ${blockBytes.length}.`,
    );
  }
  addOperation("head_blob", "passed", {
    blob: blockBlob,
    content_length: properties.contentLength,
  });

  const listedNames = [];
  let pageCount = 0;
  for await (const page of containerClient
    .listBlobsFlat({ prefix: `${prefix}/` })
    .byPage({ maxPageSize: 1 })) {
    pageCount += 1;
    for (const item of page.segment.blobItems ?? []) {
      listedNames.push(item.name);
    }
  }
  if (!listedNames.includes(simpleBlob) || !listedNames.includes(blockBlob)) {
    throw new Error("Prefix listing did not contain every expected canary blob.");
  }
  if (pageCount < 2) {
    throw new Error("Expected paged listing to span at least two pages.");
  }
  addOperation("list_blobs", "passed", {
    blob_count: listedNames.length,
    page_count: pageCount,
    blobs: listedNames.sort(),
  });

  await withRequestId(requestId(runId, "delete-simple"), () => simpleClient.deleteIfExists());
  simpleDeleted = true;
  addOperation("delete_blob", "passed", {
    blob: simpleBlob,
    request_id_mode: "explicit-x-ms-client-request-id",
  });

  await withRequestId(requestId(runId, "delete-block"), () => blockClient.deleteIfExists());
  blockDeleted = true;
  addOperation("delete_blob_large", "passed", {
    blob: blockBlob,
    request_id_mode: "explicit-x-ms-client-request-id",
  });
} catch (caught) {
  status = "failed";
  error = `${caught?.name ?? "Error"}: ${caught?.message ?? String(caught)}`;
  trace = caught?.stack ?? String(caught);
} finally {
  if (!simpleDeleted) {
    try {
      await withRequestId(requestId(runId, "cleanup-simple"), () =>
        simpleClient.deleteIfExists(),
      );
    } catch (caught) {
      cleanupErrors.push(`simple cleanup: ${caught?.message ?? String(caught)}`);
    }
  }
  if (!blockDeleted) {
    try {
      await withRequestId(requestId(runId, "cleanup-block"), () =>
        blockClient.deleteIfExists(),
      );
    } catch (caught) {
      cleanupErrors.push(`block cleanup: ${caught?.message ?? String(caught)}`);
    }
  }

  if (cleanupErrors.length > 0 && status === "passed") {
    status = "failed";
    error = cleanupErrors.join("; ");
  }

  const payload = {
    client: "azure-sdk-node",
    result: status,
    endpoint,
    container,
    prefix,
    timestamp_utc: utcNow(),
    commit,
    project_version: projectVersion,
    tool_versions: {
      node: process.version,
      "@azure/identity": identityPackage.version,
      "@azure/storage-blob": storagePackage.version,
      platform: os.platform(),
      arch: os.arch(),
    },
    operations,
  };
  if (error) {
    payload.error = error;
  }
  if (trace) {
    payload.traceback = trace.split("\n");
  }
  await writeResult(resultPath, payload);
}

process.exit(status === "passed" ? 0 : 1);
