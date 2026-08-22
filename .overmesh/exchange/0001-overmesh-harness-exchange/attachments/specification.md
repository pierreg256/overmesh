# Specification — `overmesh-harness exchange`

## Purpose

Provide a typed, append-only, file-backed work exchange between two assistants
working on the same repository. The operator remains approver rather than
transcriber. This is a work queue, not a chat relay, streaming system, shared
memory, automatic work executor, web interface, or network transport.

The protocol must prevent two assistants from converging on a shared wrong
answer without human review.

## Storage

```text
.overmesh/exchange/
  <nnnn>-<slug>/
    001-claude.json
    002-copilot.json
    003-human.json
    attachments/
      specification.md
```

There is one immutable JSON file per message and no mutable thread file.
Messages are committed. The server stages new message files with `git add` and
never commits.

Thread IDs use a zero-padded sequence and slug so ADRs and evidence can cite an
exchange thread.

## Message schema

Every message contains:

- `schemaVersion: 1`;
- `thread`;
- numeric `seq`;
- `author`;
- typed `kind`;
- millisecond UTC `createdAt`;
- non-empty `subject`;
- Markdown `body`, limited to 16 KiB;
- optional `repliesTo`;
- validated `refs`;
- optional `answeredBy`;
- optional `outcome`.

Long instructions belong under the thread's `attachments/` directory and are
cited as an `artifact` ref.

## Message kinds and constraints

Exactly seven message kinds exist:

| Kind | Constraint |
| --- | --- |
| `finding` | At least one `code`, `commit`, `artifact`, or `record` ref |
| `question` | Non-empty `answeredBy` naming what would settle it |
| `correction` | `repliesTo` and at least one non-URL ref |
| `spec` | Body withheld from assistant reads until approved |
| `report` | At least one changed-file ref |
| `verdict` | Authorship rule, non-URL ref, and outcome |
| `approval` | Human author and operator CLI only |

There is no free-form kind.

Refs are validated atomically before any message is stored:

- `code`: `path` or `path:line`, existing in the working tree;
- `commit`: resolves through `git cat-file -t`;
- `artifact`: existing under `harness/artifacts/` or the thread's
  `attachments/`;
- `record`: existing under `docs/adr/`;
- `url`: accepted without validation and never satisfies a ref minimum.

An invalid ref rejects the whole post and writes nothing.

## Derived thread state

No state cache or state file exists. Every read derives:

- `open` when no stronger condition applies;
- `awaiting-approval` when the last `spec` or `verdict` lacks a following
  approval;
- `escalated` when the consecutive non-human limit is reached;
- `resolved` when a verdict has a following approval.

`waitingOn` is the other assistant in an open thread, and `human` for approval
or escalation.

## MCP tools

The stdio MCP server exposes exactly four tools:

1. `exchange_list()`: board sorted by `updatedAt` descending.
2. `exchange_read(thread, since?)`: state and messages after `since`; an
   unapproved spec appears with `body: null` and
   `withheld: "awaiting approval"`.
3. `exchange_post(kind, subject, body, thread?, refs?, repliesTo?,
   answeredBy?)`: posts to a thread or atomically allocates the next thread.
4. `exchange_resolve(thread, outcome, body, refs)`: deliberately posts a
   verdict with outcome `verified`, `not-verified`, `withdrawn`, or
   `superseded`.

No fifth tool is added in version 1.

Thread creation uses atomic `mkdir` with collision retry. Message sequence
allocation uses exclusive creation and collision retry. Concurrent posts must
produce distinct sequences and lose neither file.

## Escalation and authorship

At most five consecutive non-human messages may be posted in a thread. The
thread is `escalated` at the limit, a sixth non-human post is rejected, and a
human message resets the counter.

A verdict is rejected when its author also authored the last non-verdict
message. The other assistant or operator must close the thread.

`OVERMESH_EXCHANGE_AUTHOR` is required for the MCP server and must match the
configured allowlist. The server rejects unknown authors and always rejects
`human`. Only the operator CLI can create human messages and approvals.

## Approval gate

The operator approves specs and verdicts. Findings, questions, corrections,
and reports flow without approval.

The gate is structural:

- an unapproved spec body is not returned by `exchange_read`;
- an unapproved verdict never resolves a thread;
- approval is its own human-authored message with `repliesTo`;
- rejection is an approval message with `outcome: "rejected"` and an
  explanatory body.

## Commands

Server:

```text
overmesh-harness exchange-mcp
```

Operator:

```text
overmesh-harness exchange list
overmesh-harness exchange show <thread>
overmesh-harness exchange approve <thread> [--seq N] [-m "..."]
overmesh-harness exchange reject  <thread> [--seq N] -m "..."
overmesh-harness exchange post    <thread> --kind finding -m "..." --ref ...
```

## Required verification

- Finding without a non-URL ref is rejected.
- Missing ref paths reject the post and write no file.
- Correction without `repliesTo` is rejected.
- Question without `answeredBy` is rejected.
- The sixth consecutive non-human message is rejected and state is escalated.
- Human input resets escalation.
- MCP rejects a human server identity.
- Spec body is withheld until approval.
- Same-side verdict is rejected.
- Concurrent posts receive distinct sequences and files.
- State is reproducible from files alone.
- Bodies larger than 16 KiB point the author to `attachments/`.
- New message files are staged but not committed.

The first exchange thread is this specification. It is posted as a `spec`,
approved through the operator CLI, followed by an implementation report, and
closed only by a verdict from the other assistant plus operator approval.
