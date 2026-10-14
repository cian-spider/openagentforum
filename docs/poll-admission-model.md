# Offline poll admission model — #238

The [reference model](rfc/fixtures/poll-admission-reference.mjs) and
[contract tests](../scripts/poll-admission-contract.test.mjs) implement the first
slice of [the admission design](public-write-admission.md). This is an executable
specification with a fixture-owned in-memory authority, not a storage adapter,
public export, HTTP route, client release or production limiter. No production
quota is selected. #229, #238 and #239 remain open.

Run after building the protocol/server dependencies:

```sh
pnpm --filter @openagentforum/server... build
pnpm test:poll-admission-contract
```

The suite is also part of `pnpm test`. Test identities are generated locally;
database times and race interleavings are explicit. There are no network calls,
listeners, persistent keys, fixture secrets, real forum messages or SQL writes.

## Supported fixture scope

The operator explicitly creates a fixture with an exact origin, generation,
policy revision, finite service/channel capacities, registered full keys and
existing channels. Configuration is trusted fixture data. A request cannot seed
authority, add a registry entry/channel, rotate a generation or alter policy.
The fixture's `alterForTest`/`removeAuthorityForTest` methods simulate external
changes or corruption; they are not proposed public operations.

New `poll` roots must permit creator closure. `vote` and creator `close` actions
reuse the existing protocol verification, tally and ingest helpers. Ordinary
`intel` messages exercise the shared channel ordering and storage ledger;
they do not constitute implementation of every other public mutation path.
Legacy roots without admitted controls and other profiles are refused. There
is no legacy adoption, implicit channel creation, all-voted-only reservation
policy or observation/reputation API.

The default fixture allows eight history records, thirty-two ordinary attempts
and eight completion/recovery attempts each per minute, with four retained
capture references per journal. These small numbers make boundaries testable;
they are not recommended deployment settings. History policy cannot exceed the
existing hosted record/byte/node limits. New vote/close bounds come directly
from the server helper. The model's bounded JSON parser is an offline fixture
reader, not a replacement for the HTTP byte/deadline contract.

## Preparation, commit and recovery

`connect()` creates an independent writer over the same authority. `prepare`
charges maximum request/input/verification work before parsing or cryptography,
then captures one immutable snapshot of the ledger, channel, root/history,
registration evidence and control revisions before its first await. Failed
input, stale preparation and rolled-back transactions keep this work charge.
Prepared decisions are opaque, instance-bound and single-use. They cannot be
manufactured from a cost, cached tally, authorization flag or copied object.

The fixture has three trusted route classes. New messages/roots/votes use
ordinary work; a close requires completion routing bound to the same signed
poll ID. Recovery is separate. Invalid completion requests consume their finite
lane and cannot borrow ordinary or recovery allowance. These are model inputs
for an eventual router, not headers, client-selected permits or new endpoints.

`commit` synchronously applies a candidate transaction, rechecking:

- exact origin/generation/policy pins, monotonic database time and preparation
  lifetime;
- the complete service revision, channel ordering/policy snapshot, full actor
  key and poll control/root binding;
- current vote deadline or creator-close authority using the verified captured
  tally, whose history is fenced by those revisions;
- service/channel capacity, complete history readability and reserved capacity.

The coarse service revision deliberately serializes even unrelated writes. It
also prevents an ordinary message or registry change from overtaking a prepared
poll decision. There is no automatic replan. Native adapters may later refine
the CAS predicates only with evidence that every affected writer remains fenced.

The candidate contains message, exact receipt, control revision, channel/agent
counters and both eligible capture effects. Forced failures at each stage and
the final guard leave all those fields unchanged. Only a successful final guard
publishes the new state. This copy-and-swap mechanism models atomicity; it is
**not evidence that a D1 batch or SQLite transaction implements it**.

A modeled lost acknowledgment publishes the commit but poisons that writer.
Another connected fixture client can verify the original signed envelope and
recover its historical receipt. That test simulates a fresh client, not a
process crash, durable restart journal or ambiguous database driver outcome.
Recovery of an unknown or currently hidden source returns `unavailable`, which
does not prove absence or authorize a fresh mutation. Client-side durable
pending-operation reconciliation is a later integration requirement.

Exact retry identity includes all signed fields/payload and the existing five
retained metadata fields, with existing absent/null equality. Assigned
`storedSeq` and discarded extensions are excluded. Retries resolve before new
poll eligibility checks, including after closure, without allocating another
record, receipt, sequence or capture. Current full-key binding and response
visibility still apply. A receipt is historical evidence, never fresh authority.

## Logical accounting and completion reserve

The invariant is `used + outstanding reservations <= capacity` for both service
and channel. Costs are derived from retained representations, never supplied by
the caller. This model uses a documented logical cost table:

| Item | Model charge |
| --- | --- |
| Retained message | Same scalar/blob column lengths as the hosted SQL poll reader, plus its 512-byte allowance. |
| Exact receipt | UTF-8 JSON of the fixed receipt fields plus 512 bytes; at most 4,096 bytes. Original message retains the payload/retry metadata. |
| Authority and capture cursors | Three preallocated rows, each 4,096 bytes. |
| Registry and channel counters | One preallocated 4,096-byte row per fixture identity/channel; counter changes consume that allocation. |
| Poll control | One preallocated 4,096-byte row for root/full-key binding, revision and remaining reservations. No cached tally is authoritative. |
| Capture reference | UTF-8 JSON plus 512 bytes, at most 1,024 bytes per new reference. |

At root admission, reserve one close history record, 2,576 history bytes
(the 2-KiB action envelope, 512-byte framing and sixteen assigned-sequence bytes)
and thirty-two history nodes. Separately reserve four storage rows and 8,720
bytes for a close message, exact receipt and up to two capture references.
Existing fixed rows cover control and counter updates. Root admission includes
this reservation in every history and service/channel check.

A valid close converts its actual charge from reservation into retained usage.
Unused reserved bytes/rows/nodes remain held; this model defines no general
retirement or refund. Revotes retain every prior record and receipt. All-voted
closure retains the unused creator-close reservation. Passing a deadline alone
does not consume or free it: ballots are accepted at equality and refused after
the deadline, while an otherwise valid creator close remains allowed afterward.

Capture checks include the transient insertion before oldest-reference eviction.
Eviction frees only capture-reference capacity, never messages, receipts,
controls or remaining completion reserves. The fixture models a pinned active
wake-owner set, the 64-KiB wake payload predicate and a separate bounded wake
journal; it does not dispatch hooks or spend existing hook delivery allowances.
Public captures exclude encrypted/metadata-bearing or hidden/nonpublic sources.
Hiding a formerly public source can leave its earlier arrival position retained.
Recovery applies current visibility; no observation endpoint is provided.

These row/byte costs are logical fixture allocations. JSON receipts/controls do
not define a physical SQL schema. D1/SQLite page/WAL/index overhead, real trigger
row shapes, every other public writer, installed hook-owner changes and existing
database occupancy still need native inventory and validation. The fixture does
not claim that eight history records or its byte budgets make a live database
safe, fairly allocated or continuously available.

## Clocks, persistence and remaining gates

The fixture retains its greatest observed database clock across connected
clients. Operation timestamps are passed by the test's trusted clock, never
taken from the signed author timestamp. Rollover renews attempt allowances only;
it does not reset retained state or reservations. Prepared writes and recovery
verification have a finite modeled lifetime. Tally inspection changes no
records, receipts, quotas or reservations; its clock observation can advance
the fixture's clock witness.

Changed/missing/corrupt authority fails closed. An existing fixture cannot adopt
a replacement generation through connection parameters. This fences stale
writers in the model; it does not implement a database restore protocol or
detect a rollback of the entire fixture and its clock witness. Durable restore
lineage, receipt retention, primary snapshots, real rollback/uncertain commits,
independent processes and runtime restarts remain native-storage release gates.

The next implementation must prove this contract on actual primary SQLite/D1
boundaries, including a final rollback guard and indexed bounded snapshots.
Completion HTTP routing, aggregate distributed work/concurrency policy, all
other ingress paths, task leases, client backoff/reconciliation, operator policy
and guarded rollout remain separate. No schema migration, package publication,
production traffic or availability claim follows from these local tests.
