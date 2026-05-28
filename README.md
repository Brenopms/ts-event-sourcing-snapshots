# @ts-event-sourcing/snapshots

[![npm version](https://img.shields.io/npm/v/@ts-event-sourcing/snapshots)](https://www.npmjs.com/package/@ts-event-sourcing/snapshots)
[![license](https://img.shields.io/npm/l/@ts-event-sourcing/snapshots)](LICENSE)

A snapshot extension for [`@ts-event-sourcing/core`](https://www.npmjs.com/package/@ts-event-sourcing/core) that accelerates aggregate rebuilds for long-lived event streams.

Event-sourced aggregates rebuild their state by replaying every event from the beginning of the stream. For short-lived aggregates this is fast. For aggregates that accumulate thousands of events over their lifetime, the replay cost grows with every command. This library introduces a snapshot layer that lets you persist aggregate state at a given event version and resume from there — so a stream with 10,000 events and a snapshot at version 9,950 only replays the last 50 events on load.

Snapshots are a **performance optimization, not a source of truth.** The event store remains authoritative. A snapshot at version 500 is only valid if the events that produced it are still in the store. This library makes that invariant easy to maintain.

```
npm install @ts-event-sourcing/snapshots
```

---

## Table of Contents

- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [API Reference](#api-reference)
  - [`SnapshotStore<State>`](#snapshotstorestate)
  - [`SnapshotError`](#snapshtoterror)
  - [`InMemorySnapshotStore<State>`](#inmemorysnapshtotstorestate)
  - [`loadAggregateWithSnapshot`](#loadaggregateWithsnapshot)
  - [`takeSnapshot`](#takesnapshot)
  - [`defineSnapshotCommand`](#definesnapshotcommand)
- [Departures from Core](#departures-from-core)
- [Migrating from Core](#migrating-from-core)
- [Snapshot Lifecycle](#snapshot-lifecycle)
- [Implementing a Production SnapshotStore](#implementing-a-production-snapshotstore)
- [Testing Strategy](#testing-strategy)
- [Pitfalls & Best Practices](#pitfalls--best-practices)
- [Out of Scope](#out-of-scope)
- [Design Philosophy](#design-philosophy)

---

## Quick Start

For engineers already familiar with `@ts-event-sourcing/core`. Full explanations follow below.

**1. Set up your stores — one for events, one for snapshots:**

```ts
import { InMemoryEventStore } from "@ts-event-sourcing/core"
import { InMemorySnapshotStore } from "@ts-event-sourcing/snapshots"

const store = new InMemoryEventStore<CartEvent>()
const snapshotStore = new InMemorySnapshotStore<CartState>()
```

**2. Replace `defineCommand` with `defineSnapshotCommand`:**

Your aggregate definition and command handlers are unchanged. Only the binding step changes.

```ts
import { defineSnapshotCommand } from "@ts-event-sourcing/snapshots"

// Before (core):
// const addItem = defineCommand({ aggregate: cartAggregate, handler: addItemHandler })

// After (snapshots):
const addItem = defineSnapshotCommand({
  aggregate: cartAggregate,
  handler: addItemHandler,
  snapshotStore,            // ← the only addition
})
```

**3. Execute commands and take snapshots periodically:**

The `execute` method has the same signature as core's `defineCommand`. Add a `takeSnapshot` call wherever your snapshotting strategy dictates.

```ts
import { createAggregate, unwrap } from "@ts-event-sourcing/core"
import { takeSnapshot } from "@ts-event-sourcing/snapshots"

await createAggregate({ store, streamId: "cart-1", events: [], idempotencyKey: "open" })

const result = unwrap(
  await addItem.execute({
    store,
    streamId: "cart-1",
    command: { itemId: "apple" },
    idempotencyKey: "add-apple",
  })
)

// Snapshot every 100 events — or on any schedule you choose
if (result.lastVersion % 100 === 0) {
  await takeSnapshot({
    snapshotStore,
    streamId: "cart-1",
    snapshot: { state: result.state, version: result.lastVersion },
  })
}
```

**Key things to know:**

- Domain code — aggregates, reducers, command handlers — is **completely unchanged**
- `defineSnapshotCommand` is a drop-in replacement for `defineCommand` with one additional parameter
- `takeSnapshot` chooses `create` or `replace` automatically — you never interact with the store directly
- The library **never throws** — all failures, including snapshot store errors, are typed `Result` values
- `InMemorySnapshotStore` is for tests only; bring your own implementation for production

---

## How It Works

When you call `defineSnapshotCommand(...).execute(...)`, the execution pipeline changes in one place: **aggregate loading**.

In core, `executeCommand` loads the stream from the event store and folds all events to rebuild state. With `defineSnapshotCommand`, the loading step is delegated to `loadAggregateWithSnapshot`:

1. **Load the latest snapshot** from the `SnapshotStore` for the given stream ID
2. **Load all events** from the `EventStore` for that stream
3. **If a snapshot exists**, discard all events up to and including the snapshot version, and fold only the remaining events into the snapshot state
4. **If no snapshot exists**, fall back to a full replay from `initialState` — identical to core's behaviour
5. Return `{ state, lastVersion }` to the caller

The returned value is always identical to what a full replay would produce. Snapshots accelerate loading without changing the result.

```
Event store:   [v1] [v2] ... [v500] [v501] ... [v600]
Snapshot:                    ↑ state at v500
                                      ↑ fold these 100 events only
Result:        state = fold(snapshot.state, events[500..600])
```

After the command handler runs and new events are appended, the post-append state rebuild uses an **incremental fold** — the new events are folded into the loader's returned state rather than replaying the entire stream again.

---

## API Reference

### `SnapshotStore<State>`

The persistence contract for snapshots. Implement this interface to connect the library to any storage backend.

```ts
interface SnapshotStore<State> {
  create(params: {
    streamId: string
    snapshot: { version: number; state: State }
  }): Promise<Result<void, SnapshotError>>

  replace(params: {
    streamId: string
    snapshot: { version: number; state: State }
  }): Promise<Result<void, SnapshotError>>

  load(params: {
    streamId: string
  }): Promise<Result<Snapshot<State> | null, SnapshotError>>
}
```

- `create` — stores the first snapshot for a stream. Returns `SnapshotAlreadyExists` if one already exists.
- `replace` — overwrites the existing snapshot for a stream. Returns `SnapshotNotFound` if none exists.
- `load` — returns the current snapshot for a stream, or `null` if none has been taken yet.

One snapshot is stored per stream ID. There is no built-in snapshot history. If you need multiple checkpoints per stream, use distinct key schemes in your implementation (e.g. `cart-1:v500`).

The `Snapshot<State>` type returned by `load` includes the `streamId` alongside the version and state you stored:

```ts
type Snapshot<State> = {
  streamId: string
  version: number
  state: State
}
```

---

### `SnapshotError`

A discriminated union covering all error conditions from snapshot storage operations.

```ts
type SnapshotError =
  | { type: "SnapshotAlreadyExists" }
  | { type: "SnapshotNotFound" }
  | { type: "StoreError"; cause: unknown }
```

- `SnapshotAlreadyExists` — returned by `create` when a snapshot already exists for the stream
- `SnapshotNotFound` — returned by `replace` when no snapshot exists yet for the stream
- `StoreError` — a technical failure from the underlying storage (database error, network error, etc); `cause` carries the original error for logging

You will never encounter `SnapshotAlreadyExists` or `SnapshotNotFound` in normal usage if you go through `takeSnapshot` — it handles the `create`/`replace` selection for you. These variants are part of the interface contract for implementors and direct callers.

---

### `InMemorySnapshotStore<State>`

A reference implementation of `SnapshotStore<State>` backed by an in-memory `Map`. Provided for testing and local development.

```ts
const snapshotStore = new InMemorySnapshotStore<CartState>()
```

**Not production-ready.** State is lost when the process exits. Use this in unit tests and integration tests in the same way you use `InMemoryEventStore` from core.

The `InMemorySnapshotStore` implements the full `SnapshotStore` contract and can be used to validate custom store implementations against a known-correct baseline.

---

### `loadAggregateWithSnapshot`

Loads an aggregate by combining the latest snapshot from a `SnapshotStore` with post-snapshot events from an `EventStore`. Falls back to full replay when no snapshot exists.

```ts
const result = await loadAggregateWithSnapshot({
  store,          // EventStore<Event>
  aggregate,      // AggregateDefinition<State, Event>
  streamId,       // string
  snapshotStore,  // SnapshotStore<State>
})
// Result<{ state: State; lastVersion: number }, CoreError | SnapshotError>
```

The return type is identical to core's `loadAggregate`. This means `loadAggregateWithSnapshot` is a drop-in replacement anywhere you currently call `loadAggregate` and want snapshot acceleration.

**Error handling:** The result error union is `CoreError | SnapshotError`. A `SnapshotError` means the snapshot store itself failed (e.g. database connection error); the event stream has not been touched in that case. A `CoreError` means the event store failed, or the stream did not exist.

```ts
const result = await loadAggregateWithSnapshot({ store, aggregate, streamId, snapshotStore })

if (!result.ok) {
  if (result.error.type === "StoreError") {
    // Infrastructure failure — log and surface as 503
  }
  if (result.error.type === "AggregateNotFound") {
    // The stream doesn't exist
  }
}
```

---

### `takeSnapshot`

Persists the current aggregate state as a snapshot. Automatically selects `create` or `replace` based on whether a snapshot already exists for the stream, and silently skips if the provided version is not newer than the existing one.

```ts
const result = await takeSnapshot({
  snapshotStore,
  streamId: "cart-1",
  snapshot: { state: result.state, version: result.lastVersion },
})
// Result<void, SnapshotError>
```

**Version safety:** `takeSnapshot` checks the current snapshot version before writing. If the incoming version is less than or equal to the stored version, the call is a no-op that returns `Ok(undefined)`. This makes it safe to call `takeSnapshot` concurrently or out of order without corrupting the snapshot with stale state.

```
existing snapshot: v50
takeSnapshot with v30  → no-op (Ok)
takeSnapshot with v50  → no-op (Ok)
takeSnapshot with v51  → replace (Ok)
takeSnapshot with v100 → replace (Ok)
```

You do not need to track whether a snapshot exists. Always call `takeSnapshot` and let it decide.

---

### `defineSnapshotCommand`

Binds an `AggregateDefinition`, a `CommandHandler`, and a `SnapshotStore` together into a reusable command object with snapshot-accelerated execution. This is the primary way to add snapshot support to an existing `@ts-event-sourcing/core` aggregate.

```ts
const addItem = defineSnapshotCommand({
  aggregate: cartAggregate,   // AggregateDefinition<State, Event>
  handler: addItemHandler,    // CommandHandler<State, Command, Event, Error>
  snapshotStore,              // SnapshotStore<State>
})

const result = await addItem.execute({
  store,           // EventStore<Event>
  streamId,        // string
  command,         // Command
  idempotencyKey,  // string
})
// Result<{ state: State; events: readonly Event[]; lastVersion: number }, Error | CoreError | SnapshotError>
```

The `execute` method has the same call signature as the object returned by core's `defineCommand`. Migrating consists of swapping the binding call; nothing at the call sites changes.

The error union on `execute` gains `SnapshotError` compared to core's `defineCommand`. Handle it at the same layer where you handle `CoreError` — it signals an infrastructure failure, not a domain rejection.

---

## Departures from Core

This library introduces three changes relative to `@ts-event-sourcing/core`. All are additive or purely in the extension layer; nothing in your domain code needs to change.

### 1. A separate `SnapshotStore` interface

Snapshots are stored in a dedicated `SnapshotStore<State>`, not in the `EventStore<E>`. The two interfaces are composed together by `loadAggregateWithSnapshot` and `defineSnapshotCommand`.

Storing snapshots as events was considered and rejected (see `docs/adr/0001-separate-snapshotstore-interface.md`). Snapshots are state blobs with different semantics from events — they don't have a `type` discriminant, aren't folded through reducers, and don't form an append-only log. Mixing them into the event store would blur the contract and leak snapshot awareness into infrastructure that doesn't need it.

In practice, a production adapter might implement both `EventStore<E>` and `SnapshotStore<State>` in the same class (backed by different tables in the same database), but they remain separate contracts at the type level.

### 2. `SnapshotError` joins the error union

Every function in this library that touches a `SnapshotStore` returns `Result<T, ... | SnapshotError>`. You must handle `SnapshotError` at the same layer where you handle `CoreError`. The addition is mechanical — if you pattern-match on `result.error.type`, add a `StoreError` case there.

### 3. `AggregateLoader` extension point in core

Core gained one backward-compatible change to support this library: `executeCommand` now accepts an optional `loader` parameter of type `AggregateLoader<State, Event, LoaderError>`. When provided, it replaces the default `store.load + rebuildAggregate` path. If omitted, behaviour is identical to before.

`defineSnapshotCommand` passes `loadAggregateWithSnapshot` as the loader automatically. If you call `executeCommand` directly and want snapshot acceleration, you can pass the loader yourself:

```ts
import { executeCommand } from "@ts-event-sourcing/core"
import { loadAggregateWithSnapshot } from "@ts-event-sourcing/snapshots"

await executeCommand({
  store,
  streamId,
  aggregate,
  command,
  idempotencyKey,
  handler,
  loader: (params) => loadAggregateWithSnapshot({ ...params, snapshotStore }),
})
```

For most cases, `defineSnapshotCommand` is the right choice. Use the `loader` parameter directly only if you're building your own command execution wrapper.

---

## Migrating from Core

Migrating an existing aggregate from `@ts-event-sourcing/core` to `@ts-event-sourcing/snapshots` requires three steps. No domain code changes.

### Step 1 — Install the package

```
npm install @ts-event-sourcing/snapshots
```

### Step 2 — Create a snapshot store

```ts
import { InMemorySnapshotStore } from "@ts-event-sourcing/snapshots"

// For tests:
const snapshotStore = new InMemorySnapshotStore<CartState>()

// For production — implement SnapshotStore<CartState> against your database
// const snapshotStore = new PostgresSnapshotStore<CartState>(pool, "cart_snapshots")
```

### Step 3 — Swap `defineCommand` for `defineSnapshotCommand`

```ts
// Before:
import { defineCommand } from "@ts-event-sourcing/core"
const addItem = defineCommand({ aggregate: cartAggregate, handler: addItemHandler })

// After:
import { defineSnapshotCommand } from "@ts-event-sourcing/snapshots"
const addItem = defineSnapshotCommand({
  aggregate: cartAggregate,
  handler: addItemHandler,
  snapshotStore,
})
```

All `execute` call sites are unchanged. The only new concern is deciding **when to take a snapshot**. A common strategy is to snapshot after every N events:

```ts
const result = unwrap(await addItem.execute({ store, streamId, command, idempotencyKey }))

if (result.lastVersion % 100 === 0) {
  await takeSnapshot({
    snapshotStore,
    streamId,
    snapshot: { state: result.state, version: result.lastVersion },
  })
}
```

Snapshots are optional. Aggregates with no snapshots load identically to core — `loadAggregateWithSnapshot` falls back to full replay when `snapshotStore.load` returns `null`.

---

## Snapshot Lifecycle

Understanding the lifecycle helps you reason about when to snapshot and what happens when things go wrong.

**Creating the first snapshot**

Call `takeSnapshot` after any command execution. There is no requirement to snapshot after the first event — you can snapshot at any point. Typically you'll start snapshotting once a stream has grown long enough that replay latency is noticeable.

**Replacing an existing snapshot**

Each stream has at most one active snapshot. `takeSnapshot` always writes the newest state — it does not accumulate a history of snapshots. If you need to keep previous checkpoints, use a naming convention in your storage keys (outside of this library's scope).

**Snapshot ahead of events**

If a snapshot is at version 50 but the event stream has only 30 events — for example, after a store migration — `loadAggregateWithSnapshot` trusts the snapshot state and folds zero events into it. The `lastVersion` returned reflects the event stream, not the snapshot. Document this behaviour if your team operates mixed-version stores.

**Snapshot regression (stale version)**

`takeSnapshot` silently skips writes where the provided version is equal to or lower than the existing snapshot version. This makes concurrent calls safe without requiring distributed locking.

**No snapshot exists**

`loadAggregateWithSnapshot` falls back to a full replay. This is always correct. Snapshots are an optimisation layered on top of an already-correct system.

---

## Implementing a Production SnapshotStore

For production use, implement the `SnapshotStore<State>` interface against your storage backend. The interface is simple: three methods, all returning `Promise<Result<T, SnapshotError>>`.

A PostgreSQL implementation backed by a single snapshot table is a natural fit:

```ts
import type { SnapshotStore, Snapshot, SnapshotError } from "@ts-event-sourcing/snapshots"
import type { Result } from "@ts-event-sourcing/core"
import { Ok, Err } from "@ts-event-sourcing/core"

export class PostgresSnapshotStore<State> implements SnapshotStore<State> {
  constructor(
    private readonly pool: Pool,
    private readonly table: string,
  ) {}

  async create(params: {
    streamId: string
    snapshot: Omit<Snapshot<State>, "streamId">
  }): Promise<Result<void, SnapshotError>> {
    try {
      await this.pool.query(
        `INSERT INTO ${this.table} (stream_id, version, state)
         VALUES ($1, $2, $3)`,
        [params.streamId, params.snapshot.version, JSON.stringify(params.snapshot.state)],
      )
      return Ok(undefined)
    } catch (e: any) {
      if (e.code === "23505") {
        return Err({ type: "SnapshotAlreadyExists" })
      }
      return Err({ type: "StoreError", cause: e })
    }
  }

  async replace(params: {
    streamId: string
    snapshot: Omit<Snapshot<State>, "streamId">
  }): Promise<Result<void, SnapshotError>> {
    try {
      const result = await this.pool.query(
        `UPDATE ${this.table} SET version = $2, state = $3
         WHERE stream_id = $1`,
        [params.streamId, params.snapshot.version, JSON.stringify(params.snapshot.state)],
      )
      if (result.rowCount === 0) {
        return Err({ type: "SnapshotNotFound" })
      }
      return Ok(undefined)
    } catch (e) {
      return Err({ type: "StoreError", cause: e })
    }
  }

  async load(params: { streamId: string }): Promise<Result<Snapshot<State> | null, SnapshotError>> {
    try {
      const result = await this.pool.query(
        `SELECT stream_id, version, state FROM ${this.table} WHERE stream_id = $1`,
        [params.streamId],
      )
      if (result.rows.length === 0) return Ok(null)
      const row = result.rows[0]
      return Ok({ streamId: row.stream_id, version: row.version, state: row.state as State })
    } catch (e) {
      return Err({ type: "StoreError", cause: e })
    }
  }
}
```

A few things worth noting about this pattern:

- `create` uses an `INSERT` and maps a unique-constraint violation (`23505`) to `SnapshotAlreadyExists`. This keeps the semantics tight.
- `replace` uses an `UPDATE` and checks `rowCount` to detect the not-found case without an additional `SELECT`.
- State is serialized as JSON. You are responsible for deserialization (e.g. reviving `Date` objects) — this library stores and returns whatever you pass in.
- All exceptions are caught and wrapped in `StoreError`. Nothing escapes as a thrown exception.

---

## Testing Strategy

Because domain code is unchanged, your existing unit tests for command handlers require no modification. Add snapshot-specific tests at the integration level.

**Unit testing snapshot logic** — test `takeSnapshot` and `loadAggregateWithSnapshot` with `InMemorySnapshotStore`. Verify the snapshot is taken at the right version and that the loaded state matches a full replay:

```ts
import { describe, it, expect } from "vitest"
import { InMemoryEventStore, createAggregate, loadAggregate, unwrap } from "@ts-event-sourcing/core"
import { InMemorySnapshotStore, defineSnapshotCommand, takeSnapshot, loadAggregateWithSnapshot } from "@ts-event-sourcing/snapshots"

describe("cart snapshot", () => {
  it("loadAggregateWithSnapshot matches full replay", async () => {
    const store = new InMemoryEventStore<CartEvent>()
    const snapshotStore = new InMemorySnapshotStore<CartState>()

    await createAggregate({ store, streamId: "cart-1", events: [], idempotencyKey: "open" })

    // Execute several commands and take a snapshot mid-stream
    const addItem = defineSnapshotCommand({ aggregate: cartAggregate, handler: addItemHandler, snapshotStore })

    for (let i = 0; i < 5; i++) {
      const r = unwrap(await addItem.execute({ store, streamId: "cart-1", command: { itemId: `item-${i}` }, idempotencyKey: `add-${i}` }))
      if (r.lastVersion === 3) {
        await takeSnapshot({ snapshotStore, streamId: "cart-1", snapshot: { state: r.state, version: r.lastVersion } })
      }
    }

    const full = unwrap(await loadAggregate({ store, streamId: "cart-1", aggregate: cartAggregate }))
    const snap = unwrap(await loadAggregateWithSnapshot({ store, aggregate: cartAggregate, streamId: "cart-1", snapshotStore }))

    expect(snap.state).toEqual(full.state)
    expect(snap.lastVersion).toBe(full.lastVersion)
  })
})
```

**Testing a custom `SnapshotStore` implementation** — the `InMemorySnapshotStore` provides a reference you can test your implementation against. Run both through the same test suite to verify contract compliance:

```ts
function snapshotStoreContractTests(factory: () => SnapshotStore<{ count: number }>) {
  const snapshot = { version: 5, state: { count: 42 } }

  it("creates and loads a snapshot", async () => {
    const store = factory()
    await store.create({ streamId: "s1", snapshot })
    const result = unwrap(await store.load({ streamId: "s1" }))
    expect(result?.version).toBe(5)
    expect(result?.state.count).toBe(42)
  })

  it("returns null when no snapshot exists", async () => {
    const store = factory()
    const result = unwrap(await store.load({ streamId: "nonexistent" }))
    expect(result).toBeNull()
  })

  it("fails to create when snapshot already exists", async () => {
    const store = factory()
    await store.create({ streamId: "s1", snapshot })
    const result = await store.create({ streamId: "s1", snapshot })
    expect(result.ok).toBe(false)
    expect(!result.ok && result.error.type).toBe("SnapshotAlreadyExists")
  })

  it("replaces an existing snapshot", async () => {
    const store = factory()
    await store.create({ streamId: "s1", snapshot })
    await store.replace({ streamId: "s1", snapshot: { version: 10, state: { count: 99 } } })
    const result = unwrap(await store.load({ streamId: "s1" }))
    expect(result?.version).toBe(10)
  })

  it("fails to replace when no snapshot exists", async () => {
    const store = factory()
    const result = await store.replace({ streamId: "s1", snapshot })
    expect(result.ok).toBe(false)
    expect(!result.ok && result.error.type).toBe("SnapshotNotFound")
  })
}

describe("InMemorySnapshotStore", () => snapshotStoreContractTests(() => new InMemorySnapshotStore()))
describe("PostgresSnapshotStore", () => snapshotStoreContractTests(() => new PostgresSnapshotStore(pool, "test_snapshots")))
```

---

## Pitfalls & Best Practices

### ✗ Don't snapshot before `createAggregate`

A snapshot at version 0 on a stream that doesn't exist yet has no meaning. Always call `createAggregate` first, then execute at least one command before taking a snapshot.

---

### ✗ Don't rely on snapshots as the source of truth

If your snapshot store becomes unavailable or its data is lost, `loadAggregateWithSnapshot` will return a `StoreError`. Your recovery path is to fall back to `loadAggregate` from core and replay from the event store — which is always correct. Design your error handling to allow this fallback.

---

### ✗ Don't snapshot at every command

Snapshotting after every command adds write overhead that negates the read savings for anything but the longest-lived streams. A common interval is every 50–200 events. Tune this based on your aggregate's event rate and acceptable load latency.

---

### ✓ Let `takeSnapshot` manage `create` vs `replace`

Never call `snapshotStore.create` or `snapshotStore.replace` directly in application code. `takeSnapshot` handles the selection correctly and adds version-safety semantics. Calling the store methods directly bypasses this protection.

---

### ✓ Handle `SnapshotError` at the same layer as `CoreError`

Both are infrastructure failures. Map them to HTTP 503 or equivalent, log the `cause`, and surface them as retryable errors. They are distinct from domain rejections, which live in your aggregate's own error type.

```ts
if (!result.ok) {
  const err = result.error
  if ("type" in err) {
    switch (err.type) {
      case "AlreadyCheckedOut":  return res.status(409).json({ error: "Cart is checked out" })
      case "AggregateNotFound":  return res.status(404).json({ error: "Cart not found" })
      case "StoreError":         return res.status(503).json({ error: "Service unavailable" })
      case "ConcurrencyConflict": return res.status(409).json({ error: "Retry your request" })
    }
  }
}
```

---

### ✓ Serialize `Date` fields explicitly in production stores

This library stores whatever state you pass in. If your aggregate state contains `Date` objects, your `SnapshotStore` implementation must handle serialization and deserialization — JSON.stringify/parse will silently turn `Date` into a string. Use a reviver or serialize to ISO strings explicitly.

---

### ✓ Use a single `SnapshotStore` instance per aggregate type

Bind the `snapshotStore` once and share it across all `defineSnapshotCommand` calls for the same aggregate. Creating a new instance per command resets the in-memory state in tests, and creates redundant connections in production.

---

## Out of Scope

The following are intentionally not part of this library:

- Snapshot frequency policies (auto-snapshot every N events)
- Snapshot pruning or deletion
- Snapshot history (multiple checkpoints per stream)
- Snapshot compression or custom serialization
- Production store adapters (Postgres, Redis, DynamoDB) — these are separate packages
- Push-based snapshot invalidation
- Migration between snapshot state shapes

These concerns belong in adapters and higher-level tooling built on top of this extension. The public API surface is intentionally small: six exports, zero runtime dependencies beyond core.

---

## Design Philosophy

The same principles that shape `@ts-event-sourcing/core` apply here.

**Snapshots are an optimization, not a primitive.** The event store is the source of truth. A snapshot at version 500 is only meaningful because the 500 events that produced it are still in the store. This library never treats snapshots as authoritative — they are always combined with the event log, not substituted for it.

**Infrastructure agnosticism.** The `SnapshotStore` interface is the only bridge between the library and your database. No specific backend is required or preferred. A Redis snapshot store and a Postgres snapshot store implement the same three-method interface.

**No auto-snapshot policies.** When to snapshot is a decision that depends on your aggregate's event rate, your store's write performance, and your acceptable load latency. This library provides the mechanism; you provide the policy.

**The error model doesn't change shape.** `SnapshotError` follows the same discriminated union pattern as `CoreError`. Adding snapshot support doesn't require learning a new error handling idiom — it's the same `result.error.type` switch you already write.

**Domain code is untouched.** Aggregates, reducers, and command handlers know nothing about snapshots. Snapshot acceleration is a concern of the execution and loading layer, not the domain. You can add or remove snapshot support without changing a single line of business logic.
