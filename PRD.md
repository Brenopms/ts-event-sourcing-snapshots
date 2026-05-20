# Product Requirements Document: Snapshot Extension for @ts-event-sourcing/core

## 1. Executive Summary

The **Snapshot Extension** (`@ts-event-sourcing/snapshots`) is a companion library for `@ts-event-sourcing/core` that accelerates aggregate rebuilds for long-lived streams. It introduces a `SnapshotStore` interface for persisting aggregate state at a given event version, a snapshot-aware aggregate loader that avoids full event replay, and ergonomic wrappers for taking snapshots and executing snapshot-accelerated commands.

The extension follows the same design principles as core: **zero runtime dependencies beyond core**, **Result-based error model**, **compile-time type safety**, and **minimal surface area**. Snapshots remain an optimization — the source of truth is always the event store.

## 2. Problem Statement

Event-sourced aggregates rebuild state by folding every event from the beginning of the stream. For long-lived aggregates (thousands or millions of events), this replay becomes expensive on every command execution or load. The core library provides no built-in acceleration for this.

Users need a **standard, type-safe snapshot extension** that:

- Stores aggregate state at periodic event versions
- Loads aggregates by combining the latest snapshot with only the events that occurred after it
- Composes cleanly with existing `EventStore` implementations
- Doesn't force a specific snapshot frequency or storage backend

## 3. Target Users

- **Backend engineers** using `@ts-event-sourcing/core` with aggregates that accumulate many events
- **Library authors** building production event store adapters that want snapshot support
- **Teams that value** the core's minimalist philosophy and want snapshots as a separable concern

## 4. Goals (Functional Requirements)

### 4.1 Snapshot Storage Contract

- Provide **`SnapshotStore<State>`** — an interface with `create`, `replace`, and `load` operations. Separate from `EventStore<E>`. Snapshots are stored as opaque state blobs indexed by stream ID and version.
- Provide **`SnapshotError`** — a discriminated union of `SnapshotAlreadyExists`, `SnapshotNotFound`, and `StoreError`.

### 4.2 Snapshot-Aware Aggregate Loading

- Provide **`loadAggregateWithSnapshot`** — a function that loads an aggregate by combining the latest snapshot from a `SnapshotStore` with post-snapshot events from an `EventStore`. Falls back to full event replay if no snapshot exists. Returns the same type as core's `loadAggregate`.

### 4.3 Snapshot Creation

- Provide **`takeSnapshot`** — a function that persists the caller's aggregate state and version to a `SnapshotStore`. Automatically selects `create` or `replace` based on whether a snapshot already exists.

### 4.4 Ergonomic Command Execution

- Provide **`defineSnapshotCommand`** — a wrapper that binds an `AggregateDefinition`, `CommandHandler`, and `SnapshotStore` together, exposing an `execute` method that leverages `loadAggregateWithSnapshot` internally. Mirrors core's `defineCommand` pattern.

### 4.5 Reference Implementation

- Provide **`InMemorySnapshotStore<State>`** — an in-memory implementation of `SnapshotStore` for testing and local development (not production-ready).

### 4.6 Changes to Core

The core library gains one backward-compatible change to support the snapshot extension:

- **`AggregateLoader<State, Event, LoaderError>`** — a new type representing a pluggable aggregate loading strategy. Default `LoaderError = never`.
- **`executeCommand` optional `loader` parameter** — when provided, delegates aggregate loading to the loader instead of the default `store.load` + `rebuildAggregate` path. Post-append rebuild uses an incremental fold rather than a full replay.

## 5. Non-Functional Requirements

### 5.1 Determinism

- `loadAggregateWithSnapshot` must return the same state as `loadAggregate` for any given stream, regardless of whether a snapshot was used.
- Snapshot creation does not mutate the event stream.

### 5.2 Type Safety

- All public APIs are strictly typed with generic parameters for state, event, and error types.
- The error union of `loadAggregateWithSnapshot` and `defineSnapshotCommand.execute` must include both `CoreError` (from the event store) and `SnapshotError`.

### 5.3 Minimalism

- The public API surface consists of 6 exports: `SnapshotStore`, `SnapshotError`, `InMemorySnapshotStore`, `loadAggregateWithSnapshot`, `takeSnapshot`, `defineSnapshotCommand`.
- Zero runtime dependencies beyond `@ts-event-sourcing/core` (peer dependency).
- No decorators, reflection, or code generation.

### 5.4 Infrastructure Agnosticism

- The `SnapshotStore` interface is the only bridge to snapshot storage.
- No built-in adapters for databases. Production stores are separate packages.
- No auto-snapshot policies (frequency, triggers); these are the caller's responsibility.

### 5.5 Error Model

- All public functions return `Result<T, E>`. No exceptions cross the library boundary.
- Errors are categorized: `CoreError` for event store / replay failures, `SnapshotError` for snapshot storage failures, and user-defined domain errors.

## 6. Public API

### Snapshot Store Contract

- `SnapshotStore<State>` — interface (`create`, `replace`, `load`)
- `SnapshotError` — discriminated union (`SnapshotAlreadyExists`, `SnapshotNotFound`, `StoreError`)

### Aggregate Loading

- `loadAggregateWithSnapshot<S, E>(params)` — async, returns `Result<{ state: S; lastVersion: number }, CoreError | SnapshotError>`

### Snapshot Creation

- `takeSnapshot<S>(params)` — async, returns `Result<void, SnapshotError>`

### Command Execution

- `defineSnapshotCommand<S, C, E, Err>(params)` — returns `{ execute }` with `SnapshotError` in the error union

### Reference Implementation

- `InMemorySnapshotStore<S>` — class implementing `SnapshotStore<S>`

## 7. Changes to Core

### New Type

- `AggregateLoader<State, Event extends AnyEvent, LoaderError = never>` — function type: `(params: { store, aggregate, streamId }) => Promise<Result<{ state, lastVersion }, LoaderError>>`

### Updated Function Signature

- `executeCommand` gains an optional `loader?: AggregateLoader<State, Event, LoaderError>` parameter. When provided, the loader replaces steps 1–2 (load + rebuild). The post-append rebuild (step 5) switches from `rebuildAggregate` (full replay) to `fold` (incremental fold of new events into the loader's returned state). No behavior changes when `loader` is omitted.

## 8. Out of Scope (Explicitly)

- Snapshot frequency policies (auto-snapshot-every-N-events)
- Snapshot pruning or deletion
- Snapshot compression or serialization strategies
- Production snapshot storage adapters (separate packages)
- Push-based snapshot invalidation or subscription
- Migration between snapshot formats

## 9. Example User Journey

```ts
import { createAggregate, executeCommand, InMemoryEventStore } from "@ts-event-sourcing/core";
import {
  InMemorySnapshotStore,
  defineSnapshotCommand,
  takeSnapshot,
} from "@ts-event-sourcing/snapshots";

// 1. Define domain types
type CartEvent = { type: "ItemAdded"; itemId: string } | { type: "CheckedOut" };
type CartState = { items: string[]; checkedOut: boolean };
type AddItemCommand = { itemId: string };
type CartError = "ALREADY_CHECKED_OUT";

const cartAggregate = {
  initialState: { items: [], checkedOut: false } as CartState,
  reduce: (state: CartState, event: CartEvent): CartState => {
    switch (event.type) {
      case "ItemAdded": return { ...state, items: [...state.items, event.itemId] };
      case "CheckedOut": return { ...state, checkedOut: true };
    }
  },
};

const addItemHandler = ({ state, command }) => {
  if (state.checkedOut) return Err("ALREADY_CHECKED_OUT");
  return Ok([{ type: "ItemAdded" as const, itemId: command.itemId }]);
};

// 2. Set up stores
const store = new InMemoryEventStore<CartEvent>();
const snapshotStore = new InMemorySnapshotStore<CartState>();

// 3. Create the aggregate and bind a snapshot-accelerated command
await createAggregate({ store, streamId: "cart-1", events: [], idempotencyKey: "create" });

const addItem = defineSnapshotCommand({
  aggregate: cartAggregate,
  handler: addItemHandler,
  snapshotStore,
});

// 4. Execute many commands, periodically taking snapshots
const result = await addItem.execute({
  store,
  streamId: "cart-1",
  command: { itemId: "apple" },
  idempotencyKey: "cmd-1",
});

if (result.ok) {
  // Take a snapshot every 100 events
  if (result.value.lastVersion % 100 === 0) {
    await takeSnapshot({
      snapshotStore,
      streamId: "cart-1",
      snapshot: { state: result.value.state, version: result.value.lastVersion },
    });
  }
}
```

## 10. Success Criteria

1. A developer can add snapshot support to an existing `@ts-event-sourcing/core` aggregate without changing any domain code.
2. `loadAggregateWithSnapshot` returns the same `{ state, lastVersion }` as `loadAggregate` for any stream and snapshot configuration.
3. `takeSnapshot` correctly chooses between `create` and `replace` without the caller needing to know whether a snapshot already exists.
4. `defineSnapshotCommand` has the same ergonomic shape as core's `defineCommand`.
5. The `InMemorySnapshotStore` passes the same test suite as any production snapshot store adapter.
6. No exceptions cross the snapshot library boundary; all failures are typed `Result`.

## 11. Risks & Mitigations

| Risk | Mitigation |
|------|-------------|
| Snapshots get ahead of events (e.g., snapshot at v50 but event stream has only 30 events due to store migration). | `loadAggregateWithSnapshot` trusts the snapshot state and folds zero events. Document this behavior. |
| `takeSnapshot` regresses to a lower version (caller passes v30 when a v50 snapshot exists). | `takeSnapshot` only calls `replace` if the new version exceeds the existing one. Older versions are silently skipped. |
| Long streams still load all events even with a snapshot (no `fromVersion` on `EventStore.load`). | The loader slices events by index after loading. Waste is bounded by snapshot frequency (e.g., <1000 events). A future `fromVersion` store parameter can optimize this. |
| Users call `executeCommand` directly without a loader, missing the snapshot acceleration. | `defineSnapshotCommand` wraps it correctly. Power users can pass the loader to `executeCommand` directly. |

## 12. Dependencies

- **Peer**: `@ts-event-sourcing/core` >= 1.0.1
- **Runtime**: None beyond core
- **Dev**: `vitest`, `typescript`, `tsdown`, `@biomejs/biome` (same toolchain as core)

## 13. Future Possibilities (Not Committed)

- Production-grade snapshot stores (PostgreSQL snapshot table)
- `fromVersion` parameter on `EventStore.load` to optimize post-snapshot loading
- Snapshot strategy helpers (e.g., `snapshotEvery(store, snapshotStore, n)`)
- Snapshot pruning utilities
- Property-based tests verifying snapshot correctness against full replay
