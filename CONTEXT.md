# Snapshot Extension — Glossary

## Snapshot

A persisted copy of an aggregate's **state** at a specific event **version**. Snapshots are an optimization — they accelerate aggregate rebuilds by avoiding full event replay. The source of truth is always the event store; a snapshot is derived and discardable.

## Snapshot Store (`SnapshotStore<State>`)

The persistence contract for snapshots. Defines three operations:

- **`create`** — persists a snapshot for a stream. Fails with `SnapshotAlreadyExists` if a snapshot already exists for that stream.
- **`replace`** — overwrites an existing snapshot. Fails with `SnapshotNotFound` if no snapshot exists.
- **`load`** — retrieves the latest snapshot for a stream, or `null` if none exists.

`create` and `replace` are kept separate (following core's explicit-lifecycle pattern) rather than fused into a single upsert operation.

## Snapshot Error (`SnapshotError`)

A discriminated union of snapshot-specific technical failures:

- `SnapshotAlreadyExists` — `create` called when a snapshot already exists for the stream.
- `SnapshotNotFound` — `replace` called when no snapshot exists.
- `StoreError` — infrastructure failure in the snapshot storage layer.

These errors are separate from core's `CoreError`; the two are unioned at composition points.

## Snapshot-Aware Loader (`loadAggregateWithSnapshot`)

A function that loads an aggregate by combining the latest snapshot from a `SnapshotStore` with post-snapshot events from an `EventStore`. Falls back to full event replay if no snapshot exists. Returns the same type as core's `loadAggregate`.

## Post-Snapshot Events

Events that occurred **after** the snapshot version. When a snapshot exists at version N, post-snapshot events are events N+1 through the stream's current `lastVersion`. These events are folded incrementally into the snapshot state to produce the current aggregate state.

## takeSnapshot

A function that persists the caller's aggregate state and version to a `SnapshotStore`. Automatically chooses `create` (if no snapshot exists) or `replace` (if one does and the new version is higher). Returns `SnapshotAlreadyExists` only on a `create` collision where the version is not higher — in which case the operation is silently skipped.

## defineSnapshotCommand

An ergonomic wrapper that binds an `AggregateDefinition`, `CommandHandler`, and `SnapshotStore` into a single executable unit. Its `execute` method internally calls core's `executeCommand` with `loadAggregateWithSnapshot` as the loader. Mirrors core's `defineCommand` pattern.

## Aggregate Loader (`AggregateLoader<State, Event, LoaderError>`)

A core type representing a pluggable aggregate loading strategy. Passed to `executeCommand`'s optional `loader` parameter. The snapshot extension's `loadAggregateWithSnapshot` implements this type with `LoaderError = CoreError | SnapshotError`. Default `LoaderError = never` preserves the existing error union when no loader is provided.
