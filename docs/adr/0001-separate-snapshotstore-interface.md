# ADR 0001: Separate SnapshotStore Interface

## Status

Accepted

## Context

The `@ts-event-sourcing/snapshots` extension needs a place to persist aggregate state snapshots. Two options were considered:

1. **Extend the `EventStore<E>` interface** — store snapshots as a special kind of event within the same storage contract. The store's `load` would need snapshot-awareness to skip events before a snapshot version.

2. **Define a separate `SnapshotStore<State>` interface** — a distinct contract with its own `create`, `replace`, and `load` operations. The `EventStore` stays unchanged.

## Decision

**We chose option 2: a separate `SnapshotStore<State>` interface.**

## Rationale

- **Semantic clarity.** Snapshots are state blobs, not events. They don't have a `type` discriminant, aren't folded through reducers, and don't form an append-only log. Treating them as events would blur the contract.
- **Separation of concerns.** The `EventStore` is the source of truth; the `SnapshotStore` is a performance optimization. Making them separate keeps each contract honest and testable in isolation.
- **Composition over inheritance.** A caller composes `EventStore<E>` + `SnapshotStore<S>` together. They can be backed by different infrastructure (e.g., events in Postgres, snapshots in Redis) or the same database with different tables. The library doesn't force colocation.
- **Core stays unchanged.** The core's `EventStore` interface remains minimal. No new methods, no new error types, no snapshot awareness leaking into the persistence boundary.

## Consequences

- The snapshot extension carries its own error type (`SnapshotError`) and interface (`SnapshotStore`), increasing the total type surface.
- A production store adapter (e.g., Postgres) may implement both `EventStore` and `SnapshotStore` in the same class, but they remain separate contracts at the type level.
- The `AggregateLoader` type needs to wire both stores together, but that's the snapshot package's responsibility — not core's.
