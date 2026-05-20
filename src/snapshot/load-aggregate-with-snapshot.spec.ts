import { type EventStore, loadAggregate, Ok } from "@ts-event-sourcing/core";
import { describe, expect, it, vi } from "vitest";
import { loadAggregateWithSnapshot } from "./load-aggregate-with-snapshot";

const aggregate = {
	initialState: { count: 0 },
	reduce: (state: { count: number }, _event: { type: "INC" }) => ({
		count: state.count + 1,
	}),
};

describe("loadAggregateWithSnapshot", () => {
	it("loads using snapshot and folds only post-snapshot events", async () => {
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi
				.fn()
				.mockResolvedValue(
					Ok({ streamId: "stream-1", version: 5, state: { count: 10 } }),
				),
		};

		const store = {
			load: vi.fn().mockResolvedValue(
				Ok({
					type: "loaded",
					events: [
						{ type: "INC" },
						{ type: "INC" },
						{ type: "INC" },
						{ type: "INC" },
						{ type: "INC" },
						{ type: "INC" },
						{ type: "INC" },
					],
					lastVersion: 7,
				}),
			),
			append: vi.fn(),
		};

		const result = await loadAggregateWithSnapshot({
			store,
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(result.ok).toBe(true);
		if (!result.ok) throw new Error();
		expect(result.value.state.count).toBe(12);
		expect(result.value.lastVersion).toBe(7);
	});

	it("falls back to full replay when no snapshot exists", async () => {
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(null)),
		};

		const store = {
			load: vi.fn().mockResolvedValue(
				Ok({
					type: "loaded",
					events: [{ type: "INC" }, { type: "INC" }, { type: "INC" }],
					lastVersion: 3,
				}),
			),
			append: vi.fn(),
		};

		const result = await loadAggregateWithSnapshot({
			store,
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(result.ok).toBe(true);
		if (!result.ok) throw new Error();
		expect(result.value.state.count).toBe(3);
		expect(result.value.lastVersion).toBe(3);
	});

	it("propagates snapshot store errors", async () => {
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue({
				ok: false,
				error: { type: "StoreError", cause: "db down" },
			}),
		};

		const result = await loadAggregateWithSnapshot({
			store: { load: vi.fn(), append: vi.fn() },
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("StoreError");
	});

	it("propagates event store errors", async () => {
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(null)),
		};

		const store = {
			load: vi.fn().mockResolvedValue({
				ok: false,
				error: { type: "StoreError", cause: "db down" },
			}),
			append: vi.fn(),
		};

		const result = await loadAggregateWithSnapshot({
			store,
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("StoreError");
	});

	it("returns AggregateNotFound when stream is empty", async () => {
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(null)),
		};

		const store = {
			load: vi
				.fn()
				.mockResolvedValue(Ok({ type: "empty", lastVersion: 0, events: [] })),
			append: vi.fn(),
		};

		const result = await loadAggregateWithSnapshot({
			store,
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("AggregateNotFound");
	});

	it("returns AggregateNotFound when snapshot exists but event stream is empty", async () => {
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi
				.fn()
				.mockResolvedValue(
					Ok({ streamId: "stream-1", version: 5, state: { count: 10 } }),
				),
		};

		const store = {
			load: vi
				.fn()
				.mockResolvedValue(Ok({ type: "empty", lastVersion: 0, events: [] })),
			append: vi.fn(),
		};

		const result = await loadAggregateWithSnapshot({
			store,
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("AggregateNotFound");
	});

	it("returns identical state and version as loadAggregate with a snapshot", async () => {
		const events = [
			{ type: "INC" },
			{ type: "INC" },
			{ type: "INC" },
			{ type: "INC" },
			{ type: "INC" },
			{ type: "INC" },
			{ type: "INC" },
		];

		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi
				.fn()
				.mockResolvedValue(
					Ok({ streamId: "stream-1", version: 5, state: { count: 5 } }),
				),
		};

		// loadAggregate does not use snapshotStore
		const aggregateResult = await loadAggregate({
			store: {
				load: vi
					.fn()
					.mockResolvedValue(Ok({ type: "loaded", events, lastVersion: 7 })),
			} as unknown as EventStore<{
				type: "INC";
			}>,
			streamId: "stream-1",
			aggregate,
		});

		const snapshotResult = await loadAggregateWithSnapshot({
			store: {
				load: vi
					.fn()
					.mockResolvedValue(Ok({ type: "loaded", events, lastVersion: 7 })),
				append: vi.fn(),
			},
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(aggregateResult.ok).toBe(true);
		expect(snapshotResult.ok).toBe(true);
		if (!aggregateResult.ok || !snapshotResult.ok) throw new Error();

		expect(snapshotResult.value.state).toEqual(aggregateResult.value.state);
		expect(snapshotResult.value.lastVersion).toBe(
			aggregateResult.value.lastVersion,
		);
	});

	it("returns identical state and version as loadAggregate without a snapshot", async () => {
		const events = [{ type: "INC" }, { type: "INC" }, { type: "INC" }];

		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(null)),
		};

		const aggregateResult = await loadAggregate({
			store: {
				load: vi
					.fn()
					.mockResolvedValue(Ok({ type: "loaded", events, lastVersion: 3 })),
			} as unknown as EventStore<{
				type: "INC";
			}>,
			streamId: "stream-1",
			aggregate,
		});

		const snapshotResult = await loadAggregateWithSnapshot({
			store: {
				load: vi
					.fn()
					.mockResolvedValue(Ok({ type: "loaded", events, lastVersion: 3 })),
				append: vi.fn(),
			},
			aggregate,
			streamId: "stream-1",
			snapshotStore,
		});

		expect(aggregateResult.ok).toBe(true);
		expect(snapshotResult.ok).toBe(true);
		if (!aggregateResult.ok || !snapshotResult.ok) throw new Error();

		expect(snapshotResult.value.state).toEqual(aggregateResult.value.state);
		expect(snapshotResult.value.lastVersion).toBe(
			aggregateResult.value.lastVersion,
		);
	});
});
