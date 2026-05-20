import { describe, expect, it } from "vitest";
import { InMemorySnapshotStore } from "./in-memory-snapshot-store";

describe("InMemorySnapshotStore", () => {
	const snapshot = { version: 5, state: { count: 42 } } as const;

	it("creates a snapshot and loads it back", async () => {
		const store = new InMemorySnapshotStore<{ count: number }>();

		const createResult = await store.create({
			streamId: "stream-1",
			snapshot,
		});

		expect(createResult.ok).toBe(true);

		const loadResult = await store.load({ streamId: "stream-1" });

		expect(loadResult.ok).toBe(true);
		if (!loadResult.ok) throw new Error();

		expect(loadResult.value).not.toBeNull();
		expect(loadResult.value?.streamId).toBe("stream-1");
		expect(loadResult.value?.version).toBe(5);
		expect(loadResult.value?.state.count).toBe(42);
	});

	it("fails to create when a snapshot already exists for the stream", async () => {
		const store = new InMemorySnapshotStore<{ count: number }>();

		await store.create({ streamId: "stream-1", snapshot });

		const result = await store.create({ streamId: "stream-1", snapshot });

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("SnapshotAlreadyExists");
	});

	it("replaces an existing snapshot and reflects the new state", async () => {
		const store = new InMemorySnapshotStore<{ count: number }>();

		await store.create({ streamId: "stream-1", snapshot });

		const replaceResult = await store.replace({
			streamId: "stream-1",
			snapshot: { version: 10, state: { count: 99 } },
		});

		expect(replaceResult.ok).toBe(true);

		const loadResult = await store.load({ streamId: "stream-1" });
		if (!loadResult.ok) throw new Error();
		expect(loadResult.value?.version).toBe(10);
		expect(loadResult.value?.state.count).toBe(99);
	});

	it("fails to replace when no snapshot exists for the stream", async () => {
		const store = new InMemorySnapshotStore<{ count: number }>();

		const result = await store.replace({
			streamId: "stream-1",
			snapshot,
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("SnapshotNotFound");
	});

	it("returns null when loading a stream with no snapshot", async () => {
		const store = new InMemorySnapshotStore<{ count: number }>();

		const result = await store.load({ streamId: "nonexistent" });

		expect(result.ok).toBe(true);
		if (!result.ok) throw new Error();
		expect(result.value).toBeNull();
	});
});
