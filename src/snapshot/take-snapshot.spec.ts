import { Ok } from "@ts-event-sourcing/core";
import { describe, expect, it, vi } from "vitest";
import { takeSnapshot } from "./take-snapshot";

describe("takeSnapshot", () => {
	it("creates a snapshot when none exists", async () => {
		const snapshotStore = {
			create: vi.fn().mockResolvedValue(Ok(undefined)),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(null)),
		};

		const result = await takeSnapshot({
			snapshotStore,
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});

		expect(result.ok).toBe(true);
		expect(snapshotStore.create).toHaveBeenCalledWith({
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});
		expect(snapshotStore.replace).not.toHaveBeenCalled();
	});

	it("replaces when existing snapshot has a lower version", async () => {
		const existingSnapshot = {
			streamId: "s1",
			version: 3,
			state: { count: 10 },
		};
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn().mockResolvedValue(Ok(undefined)),
			load: vi.fn().mockResolvedValue(Ok(existingSnapshot)),
		};

		const result = await takeSnapshot({
			snapshotStore,
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});

		expect(result.ok).toBe(true);
		expect(snapshotStore.replace).toHaveBeenCalledWith({
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});
		expect(snapshotStore.create).not.toHaveBeenCalled();
	});

	it("silently skips when new version is same as existing", async () => {
		const existingSnapshot = {
			streamId: "s1",
			version: 5,
			state: { count: 10 },
		};
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(existingSnapshot)),
		};

		const result = await takeSnapshot({
			snapshotStore,
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});

		expect(result.ok).toBe(true);
		expect(snapshotStore.create).not.toHaveBeenCalled();
		expect(snapshotStore.replace).not.toHaveBeenCalled();
	});

	it("silently skips when new version is lower than existing", async () => {
		const existingSnapshot = {
			streamId: "s1",
			version: 10,
			state: { count: 10 },
		};
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(existingSnapshot)),
		};

		const result = await takeSnapshot({
			snapshotStore,
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});

		expect(result.ok).toBe(true);
		expect(snapshotStore.create).not.toHaveBeenCalled();
		expect(snapshotStore.replace).not.toHaveBeenCalled();
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

		const result = await takeSnapshot({
			snapshotStore,
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("StoreError");
	});

	it("propagates errors from create", async () => {
		const snapshotStore = {
			create: vi.fn().mockResolvedValue({
				ok: false,
				error: { type: "SnapshotAlreadyExists" },
			}),
			replace: vi.fn(),
			load: vi.fn().mockResolvedValue(Ok(null)),
		};

		const result = await takeSnapshot({
			snapshotStore,
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("SnapshotAlreadyExists");
	});

	it("propagates errors from replace", async () => {
		const existingSnapshot = {
			streamId: "s1",
			version: 3,
			state: { count: 10 },
		};
		const snapshotStore = {
			create: vi.fn(),
			replace: vi.fn().mockResolvedValue({
				ok: false,
				error: { type: "StoreError", cause: "disk full" },
			}),
			load: vi.fn().mockResolvedValue(Ok(existingSnapshot)),
		};

		const result = await takeSnapshot({
			snapshotStore,
			streamId: "stream-1",
			snapshot: { state: { count: 42 }, version: 5 },
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error.type).toBe("StoreError");
	});
});
