import { Err, Ok } from "@ts-event-sourcing/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { InMemorySnapshotStore } from "../in-memory-snapshot-store/in-memory-snapshot-store";
import { defineSnapshotCommand } from "./define-snapshot-command";

const aggregate = {
	initialState: { count: 0 },
	reduce: (state: { count: number }, _event: { type: "INC" }) => ({
		count: state.count + 1,
	}),
};

describe("defineSnapshotCommand", () => {
	let handler: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		handler = vi.fn().mockReturnValue(Ok([{ type: "INC" as const }]));
	});

	it("executes a command using snapshot-accelerated loading", async () => {
		const snapshotStore = new InMemorySnapshotStore<{ count: number }>();
		await snapshotStore.create({
			streamId: "stream-1",
			snapshot: { version: 90, state: { count: 90 } },
		});

		const store = {
			load: vi.fn().mockResolvedValue(
				Ok({
					type: "loaded",
					events: Array.from({ length: 100 }, () => ({ type: "INC" })),
					lastVersion: 100,
				}),
			),
			append: vi.fn().mockResolvedValue(Ok({ lastVersion: 101 })),
		};

		const command = defineSnapshotCommand({
			aggregate,
			handler,
			snapshotStore,
		});

		const result = await command.execute({
			store,
			streamId: "stream-1",
			command: {},
			idempotencyKey: "cmd-1",
		});

		expect(result.ok).toBe(true);
		if (!result.ok) throw new Error();
		expect(result.value.state.count).toBe(101);
		expect(result.value.lastVersion).toBe(101);
	});

	it("propagates handler errors", async () => {
		const errorHandler = vi.fn().mockReturnValue(Err("INVALID"));
		const snapshotStore = new InMemorySnapshotStore<{ count: number }>();

		const store = {
			load: vi
				.fn()
				.mockResolvedValue(Ok({ type: "loaded", events: [], lastVersion: 0 })),
			append: vi.fn(),
		};

		const command = defineSnapshotCommand({
			aggregate,
			handler: errorHandler,
			snapshotStore,
		});

		const result = await command.execute({
			store,
			streamId: "stream-1",
			command: {},
			idempotencyKey: "cmd-1",
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect(result.error).toBe("INVALID");
	});

	it("propagates snapshot store errors", async () => {
		const store = {
			load: vi.fn(),
			append: vi.fn(),
		};

		const failingSnapshotStore = {
			create: vi.fn(),
			replace: vi.fn(),
			load: () =>
				Promise.resolve({
					ok: false as const,
					error: { type: "StoreError" as const, cause: "boom" },
				}),
		};

		const command = defineSnapshotCommand({
			aggregate,
			handler,
			snapshotStore: failingSnapshotStore,
		});

		const result = await command.execute({
			store,
			streamId: "stream-1",
			command: {},
			idempotencyKey: "cmd-1",
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect((result.error as { type: string }).type).toBe("StoreError");
	});

	it("propagates event store errors", async () => {
		const snapshotStore = new InMemorySnapshotStore<{ count: number }>();

		const store = {
			load: vi
				.fn()
				.mockResolvedValue(Ok({ type: "loaded", events: [], lastVersion: 0 })),
			append: vi.fn().mockResolvedValue({
				ok: false,
				error: { type: "StoreError", cause: "db down" },
			}),
		};

		const command = defineSnapshotCommand({
			aggregate,
			handler,
			snapshotStore,
		});

		const result = await command.execute({
			store,
			streamId: "stream-1",
			command: {},
			idempotencyKey: "cmd-1",
		});

		expect(result.ok).toBe(false);
		if (result.ok) throw new Error();
		expect((result as any).error.type).toBe("StoreError");
	});

	it("returns the events emitted by the handler", async () => {
		const snapshotStore = new InMemorySnapshotStore<{ count: number }>();

		const store = {
			load: vi
				.fn()
				.mockResolvedValue(Ok({ type: "loaded", events: [], lastVersion: 0 })),
			append: vi.fn().mockResolvedValue(Ok({ lastVersion: 1 })),
		};

		const incHandler = vi.fn().mockReturnValue(Ok([{ type: "INC" as const }]));

		const command = defineSnapshotCommand({
			aggregate,
			handler: incHandler,
			snapshotStore,
		});

		const result = await command.execute({
			store,
			streamId: "stream-1",
			command: {},
			idempotencyKey: "cmd-1",
		});

		expect(result.ok).toBe(true);
		if (!result.ok) throw new Error();
		expect(result.value.events).toEqual([{ type: "INC" }]);
	});
});
