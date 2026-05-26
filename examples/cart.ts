/**
 * Cart example from the PRD — simplest possible snapshot workflow.
 *
 * Demonstrates the full snapshot cycle:
 * 1. Define domain types and handlers (no snapshot awareness)
 * 2. Bind commands with defineSnapshotCommand
 * 3. Execute commands, take snapshots periodically
 * 4. Verify determinism: snapshot load = full replay
 *
 */

import type { CommandHandler } from "@ts-event-sourcing/core";
import {
	createAggregate,
	Err,
	InMemoryEventStore,
	loadAggregate,
	Ok,
	unwrap,
} from "@ts-event-sourcing/core";
import {
	defineSnapshotCommand,
	InMemorySnapshotStore,
	loadAggregateWithSnapshot,
	takeSnapshot,
} from "../src";

// ─── Domain ────────────────────────────────────────────────────────────────

type CartEvent = { type: "ItemAdded"; itemId: string } | { type: "CheckedOut" };
type CartState = { items: string[]; checkedOut: boolean };
type AddItemCommand = { itemId: string };
type CartError = "ALREADY_CHECKED_OUT";

const cartAggregate = {
	initialState: { items: [], checkedOut: false } as CartState,
	reduce: (state: CartState, event: CartEvent): CartState => {
		switch (event.type) {
			case "ItemAdded":
				return { ...state, items: [...state.items, event.itemId] };
			case "CheckedOut":
				return { ...state, checkedOut: true };
		}
	},
};

const addItemHandler: CommandHandler<
	CartState,
	AddItemCommand,
	CartEvent,
	CartError
> = ({ state, command }) => {
	if (state.checkedOut) return Err("ALREADY_CHECKED_OUT" as const);
	return Ok([{ type: "ItemAdded" as const, itemId: command.itemId }]);
};

const checkoutHandler: CommandHandler<
	CartState,
	AddItemCommand,
	CartEvent,
	CartError
> = ({ state }: { state: CartState }) => {
	if (state.checkedOut) return Err("ALREADY_CHECKED_OUT" as const);
	return Ok([{ type: "CheckedOut" as const }]);
};

const SNAPSHOT_EVERY = 3; // take a snapshot every 3 events

async function main() {
	console.log(" Cart (Snapshot-Accelerated)\n");

	const store = new InMemoryEventStore<CartEvent>();
	const snapshotStore = new InMemorySnapshotStore<CartState>();

	// Bind snapshot-accelerated commands — snapshotStore bound once
	const addItem = defineSnapshotCommand({
		aggregate: cartAggregate,
		handler: addItemHandler,
		snapshotStore,
	});
	const checkout = defineSnapshotCommand({
		aggregate: cartAggregate,
		handler: checkoutHandler,
		snapshotStore,
	});

	// Create aggregate stream
	await createAggregate({
		store,
		streamId: "cart-1",
		events: [],
		idempotencyKey: "create",
	});
	console.log("✓ Created cart-1");

	// Execute commands, snapshotting periodically
	for (const itemId of ["apple", "banana", "cherry", "date", "elderberry"]) {
		const result = unwrap(
			await addItem.execute({
				store,
				streamId: "cart-1",
				command: { itemId },
				idempotencyKey: `add-${itemId}`,
			}),
		);
		console.log(
			`✓ Added ${itemId} (v${result.lastVersion}, items: ${result.state.items.length})`,
		);

		if (result.lastVersion % SNAPSHOT_EVERY === 0) {
			await takeSnapshot({
				snapshotStore,
				streamId: "cart-1",
				snapshot: { state: result.state, version: result.lastVersion },
			});
			console.log(`  ↳ Snapshot at v${result.lastVersion}`);
		}
	}

	// Checkout
	const checkoutResult = unwrap(
		await checkout.execute({
			store,
			streamId: "cart-1",
			command: undefined as never,
			idempotencyKey: "checkout",
		}),
	);
	console.log(`✓ Checked out (v${checkoutResult.lastVersion})`);

	// Take final snapshot
	await takeSnapshot({
		snapshotStore,
		streamId: "cart-1",
		snapshot: {
			state: checkoutResult.state,
			version: checkoutResult.lastVersion,
		},
	});
	console.log(`  ↳ Final snapshot at v${checkoutResult.lastVersion}`);

	// Verify determinism
	const full = unwrap(
		await loadAggregate({
			store,
			streamId: "cart-1",
			aggregate: cartAggregate,
		}),
	);
	const snap = unwrap(
		await loadAggregateWithSnapshot({
			store,
			aggregate: cartAggregate,
			streamId: "cart-1",
			snapshotStore,
		}),
	);

	console.log(
		`\nFull replay:      ${full.state.items.length} items (v${full.lastVersion})`,
	);
	console.log(
		`Snapshot load:    ${snap.state.items.length} items (v${snap.lastVersion})`,
	);
	console.log(
		`Determinism:      ${JSON.stringify(full.state) === JSON.stringify(snap.state) ? "✓ IDENTICAL" : "✗ MISMATCH"}`,
	);
	console.log(`Checked out:      ${full.state.checkedOut}`);
}

main().catch((e) => {
	console.error("Failed:", e);
	process.exit(1);
});
