import type {
	AggregateDefinition,
	AnyEvent,
	CoreError,
	EventStore,
} from "@ts-event-sourcing/core";
import { fold, Ok, type Result } from "@ts-event-sourcing/core";
import type {
	SnapshotError,
	SnapshotStore,
} from "../snapshot-store/snapshot-store";

export async function loadAggregateWithSnapshot<
	State,
	Event extends AnyEvent,
>(params: {
	store: EventStore<Event>;
	aggregate: AggregateDefinition<State, Event>;
	streamId: string;
	snapshotStore: SnapshotStore<State>;
}): Promise<
	Result<{ state: State; lastVersion: number }, CoreError | SnapshotError>
> {
	const { store, aggregate, streamId, snapshotStore } = params;

	const snapshotResult = await snapshotStore.load({ streamId });

	if (!snapshotResult.ok) {
		return snapshotResult;
	}

	const snapshot = snapshotResult.value;

	const loadResult = await store.load({ streamId });

	if (!loadResult.ok) {
		return loadResult;
	}

	const stream = loadResult.value;

	if (stream.type === "empty") {
		return { ok: false, error: { type: "AggregateNotFound" } };
	}

	const startingState = snapshot ? snapshot.state : aggregate.initialState;
	const events = snapshot
		? stream.events.slice(snapshot.version)
		: stream.events;

	const state = fold(startingState, aggregate.reduce, events);
	return Ok({ state, lastVersion: stream.lastVersion });
}
