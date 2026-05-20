import { Ok, type Result } from "@ts-event-sourcing/core";
import type {
	SnapshotError,
	SnapshotStore,
} from "../snapshot-store/snapshot-store";

export async function takeSnapshot<State>(params: {
	snapshotStore: SnapshotStore<State>;
	streamId: string;
	snapshot: { state: State; version: number };
}): Promise<Result<void, SnapshotError>> {
	const { snapshotStore, streamId, snapshot } = params;

	const loadResult = await snapshotStore.load({ streamId });

	if (!loadResult.ok) {
		return loadResult;
	}

	const existing = loadResult.value;

	if (existing === null) {
		return snapshotStore.create({ streamId, snapshot });
	}

	if (snapshot.version > existing.version) {
		return snapshotStore.replace({ streamId, snapshot });
	}

	return Ok(undefined);
}
