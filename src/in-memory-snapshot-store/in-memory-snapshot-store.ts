import { Err, Ok, type Result } from "@ts-event-sourcing/core";
import type {
	Snapshot,
	SnapshotError,
	SnapshotStore,
} from "../snapshot-store/snapshot-store";

export class InMemorySnapshotStore<State> implements SnapshotStore<State> {
	private snapshots = new Map<string, Snapshot<State>>();

	async create(params: {
		streamId: string;
		snapshot: Omit<Snapshot<State>, "streamId">;
	}): Promise<Result<void, SnapshotError>> {
		if (this.snapshots.has(params.streamId)) {
			return Err({ type: "SnapshotAlreadyExists" });
		}

		const snapshot: Snapshot<State> = {
			streamId: params.streamId,
			...params.snapshot,
		};
		this.snapshots.set(params.streamId, snapshot);
		return Ok(undefined);
	}

	async replace(params: {
		streamId: string;
		snapshot: Omit<Snapshot<State>, "streamId">;
	}): Promise<Result<void, SnapshotError>> {
		const existing = this.snapshots.get(params.streamId);

		if (!existing) {
			return Err({ type: "SnapshotNotFound" });
		}

		const snapshot: Snapshot<State> = {
			streamId: params.streamId,
			...params.snapshot,
		};
		this.snapshots.set(params.streamId, snapshot);
		return Ok(undefined);
	}

	async load(params: {
		streamId: string;
	}): Promise<Result<Snapshot<State> | null, SnapshotError>> {
		return Ok(this.snapshots.get(params.streamId) ?? null);
	}
}
