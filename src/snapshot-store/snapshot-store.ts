import type { Result } from "@ts-event-sourcing/core";

export type Snapshot<State> = {
	streamId: string;
	version: number;
	state: State;
};

export type SnapshotError =
	| { type: "SnapshotAlreadyExists" }
	| { type: "SnapshotNotFound" }
	| { type: "StoreError"; cause: unknown };

export interface SnapshotStore<State> {
	create(params: {
		streamId: string;
		snapshot: Omit<Snapshot<State>, "streamId">;
	}): Promise<Result<void, SnapshotError>>;

	replace(params: {
		streamId: string;
		snapshot: Omit<Snapshot<State>, "streamId">;
	}): Promise<Result<void, SnapshotError>>;

	load(params: {
		streamId: string;
	}): Promise<Result<Snapshot<State> | null, SnapshotError>>;
}
