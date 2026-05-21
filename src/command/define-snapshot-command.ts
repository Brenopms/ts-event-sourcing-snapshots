import type {
	AggregateDefinition,
	AnyEvent,
	CommandHandler,
	CoreError,
	EventStore,
} from "@ts-event-sourcing/core";
import { executeCommand, type Result } from "@ts-event-sourcing/core";
import { loadAggregateWithSnapshot } from "../snapshot/load-aggregate-with-snapshot";
import type {
	SnapshotError,
	SnapshotStore,
} from "../snapshot-store/snapshot-store";

export type DefinedSnapshotCommand<S, C, E extends AnyEvent, Err> = {
	execute(input: {
		store: EventStore<E>;
		streamId: string;
		command: C;
		idempotencyKey: string;
	}): Promise<
		Result<
			{ state: S; events: readonly E[]; lastVersion: number },
			Err | CoreError | SnapshotError
		>
	>;
};

export function defineSnapshotCommand<
	State,
	Command,
	Event extends AnyEvent,
	Error,
>(params: {
	aggregate: AggregateDefinition<State, Event>;
	handler: CommandHandler<State, Command, Event, Error>;
	snapshotStore: SnapshotStore<State>;
}): DefinedSnapshotCommand<State, Command, Event, Error> {
	const { aggregate, handler, snapshotStore } = params;

	return {
		execute(
			input,
		): Promise<
			Result<
				{ state: State; events: readonly Event[]; lastVersion: number },
				Error | CoreError | SnapshotError
			>
		> {
			const { store, streamId, command, idempotencyKey } = input;

			return executeCommand({
				store,
				streamId,
				aggregate,
				command,
				idempotencyKey,
				handler,
				loader: (loaderParams) =>
					loadAggregateWithSnapshot({
						...loaderParams,
						snapshotStore,
					}),
			});
		},
	};
}
