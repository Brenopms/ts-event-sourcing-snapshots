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

export function defineSnapshotCommand<
	State,
	Command,
	Event extends AnyEvent,
	Error,
>(params: {
	aggregate: AggregateDefinition<State, Event>;
	handler: CommandHandler<State, Command, Event, Error>;
	snapshotStore: SnapshotStore<State>;
}): {
	execute(input: {
		store: EventStore<Event>;
		streamId: string;
		command: Command;
		idempotencyKey: string;
	}): Promise<
		Result<
			{ state: State; events: readonly Event[]; lastVersion: number },
			Error | CoreError | SnapshotError
		>
	>;
} {
	const { aggregate, handler, snapshotStore } = params;

	return {
		execute(input: {
			store: EventStore<Event>;
			streamId: string;
			command: Command;
			idempotencyKey: string;
		}): Promise<
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
