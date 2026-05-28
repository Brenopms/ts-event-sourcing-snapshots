/**
 * Healthcare example using @ts-event-sourcing/snapshots.
 *
 * Demonstrates snapshot-accelerated command execution with the same
 * healthcare domain as the core library example. Shows periodic
 * snapshotting, snapshot-aware loading, and determinism guarantees.
 *
 * Run: npx tsx examples/healthcare-snapshot.ts
 */

import {
	type AggregateDefinition,
	type CommandHandler,
	createAggregate,
	Err,
	InMemoryEventStore,
	loadAggregate,
	matchEvent,
	Ok,
	unwrap,
} from "@ts-event-sourcing/core";

import {
	defineSnapshotCommand,
	InMemorySnapshotStore,
	loadAggregateWithSnapshot,
	takeSnapshot,
} from "../src";

// ═══════════════════════════════════════════════════════════════════════════
// Domain types (identical to core healthcare example)
// ═══════════════════════════════════════════════════════════════════════════

type PatientEvent =
	| {
			type: "PatientRegistered";
			patientId: string;
			name: string;
			dateOfBirth: Date;
	  }
	| {
			type: "AllergyRecorded";
			patientId: string;
			allergen: string;
			severity: "Mild" | "Moderate" | "Severe";
	  }
	| {
			type: "PrescriptionIssued";
			patientId: string;
			prescriptionId: string;
			drug: string;
			dosage: string;
			startDate: Date;
			endDate: Date;
	  }
	| {
			type: "EncounterStarted";
			patientId: string;
			encounterId: string;
			reason: string;
			startedAt: Date;
	  }
	| {
			type: "EncounterClosed";
			patientId: string;
			encounterId: string;
			notes: string;
			closedAt: Date;
	  };

type PatientState = {
	patientId: string;
	name: string;
	dateOfBirth: Date;
	allergies: Array<{ allergen: string; severity: string }>;
	prescriptions: Array<{
		prescriptionId: string;
		drug: string;
		dosage: string;
		startDate: Date;
		endDate: Date;
	}>;
	currentEncounter: {
		encounterId: string;
		reason: string;
		startedAt: Date;
		status: "Open" | "Closed";
		notes?: string;
		closedAt?: Date;
	} | null;
};

type PatientError =
	| { type: "PatientAlreadyRegistered" }
	| { type: "PatientNotFound" }
	| { type: "EncounterAlreadyOpen" }
	| { type: "NoOpenEncounter" }
	| { type: "AllergyConflict"; drug: string; allergen: string }
	| { type: "InvalidPrescriptionDates" }
	| { type: "DuplicatePrescriptionId" };

const patientAggregate: AggregateDefinition<PatientState, PatientEvent> = {
	initialState: {
		patientId: "",
		name: "",
		dateOfBirth: new Date(0),
		allergies: [],
		prescriptions: [],
		currentEncounter: null,
	},
	reduce: (state, event) =>
		matchEvent(event, {
			PatientRegistered: (e) => ({
				patientId: e.patientId,
				name: e.name,
				dateOfBirth: e.dateOfBirth,
				allergies: [],
				prescriptions: [],
				currentEncounter: null,
			}),
			AllergyRecorded: (e) => ({
				...state,
				allergies: [
					...state.allergies,
					{ allergen: e.allergen, severity: e.severity },
				],
			}),
			PrescriptionIssued: (e) => ({
				...state,
				prescriptions: [
					...state.prescriptions,
					{
						prescriptionId: e.prescriptionId,
						drug: e.drug,
						dosage: e.dosage,
						startDate: e.startDate,
						endDate: e.endDate,
					},
				],
			}),
			EncounterStarted: (e) => ({
				...state,
				currentEncounter: {
					encounterId: e.encounterId,
					reason: e.reason,
					startedAt: e.startedAt,
					status: "Open",
				},
			}),
			EncounterClosed: (e) => ({
				...state,
				currentEncounter: state.currentEncounter
					? {
							...state.currentEncounter,
							status: "Closed",
							notes: e.notes,
							closedAt: e.closedAt,
						}
					: null,
			}),
		}),
};

// ═══════════════════════════════════════════════════════════════════════════
// Command handlers (identical to core — handlers are domain-only, no snapshots)
// ═══════════════════════════════════════════════════════════════════════════

const registerPatientHandler: CommandHandler<
	PatientState,
	{ patientId: string; name: string; dateOfBirth: Date },
	PatientEvent,
	PatientError
> = ({ command }) => Ok([{ type: "PatientRegistered", ...command }]);

const recordAllergyHandler: CommandHandler<
	PatientState,
	{ allergen: string; severity: "Mild" | "Moderate" | "Severe" },
	PatientEvent,
	PatientError
> = ({ state, command }) =>
	Ok([
		{
			type: "AllergyRecorded",
			patientId: state.patientId,
			allergen: command.allergen,
			severity: command.severity,
		},
	]);

const issuePrescriptionHandler: CommandHandler<
	PatientState,
	{
		prescriptionId: string;
		drug: string;
		dosage: string;
		startDate: Date;
		endDate: Date;
	},
	PatientEvent,
	PatientError
> = ({ state, command }) => {
	if (
		state.prescriptions.some((p) => p.prescriptionId === command.prescriptionId)
	)
		return Err({ type: "DuplicatePrescriptionId" });
	const allergy = state.allergies.find((a) =>
		command.drug.toLowerCase().includes(a.allergen.toLowerCase()),
	);
	if (allergy)
		return Err({
			type: "AllergyConflict",
			drug: command.drug,
			allergen: allergy.allergen,
		});
	if (command.endDate <= command.startDate)
		return Err({ type: "InvalidPrescriptionDates" });
	return Ok([
		{ type: "PrescriptionIssued", patientId: state.patientId, ...command },
	]);
};

const startEncounterHandler: CommandHandler<
	PatientState,
	{ encounterId: string; reason: string; startedAt: Date },
	PatientEvent,
	PatientError
> = ({ state, command }) => {
	if (state.currentEncounter?.status === "Open")
		return Err({ type: "EncounterAlreadyOpen" });
	return Ok([
		{ type: "EncounterStarted", patientId: state.patientId, ...command },
	]);
};

const closeEncounterHandler: CommandHandler<
	PatientState,
	{ notes: string; closedAt: Date },
	PatientEvent,
	PatientError
> = ({ state, command }) => {
	if (!state.currentEncounter || state.currentEncounter.status !== "Open")
		return Err({ type: "NoOpenEncounter" });
	return Ok([
		{
			type: "EncounterClosed",
			patientId: state.patientId,
			encounterId: state.currentEncounter.encounterId,
			notes: command.notes,
			closedAt: command.closedAt,
		},
	]);
};

// ═══════════════════════════════════════════════════════════════════════════
// Main — demonstrates snapshot-accelerated healthcare workflow
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
	const store = new InMemoryEventStore<PatientEvent>();
	const snapshotStore = new InMemorySnapshotStore<PatientState>();
	const DOB = new Date("1985-06-15");
	const SNAPSHOT_INTERVAL = 3; // take a snapshot every 3 commands

	console.log("⚕ Healthcare (Snapshot-Accelerated)\n");

	// ── Step 1: Bind commands with snapshot acceleration ─────────────────

	const registerPatient = defineSnapshotCommand({
		aggregate: patientAggregate,
		handler: registerPatientHandler,
		snapshotStore,
	});
	const recordAllergy = defineSnapshotCommand({
		aggregate: patientAggregate,
		handler: recordAllergyHandler,
		snapshotStore,
	});
	const issuePrescription = defineSnapshotCommand({
		aggregate: patientAggregate,
		handler: issuePrescriptionHandler,
		snapshotStore,
	});
	const startEncounter = defineSnapshotCommand({
		aggregate: patientAggregate,
		handler: startEncounterHandler,
		snapshotStore,
	});
	const closeEncounter = defineSnapshotCommand({
		aggregate: patientAggregate,
		handler: closeEncounterHandler,
		snapshotStore,
	});

	// ── Step 2: Create aggregate stream ──────────────────────────────────

	await createAggregate({
		store,
		streamId: "p-1",
		events: [],
		idempotencyKey: "open-p-1",
	});
	console.log("✓ Created empty stream for patient p-1");

	// ── Step 3: Execute commands, snapshotting periodically ──────────────

	let cmdCount = 0;

	async function executeAndSnapshot(
		result: Awaited<ReturnType<typeof registerPatient.execute>>,
	) {
		cmdCount++;
		if (result.ok && cmdCount % SNAPSHOT_INTERVAL === 0) {
			await takeSnapshot({
				snapshotStore,
				streamId: "p-1",
				snapshot: {
					state: result.value.state,
					version: result.value.lastVersion,
				},
			});
			console.log(`  ↳ Snapshot taken at version ${result.value.lastVersion}`);
		}
		return result;
	}

	// Register patient
	unwrap(
		await executeAndSnapshot(
			await registerPatient.execute({
				store,
				streamId: "p-1",
				command: { patientId: "p-1", name: "Jane Doe", dateOfBirth: DOB },
				idempotencyKey: "reg",
			}),
		),
	);
	console.log("✓ Registered patient: Jane Doe (1985-06-15)");

	// Record allergy
	unwrap(
		await executeAndSnapshot(
			await recordAllergy.execute({
				store,
				streamId: "p-1",
				command: { allergen: "Penicillin", severity: "Severe" },
				idempotencyKey: "allergy-1",
			}),
		),
	);
	console.log("✓ Recorded allergy: Penicillin (Severe)");

	// Record another allergy
	unwrap(
		await executeAndSnapshot(
			await recordAllergy.execute({
				store,
				streamId: "p-1",
				command: { allergen: "Latex", severity: "Mild" },
				idempotencyKey: "allergy-2",
			}),
		),
	);
	console.log("✓ Recorded allergy: Latex (Mild)");

	// Issue prescription (no conflict — Ibuprofen vs Penicillin)
	unwrap(
		await executeAndSnapshot(
			await issuePrescription.execute({
				store,
				streamId: "p-1",
				command: {
					prescriptionId: "RX-001",
					drug: "Ibuprofen",
					dosage: "200mg",
					startDate: new Date("2024-01-01"),
					endDate: new Date("2024-01-31"),
				},
				idempotencyKey: "rx-001",
			}),
		),
	);
	console.log("✓ Issued prescription RX-001: Ibuprofen 200mg");

	// Start encounter
	unwrap(
		await executeAndSnapshot(
			await startEncounter.execute({
				store,
				streamId: "p-1",
				command: {
					encounterId: "enc-1",
					reason: "Routine checkup",
					startedAt: new Date("2024-06-01T09:00:00Z"),
				},
				idempotencyKey: "enc-1",
			}),
		),
	);
	console.log("✓ Started encounter enc-1: Routine checkup");

	// Close encounter
	unwrap(
		await executeAndSnapshot(
			await closeEncounter.execute({
				store,
				streamId: "p-1",
				command: {
					notes: "Patient recovering well",
					closedAt: new Date("2024-06-01T10:00:00Z"),
				},
				idempotencyKey: "close-1",
			}),
		),
	);
	console.log("✓ Closed encounter enc-1");

	// Issue second prescription
	unwrap(
		await executeAndSnapshot(
			await issuePrescription.execute({
				store,
				streamId: "p-1",
				command: {
					prescriptionId: "RX-002",
					drug: "Aspirin",
					dosage: "100mg",
					startDate: new Date("2024-02-01"),
					endDate: new Date("2024-02-28"),
				},
				idempotencyKey: "rx-002",
			}),
		),
	);
	console.log("✓ Issued prescription RX-002: Aspirin 100mg");

	// Try penicillin (should fail — allergy conflict)
	const conflictResult = await executeAndSnapshot(
		await issuePrescription.execute({
			store,
			streamId: "p-1",
			command: {
				prescriptionId: "RX-003",
				drug: "Penicillin",
				dosage: "500mg",
				startDate: new Date("2024-03-01"),
				endDate: new Date("2024-03-10"),
			},
			idempotencyKey: "rx-conflict",
		}),
	);
	if (!conflictResult.ok) {
		console.log(
			`✗ Rejected penicillin: AllergyConflict (allergen: ${conflictResult.error.type === "AllergyConflict" ? conflictResult.error.allergen : "?"})`,
		);
	}

	// ── Step 4: Compare snapshot load vs full replay ─────────────────────

	console.log("\n── Snapshot vs Full Replay ──\n");

	const startFull = Date.now();
	const fullResult = unwrap(
		await loadAggregate({
			store,
			streamId: "p-1",
			aggregate: patientAggregate,
		}),
	);
	const fullTime = Date.now() - startFull;

	const startSnapshot = Date.now();
	const snapshotResult = unwrap(
		await loadAggregateWithSnapshot({
			store,
			aggregate: patientAggregate,
			streamId: "p-1",
			snapshotStore,
		}),
	);
	const snapshotTime = Date.now() - startSnapshot;

	console.log(
		`Full replay:      state={ prescriptions: ${fullResult.state.prescriptions.length}, allergies: ${fullResult.state.allergies.length} } lastVersion=${fullResult.lastVersion} (${fullTime}ms)`,
	);
	console.log(
		`Snapshot load:    state={ prescriptions: ${snapshotResult.state.prescriptions.length}, allergies: ${snapshotResult.state.allergies.length} } lastVersion=${snapshotResult.lastVersion} (${snapshotTime}ms)`,
	);
	console.log(
		`Determinism check: ${JSON.stringify(fullResult.state) === JSON.stringify(snapshotResult.state) ? "✓ IDENTICAL" : "✗ MISMATCH"}`,
	);

	// ── Step 5: Load via takeSnapshot (manual lifecycle) ─────────────────

	console.log("\n── Manual Snapshot Lifecycle ──\n");

	// Start encoder — takes snapshot at v3, then more commands
	const startEnc = await startEncounter.execute({
		store,
		streamId: "p-1",
		command: {
			encounterId: "enc-2",
			reason: "Follow-up",
			startedAt: new Date("2024-07-01T09:00:00Z"),
		},
		idempotencyKey: "enc-2",
	});
	if (startEnc.ok) {
		await takeSnapshot({
			snapshotStore,
			streamId: "p-1",
			snapshot: {
				state: startEnc.value.state,
				version: startEnc.value.lastVersion,
			},
		});
		console.log(
			`✓ Manually snapshotted at v${startEnc.value.lastVersion} after starting follow-up encounter`,
		);
	}

	unwrap(
		await closeEncounter.execute({
			store,
			streamId: "p-1",
			command: {
				notes: "All clear",
				closedAt: new Date("2024-07-01T10:00:00Z"),
			},
			idempotencyKey: "close-2",
		}),
	);
	console.log("✓ Closed follow-up encounter");

	const finalState = unwrap(
		await loadAggregateWithSnapshot({
			store,
			aggregate: patientAggregate,
			streamId: "p-1",
			snapshotStore,
		}),
	);
	console.log(
		`\nFinal state: v${finalState.lastVersion}, ${finalState.state.prescriptions.length} prescriptions, ${finalState.state.allergies.length} allergies, encounter: ${finalState.state.currentEncounter?.status ?? "none"}`,
	);
}

main().catch((e) => {
	console.error("Failed:", e);
	process.exit(1);
});
