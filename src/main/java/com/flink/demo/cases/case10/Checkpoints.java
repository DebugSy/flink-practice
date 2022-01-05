package com.flink.demo.cases.case10;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializer;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializers;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class with the methods to write/load/dispose the checkpoint and savepoint metadata.
 *
 * <p>Stored checkpoint metadata files have the following format:
 * <pre>[MagicNumber (int) | Format Version (int) | Checkpoint Metadata (variable)]</pre>
 *
 * <p>The actual savepoint serialization is version-specific via the {@link SavepointSerializer}.
 */
public class Checkpoints {

	private static final Logger LOG = LoggerFactory.getLogger(Checkpoints.class);

	/** Magic number at the beginning of every checkpoint metadata file, for sanity checks. */
	public static final int HEADER_MAGIC_NUMBER = 0x4960672d;

	// ------------------------------------------------------------------------
	//  Reading and validating checkpoint metadata
	// ------------------------------------------------------------------------

	public static Savepoint loadCheckpointMetadata(DataInputStream in, ClassLoader classLoader) throws IOException {
		checkNotNull(in, "input stream");
		checkNotNull(classLoader, "classLoader");

		final int magicNumber = in.readInt();

		if (magicNumber == HEADER_MAGIC_NUMBER) {
			final int version = in.readInt();
			final SavepointSerializer<?> serializer = SavepointSerializers.getSerializer(version);

			if (serializer != null) {
				return serializer.deserialize(in, classLoader);
			}
			else {
				throw new IOException("Unrecognized checkpoint version number: " + version);
			}
		}
		else {
			throw new IOException("Unexpected magic number. This can have multiple reasons: " +
					"(1) You are trying to load a Flink 1.0 savepoint, which is not supported by this " +
					"version of Flink. (2) The file you were pointing to is not a savepoint at all. " +
					"(3) The savepoint file has been corrupted.");
		}
	}

	@SuppressWarnings("deprecation")
	public static CompletedCheckpoint loadAndValidateCheckpoint(
			JobID jobId,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			CompletedCheckpointStorageLocation location,
			ClassLoader classLoader,
			boolean allowNonRestoredState) throws IOException {

		checkNotNull(jobId, "jobId");
		checkNotNull(tasks, "tasks");
		checkNotNull(location, "location");
		checkNotNull(classLoader, "classLoader");

		final StreamStateHandle metadataHandle = location.getMetadataHandle();
		final String checkpointPointer = location.getExternalPointer();

		// (1) load the savepoint
		final Savepoint rawCheckpointMetadata;
		try (InputStream in = metadataHandle.openInputStream()) {
			DataInputStream dis = new DataInputStream(in);
			rawCheckpointMetadata = loadCheckpointMetadata(dis, classLoader);
		}

		final Savepoint checkpointMetadata = rawCheckpointMetadata.getTaskStates() == null ?
				rawCheckpointMetadata :
				SavepointV2.convertToOperatorStateSavepointV2(tasks, rawCheckpointMetadata);

		// generate mapping from operator to task
		Map<OperatorID, ExecutionJobVertex> operatorToJobVertexMapping = new HashMap<>();
		for (ExecutionJobVertex task : tasks.values()) {
			for (OperatorID operatorID : task.getOperatorIDs()) {
				operatorToJobVertexMapping.put(operatorID, task);
			}
		}

		// (2) validate it (parallelism, etc)
		boolean expandedToLegacyIds = false;

		HashMap<OperatorID, OperatorState> operatorStates = new HashMap<>(checkpointMetadata.getOperatorStates().size());
		for (OperatorState operatorState : checkpointMetadata.getOperatorStates()) {
			Map<Integer, OperatorSubtaskState> subtaskStates = operatorState.getSubtaskStates();
			for (Integer key : subtaskStates.keySet()) {
				OperatorSubtaskState subtaskState = subtaskStates.get(key);
				long stateSize = subtaskState.getStateSize();
				System.err.println("state size is " + stateSize);
				System.err.println("key is " + key);
				StateObjectCollection<KeyedStateHandle> managedKeyedState = subtaskState.getManagedKeyedState();
				for (KeyedStateHandle keyedStateHandle : managedKeyedState) {

					KeyGroupRange keyGroupRange = keyedStateHandle.getKeyGroupRange();
					System.err.println("key group range is " + keyGroupRange);
				}
			}
		}

		// (3) convert to checkpoint so the system can fall back to it
		CheckpointProperties props = CheckpointProperties.forSavepoint();

		return new CompletedCheckpoint(
				jobId,
				checkpointMetadata.getCheckpointId(),
				0L,
				0L,
				operatorStates,
				checkpointMetadata.getMasterStates(),
				props,
				location);
	}

	// ------------------------------------------------------------------------
	//  Savepoint Disposal Hooks
	// ------------------------------------------------------------------------

	public static void disposeSavepoint(
			String pointer,
			StateBackend stateBackend,
			ClassLoader classLoader) throws IOException, FlinkException {

		checkNotNull(pointer, "location");
		checkNotNull(stateBackend, "stateBackend");
		checkNotNull(classLoader, "classLoader");

		final CompletedCheckpointStorageLocation checkpointLocation = stateBackend.resolveCheckpoint(pointer);

		final StreamStateHandle metadataHandle = checkpointLocation.getMetadataHandle();

		// load the savepoint object (the metadata) to have all the state handles that we need
		// to dispose of all state
		final Savepoint savepoint;
		try (InputStream in = metadataHandle.openInputStream();
			DataInputStream dis = new DataInputStream(in)) {

			savepoint = loadCheckpointMetadata(dis, classLoader);
		}

		Exception exception = null;

		// first dispose the savepoint metadata, so that the savepoint is not
		// addressable any more even if the following disposal fails
		try {
			metadataHandle.discardState();
		}
		catch (Exception e) {
			exception = e;
		}

		// now dispose the savepoint data
		try {
			savepoint.dispose();
		}
		catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// now dispose the location (directory, table, whatever)
		try {
			checkpointLocation.disposeStorageLocation();
		}
		catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// forward exceptions caught in the process
		if (exception != null) {
			ExceptionUtils.rethrowIOException(exception);
		}
	}

	public static void disposeSavepoint(
			String pointer,
			Configuration configuration,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IOException, FlinkException {

		checkNotNull(pointer, "location");
		checkNotNull(configuration, "configuration");
		checkNotNull(classLoader, "classLoader");

		StateBackend backend = loadStateBackend(configuration, classLoader, logger);

		disposeSavepoint(pointer, backend, classLoader);
	}

	@Nonnull
	public static StateBackend loadStateBackend(Configuration configuration, ClassLoader classLoader, @Nullable Logger logger) {
		if (logger != null) {
			logger.info("Attempting to load configured state backend for savepoint disposal");
		}

		StateBackend backend = null;
		try {
			backend = StateBackendLoader.loadStateBackendFromConfig(configuration, classLoader, null);

			if (backend == null && logger != null) {
				logger.info("No state backend configured, attempting to dispose savepoint " +
						"with default backend (file system based)");
			}
		}
		catch (Throwable t) {
			// catches exceptions and errors (like linking errors)
			if (logger != null) {
				logger.info("Could not load configured state backend.");
				logger.debug("Detailed exception:", t);
			}
		}

		if (backend == null) {
			// We use the memory state backend by default. The MemoryStateBackend is actually
			// FileSystem-based for metadata
			backend = new MemoryStateBackend();
		}
		return backend;
	}

	// ------------------------------------------------------------------------

	/** This class contains only static utility methods and is not meant to be instantiated. */
	private Checkpoints() {}
}
