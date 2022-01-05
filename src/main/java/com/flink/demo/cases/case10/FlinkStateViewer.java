package com.flink.demo.cases.case10;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.local.LocalDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/16 17:16
 */
public class FlinkStateViewer {

    public static void main(String[] args) throws IOException {
        ClassLoader classLoader = FlinkStateViewer.class.getClassLoader();
        String savepointDir = "file:///tmp/streaming-state/flink/4/e7d895717dc60b42890fc2bb065a52df/chk-25";
        JobID job = JobID.generate();
        Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

        FsStateBackend stateBackend = new FsStateBackend(savepointDir);
        CheckpointStorage checkpointStorage = stateBackend.createCheckpointStorage(job);
        CompletedCheckpointStorageLocation checkpointLocation = checkpointStorage.resolveCheckpoint(savepointDir);
        CompletedCheckpoint savepoint = Checkpoints.loadAndValidateCheckpoint(
                job, tasks, checkpointLocation, classLoader, false);
        System.out.println(savepoint);

        //read file
        File file = new File(
                "/tmp/streaming-state/flink/4/e7d895717dc60b42890fc2bb065a52df/chk-25/5d99223c-6c09-42dc-94e4-b60758aeec90");
        LocalDataInputStream localDataInputStream = new LocalDataInputStream(file);
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(localDataInputStream);

        KeyedBackendSerializationProxy<Object> serializationProxy =
                new KeyedBackendSerializationProxy<>(classLoader);
        serializationProxy.read(inView);

        List<StateMetaInfoSnapshot> restoredMetaInfos =
                serializationProxy.getStateMetaInfoSnapshots();
        System.out.println(restoredMetaInfos);

        // stateBackend.createKeyedStateBackend()
    }

}
