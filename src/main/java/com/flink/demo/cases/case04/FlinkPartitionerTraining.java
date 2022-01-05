package com.flink.demo.cases.case04;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.types.Row;

import java.util.Hashtable;

public class FlinkPartitionerTraining {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("state.checkpoints.num-retained", 10);
//        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH,
//                "file:///tmp/shiy/flink/savepoints/flinkPartitioner");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1000 * 5, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///tmp/shiy/flink/checkpoints/flinkPartitioner"));

        SingleOutputStreamOperator<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        source.printToErr();

        env.execute("Flink Partitioner Training");

    }

}
