package com.flink.demo.cases.case10;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Created by P0007 on 2019/9/5.
 */
public class FlinkStateQueryClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        QueryableStateClient client = new QueryableStateClient("127.0.0.1", 9069);

        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}) // type information
                        ); // default value of the state, if nothing was set

        CompletableFuture<ValueState<Tuple2<Long, Long>>> resultFuture =
                client.getKvState(
                        JobID.fromHexString("c4d2fcef40d1fe5298c8889b81840ee5"),
                        "query-name",
                        1L,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        descriptor
                );

        ValueState<Tuple2<Long, Long>> valueState = resultFuture.join();
        System.err.println(valueState.value());

        CompletableFuture<?> completableFuture = client.shutdownAndHandle();


    }

}
