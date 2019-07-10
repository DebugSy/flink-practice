package com.flink.demo.cases.case01;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by DebugSy on 2019/7/1.
 *  java case 01
 *  Flink China 社区第四讲 Datastream API
 *  需求：实时统计成交额
 *  1. 实时统计每个类别的成交额
 *  2. 实时统计全部类别的成交额
 */
public class FlinkChina1_4 {

  private static class Datasource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
      Random random = new Random(System.currentTimeMillis());
      while (running) {
        Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 + 500);
        String key = "类别" + (char)('A' + random.nextInt(3));
        int value = random.nextInt(10) + 1;
        System.out.println(String.format("Emit:\t(%s,%d)", key, value));
        ctx.collect(new Tuple2<>(key, value));
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new Datasource());

    KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = source.keyBy(0);

    //1. 实时统计每个类别的成交额
    keyedStream.sum(1)
            .addSink(new SinkFunction<Tuple2<String, Integer>>() {
              @Override
              public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.println(String.format("Get:\t (%s,%d)", value.f0, value.f1));
              }
            });

//    //2. 实时统计全部类别的成交额
//    keyedStream.sum(1)
//            .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
//
//      @Override
//      public Object getKey(Tuple2<String, Integer> value) throws Exception {
//        return "";//让flink将所有数据分成一组
//      }
//    }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
//
//      @Override
//      public Map<String, Integer> fold(Map<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
//        accumulator.put(value.f0, value.f1);
//        return accumulator;
//      }
//    }).addSink(new SinkFunction<Map<String, Integer>>() {
//      @Override
//      public void invoke(Map<String, Integer> value, Context context) throws Exception {
//        System.out.println(value.values().stream().mapToInt(v -> v).sum());
//      }
//    });

    env.execute("FlinkChina1_4 Demo");
  }

}
