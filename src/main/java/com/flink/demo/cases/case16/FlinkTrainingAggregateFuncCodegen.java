package com.flink.demo.cases.case16;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Window;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.codegen.AggregationCodeGenerator;
import org.apache.flink.table.plan.nodes.datastream.DataStreamCalc;
import org.apache.flink.table.plan.nodes.datastream.DataStreamGroupAggregate;
import org.apache.flink.table.plan.nodes.datastream.DataStreamGroupWindowAggregate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate;
import org.apache.flink.table.plan.rules.common.LogicalWindowAggregateRule;
import org.apache.flink.table.plan.rules.datastream.DataStreamLogicalWindowAggregateRule;
import org.apache.flink.table.runtime.aggregate.AggregateUtil;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/9/29.
 */
public class FlinkTrainingAggregateFuncCodegen {

    private static String hopWindowSql = "select username, count(*) as cnt, " +
            "HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_rowtime, " +
            "HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_start, " +
            "HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "group by username, " +
            "HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> streamSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        tEnv.registerDataStream("clicks", streamSource, OutOfOrderDataSource.CLICK_FIELDS);

        SingleOutputStreamOperator<Row> streamSourceWithWatermarks = streamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        KeyedStream<Row, Tuple> keyedStream = streamSourceWithWatermarks.keyBy(0);

        FlinkPlannerImpl planner = new FlinkPlannerImpl(tEnv.getFrameworkConfig(), tEnv.getPlanner(), tEnv.getTypeFactory());
        SqlNode sqlNode = planner.parse(hopWindowSql);
        SqlNode validateSqlNode = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(validateSqlNode);
        RelNode relNode = relRoot.rel;
        RelDataType rowType = relNode.getRowType();

        RelNode dataStreamPlan = tEnv.optimize(relNode, true);


        DataStreamCalc dataStreamCalc = (DataStreamCalc) dataStreamPlan;
        System.out.println(dataStreamCalc);

        RelNode input = dataStreamCalc.getInput();
        DataStreamGroupWindowAggregate dataStreamGroupAggregate = (DataStreamGroupWindowAggregate) input;
        System.out.println(dataStreamGroupAggregate);


        AggregationCodeGenerator generator = new AggregationCodeGenerator(tEnv.getConfig(),
                false,
                UrlClickRowDataSource.USER_CLICK_TYPEINFO,
                null);

//        AggregateUtil.createDataStreamAggregateFunction(
//                generator,
//                dataStreamGroupAggregate.name
//        )



    }

}
