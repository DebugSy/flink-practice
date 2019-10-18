package com.flink.demo.cases.case19;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * Created by P0007 on 2019/10/18.
 * java case 19
 * 利用Flink Env解析Sql，获取sql的字段和类型
 */
public class ExtractFlinkSqlColAndType {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        TypeInformation<Row> userClickTypeInfo = Types.ROW(
                new String[]{"userId", "username", "url", "clickTime", "rank", "uuid"},
                new TypeInformation[]{
                        Types.DOUBLE(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.SQL_TIMESTAMP(),
                        Types.INT(),
                        Types.STRING()
                });

        Row row = new Row(6);
        DataStream<Row> returns = env.fromCollection(Arrays.asList(row)).returns(userClickTypeInfo);
        returns.printToErr();

        tEnv.registerDataStream("tableName", returns, "userId,username,url,clickTime,rank,uuid");

        FlinkPlannerImpl planner = new FlinkPlannerImpl(tEnv.getFrameworkConfig(),
                tEnv.getPlanner(), tEnv.getTypeFactory());

        SqlNode sqlNode = planner.parse("select " +
                "userId," +
                "sum(userId) as sum_id, " +
                "avg(userId) as avg_id," +
                "max(userId) as max_id, " +
                "min(userId) as min_id, " +
                "count(userId) as count_id " +
                "from tableName group by userId");
        SqlNode validateSqlNode = planner.validate(sqlNode);
        RelDataType validatedNodeType = planner.validator().getValidatedNodeType(validateSqlNode);
        System.out.println(validatedNodeType);



        String[] strings1 = tEnv.listTables();
        System.out.println(strings1);
        System.out.println("----");
    }

}
