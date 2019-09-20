package com.flink.demo.cases.case15;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.codegen.GeneratedExpression;
import org.apache.flink.table.codegen.GeneratedFunction;

import java.util.List;

/**
 * Created by P0007 on 2019/9/19.
 */
public class FlinkCodeGenerator {

    public GeneratedFunction GenerateFunction(RelNode relNode) {
        List<RelNode> inputs = relNode.getInputs();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("public class CustomProcessFunction extends org.apache.flink.streaming.api.functions.ProcessFunction { \n")
                .append("final org.apache.flink.types.Row out = new org.apache.flink.types.Row(3)\n")
                .append("@Override\n")
                .append("public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {\n")
                .append("}\n")
                .append("\n")
                .append("@Override\n")
                .append("public void processElement(Object _in1, " +
                        "org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, " +
                        "org.apache.flink.util.Collector c) throws Exception {\n")
                .append("org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;\n")
                .append("\n")
                .append("}\n");
        return null;
    }

    public GeneratedExpression generateResultExpression() {
        return null;
    }

}
