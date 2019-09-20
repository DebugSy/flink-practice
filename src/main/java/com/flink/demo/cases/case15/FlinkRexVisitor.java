package com.flink.demo.cases.case15;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.CodeGenerator;
import org.apache.flink.table.codegen.GeneratedExpression;
import org.apache.flink.table.codegen.calls.ScalarOperators;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;

/**
 * Created by P0007 on 2019/9/18.
 */
public class FlinkRexVisitor implements RexVisitor<GeneratedExpression> {

    private RelDataType relDataType;

    public FlinkRexVisitor(RelDataType relDataType) {
        this.relDataType = relDataType;
    }

    @Override
    public GeneratedExpression visitInputRef(RexInputRef inputRef) {
        RelDataTypeField relDataTypeField = relDataType.getFieldList().get(inputRef.getIndex());
        RelDataType type = relDataTypeField.getType();
        TypeInformation<?> resultType = FlinkTypeFactory.toTypeInfo(type);
        return new GeneratedExpression("result$1", "isnull$9", "", resultType, false);
    }

    @Override
    public GeneratedExpression visitLocalRef(RexLocalRef localRef) {
        return null;
    }

    @Override
    public GeneratedExpression visitLiteral(RexLiteral literal) {
        GeneratedExpression generatedExpression = null;
        TypeInformation<?> resultType = FlinkTypeFactory.toTypeInfo(literal.getType());
        Object value = literal.getValue3();
        SqlTypeName sqlTypeName = literal.getType().getSqlTypeName();
        switch (sqlTypeName) {
            case BOOLEAN:
                break;
            case VARCHAR:
            case CHAR:
                String resultTerm = CodeGenUtils.newName("result");
                String fieldType = CodeGenUtils.primitiveTypeTermForTypeInfo(resultType);
                String code = String.format("%s %s = \"%s\"",
                        fieldType,
                        resultTerm,
                        value
                );
                generatedExpression = new GeneratedExpression(resultTerm, "false", code, resultType, false);
                break;
            default:
                throw new RuntimeException("Type not supported: " + sqlTypeName);
        }
        return generatedExpression;
    }

    @Override
    public GeneratedExpression visitCall(RexCall call) {
        GeneratedExpression generatedExpression = null;
        SqlTypeName relDataType = call.getType().getSqlTypeName();
        List<GeneratedExpression> operands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            GeneratedExpression expression = operand.accept(this);
            operands.add(expression);
        }

        SqlOperator operator = call.getOperator();
        if (PLUS.equals(operator)){

        } else if (EQUALS.equals(operator)) {
            GeneratedExpression left = operands.get(0);
            GeneratedExpression right = operands.get(1);
            generatedExpression = ScalarOperators.generateEquals(false, left, right);
        }

        return generatedExpression;
    }

    @Override
    public GeneratedExpression visitOver(RexOver over) {
        return null;
    }

    @Override
    public GeneratedExpression visitCorrelVariable(RexCorrelVariable correlVariable) {
        return null;
    }

    @Override
    public GeneratedExpression visitDynamicParam(RexDynamicParam dynamicParam) {
        return null;
    }

    @Override
    public GeneratedExpression visitRangeRef(RexRangeRef rangeRef) {
        return null;
    }

    @Override
    public GeneratedExpression visitFieldAccess(RexFieldAccess fieldAccess) {
        return null;
    }

    @Override
    public GeneratedExpression visitSubQuery(RexSubQuery subQuery) {
        return null;
    }

    @Override
    public GeneratedExpression visitTableInputRef(RexTableInputRef fieldRef) {
        return null;
    }

    @Override
    public GeneratedExpression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return null;
    }
}
