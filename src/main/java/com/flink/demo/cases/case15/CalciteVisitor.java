package com.flink.demo.cases.case15;

import org.apache.calcite.rex.*;

/**
 * Created by P0007 on 2019/9/16.
 */
public class CalciteVisitor implements RexVisitor<String> {
    @Override
    public String visitInputRef(RexInputRef rexInputRef) {
        return null;
    }

    @Override
    public String visitLocalRef(RexLocalRef rexLocalRef) {
        return null;
    }

    @Override
    public String visitLiteral(RexLiteral rexLiteral) {
        return null;
    }

    @Override
    public String visitCall(RexCall rexCall) {
        return null;
    }

    @Override
    public String visitOver(RexOver rexOver) {
        return null;
    }

    @Override
    public String visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
        return null;
    }

    @Override
    public String visitDynamicParam(RexDynamicParam rexDynamicParam) {
        return null;
    }

    @Override
    public String visitRangeRef(RexRangeRef rexRangeRef) {
        return null;
    }

    @Override
    public String visitFieldAccess(RexFieldAccess rexFieldAccess) {
        return null;
    }

    @Override
    public String visitSubQuery(RexSubQuery rexSubQuery) {
        return null;
    }

    @Override
    public String visitTableInputRef(RexTableInputRef rexTableInputRef) {
        return null;
    }

    @Override
    public String visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
        return null;
    }
}
