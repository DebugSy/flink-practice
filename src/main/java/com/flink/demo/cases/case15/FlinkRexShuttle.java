/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flink.demo.cases.case15;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.Table;
import org.apache.flink.table.codegen.GeneratedExpression;

import java.util.ArrayList;
import java.util.List;

public class FlinkRexShuttle extends RexShuttle {

  private RelDataType relDataType;

  public FlinkRexShuttle(RelDataType relDataType) {
    this.relDataType = relDataType;
  }

  public RexNode visitSubQuery(RexSubQuery subQuery) {
    boolean[] update = {false};
    List<RexNode> clonedOperands = visitList(subQuery.operands, update);
    if (update[0]) {
      return subQuery.clone(subQuery.getType(), clonedOperands);
    } else {
      return subQuery;
    }
  }

  @Override
  public RexNode visitTableInputRef(RexTableInputRef ref) {
    return ref;
  }

  @Override
  public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return fieldRef;
  }

  public RexNode visitCall(final RexCall call) {
    GeneratedExpression generatedExpression = call.accept(new FlinkRexVisitor(relDataType));
    System.out.println(generatedExpression);
    return super.visitCall(call);
  }

  /**
   * Visits each of an array of expressions and returns an array of the
   * results.
   *
   * @param exprs  Array of expressions
   * @param update If not null, sets this to true if any of the expressions
   *               was modified
   * @return Array of visited expressions
   */
  protected RexNode[] visitArray(RexNode[] exprs, boolean[] update) {
    RexNode[] clonedOperands = new RexNode[exprs.length];
    for (int i = 0; i < exprs.length; i++) {
      RexNode operand = exprs[i];
      RexNode clonedOperand = operand.accept(this);
      if ((clonedOperand != operand) && (update != null)) {
        update[0] = true;
      }
      clonedOperands[i] = clonedOperand;
    }
    return clonedOperands;
  }

  /**
   * Visits each of a list of expressions and returns a list of the
   * results.
   *
   * @param exprs  List of expressions
   * @param update If not null, sets this to true if any of the expressions
   *               was modified
   * @return Array of visited expressions
   */
  protected List<RexNode> visitList(
          List<? extends RexNode> exprs, boolean[] update) {
    ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
    for (RexNode operand : exprs) {
      RexNode clonedOperand = operand.accept(this);
      if ((clonedOperand != operand) && (update != null)) {
        update[0] = true;
      }
      clonedOperands.add(clonedOperand);
    }
    return clonedOperands.build();
  }

  /**
   * Visits a list and writes the results to another list.
   */
  public void visitList(
          List<? extends RexNode> exprs, List<RexNode> outExprs) {
    for (RexNode expr : exprs) {
      outExprs.add(expr.accept(this));
    }
  }

  /**
   * Visits each of a list of field collations and returns a list of the
   * results.
   *
   * @param collations List of field collations
   * @param update     If not null, sets this to true if any of the expressions
   *                   was modified
   * @return Array of visited field collations
   */
  protected List<RexFieldCollation> visitFieldCollations(
          List<RexFieldCollation> collations, boolean[] update) {
    ImmutableList.Builder<RexFieldCollation> clonedOperands =
            ImmutableList.builder();
    for (RexFieldCollation collation : collations) {
      RexNode clonedOperand = collation.left.accept(this);
      if ((clonedOperand != collation.left) && (update != null)) {
        update[0] = true;
        collation =
                new RexFieldCollation(clonedOperand, collation.right);
      }
      clonedOperands.add(collation);
    }
    return clonedOperands.build();
  }

  public RexNode visitCorrelVariable(RexCorrelVariable variable) {
    return variable;
  }

  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return super.visitFieldAccess(fieldAccess);
  }

  public RexNode visitInputRef(RexInputRef inputRef) {
    return inputRef;
  }

  public RexNode visitLocalRef(RexLocalRef localRef) {
    return localRef;
  }

  public RexNode visitLiteral(RexLiteral literal) {
    return literal;
  }

  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return dynamicParam;
  }

  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return rangeRef;
  }

}
