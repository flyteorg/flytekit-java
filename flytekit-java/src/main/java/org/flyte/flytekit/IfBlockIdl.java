/*
 * Copyright 2021 Flyte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.flytekit;

import static org.flyte.flytekit.MoreCollectors.toUnmodifiableList;

import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.ConjunctionExpression;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.Operand;

class IfBlockIdl {

  static IfElseBlock toIdl(SdkIfElseBlock ifElse, Map<String, Binding> extraInputs) {
    return IfElseBlock.builder()
        .case_(toIdl(ifElse.case_(), extraInputs))
        .other(
            ifElse.other().stream().map(x -> toIdl(x, extraInputs)).collect(toUnmodifiableList()))
        .elseNode(ifElse.elseNode() != null ? ifElse.elseNode().toIdl() : null)
        .build();
  }

  private static IfBlock toIdl(SdkIfBlock ifBlock, Map<String, Binding> extraInputs) {
    return IfBlock.builder()
        .condition(toIdl(ifBlock.condition(), extraInputs))
        .thenNode(ifBlock.thenNode().toIdl())
        .build();
  }

  private static BooleanExpression toIdl(
      SdkBooleanExpression condition, Map<String, Binding> extraInputs) {
    switch (condition.kind()) {
      case CONJUNCTION:
        return BooleanExpression.ofConjunction(toIdl(extraInputs, condition.conjunction()));
      case COMPARISON:
        return BooleanExpression.ofComparison(toIdl(extraInputs, condition.comparison()));
    }

    throw new AssertionError("Unexpected SdkBooleanExpression.Kind: " + condition.kind());
  }

  private static ConjunctionExpression toIdl(
      Map<String, Binding> extraInputs, SdkConjunctionExpression booleanExpression) {
    return ConjunctionExpression.create(
        booleanExpression.operator(),
        toIdl(booleanExpression.leftExpression(), extraInputs),
        toIdl(booleanExpression.rightExpression(), extraInputs));
  }

  private static ComparisonExpression toIdl(
      Map<String, Binding> extraInputs, SdkComparisonExpression<?> booleanExpression) {
    return ComparisonExpression.builder()
        .operator(booleanExpression.operator())
        .leftValue(toOperand(extraInputs, booleanExpression.left()))
        .rightValue(toOperand(extraInputs, booleanExpression.right()))
        .build();
  }

  private static Operand toOperand(Map<String, Binding> extraInputs, SdkBindingData<?> bindingData) {
    BindingData idl = bindingData.idl();

    // always allocate a new var name to make vars more predictable and easier to follow

    if (idl.kind() == BindingData.Kind.PROMISE) {
      String nextVarName = "$" + extraInputs.size();
      extraInputs.put(nextVarName, Binding.builder().binding(idl).var_(nextVarName).build());

      return Operand.ofVar(nextVarName);
    } else {
      return Operand.ofPrimitive(bindingData.idl().scalar().primitive());
    }
  }
}
