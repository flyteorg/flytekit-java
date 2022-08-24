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
package org.flyte.localengine;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.flyte.api.v1.Node.START_NODE_ID;

import com.google.errorprone.annotations.InlineMe;
import com.google.errorprone.annotations.Var;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.ConjunctionExpression;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Primitive.Kind;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.WorkflowTemplate;

public class LocalEngine {
  private final ExecutionContext context;

  public LocalEngine(ExecutionContext context) {
    this.context = requireNonNull(context);
  }

  public Map<String, Literal> compileAndExecute(
      WorkflowTemplate template, Map<String, Literal> inputs) {
    List<ExecutionNode> executionNodes =
        new ExecutionNodeCompiler(context).compile(template.nodes());

    return execute(executionNodes, inputs, template.outputs());
  }

  private Map<String, Literal> execute(
      List<ExecutionNode> executionNodes,
      Map<String, Literal> workflowInputs,
      List<Binding> workflowOutputs) {

    ExecutionListener executionListener = context.executionListener();
    executionNodes.forEach(executionListener::pending);

    Map<String, Map<String, Literal>> nodeOutputs = new HashMap<>();
    nodeOutputs.put(START_NODE_ID, workflowInputs);

    for (ExecutionNode executionNode : executionNodes) {
      execute(executionNode, nodeOutputs);
    }

    return getLiteralMap(nodeOutputs, workflowOutputs);
  }

  private void execute(ExecutionNode executionNode, Map<String, Map<String, Literal>> nodeOutputs) {
    ExecutionListener executionListener = context.executionListener();
    Map<String, Literal> inputs = getLiteralMap(nodeOutputs, executionNode.bindings());

    executionListener.starting(executionNode, inputs);

    Map<String, Literal> outputs;
    if (executionNode.subWorkflow() != null) {
      outputs = compileAndExecute(executionNode.subWorkflow(), inputs);
    } else if (executionNode.runnableNode() != null) {
      outputs = runWithRetries(executionNode, inputs);
    } else if (executionNode.branchNode() != null) {
      outputs = executeConditionally(executionNode.branchNode(), inputs, nodeOutputs);
    } else {
      throw new IllegalArgumentException("Unrecognized execution node; " + executionNode);
    }

    Map<String, Literal> previous = nodeOutputs.put(executionNode.nodeId(), outputs);

    if (previous != null) {
      throw new IllegalStateException("invariant failed");
    }

    executionListener.completed(executionNode, inputs, outputs);
  }

  private Map<String, Literal> executeConditionally(ExecutionBranchNode branchNode, Map<String, Literal> inputs,
      Map<String, Map<String, Literal>> nodeOutputs) {
    for (ExecutionIfBlock ifBlock : branchNode.ifNodes()) {
      Optional<Map<String, Literal>> outputs;
      if ((outputs = executeConditionally(ifBlock, inputs, nodeOutputs)).isPresent()) {
        return outputs.get();
      }
    }

    context.executionListener().pending(branchNode.elseNode());
    execute(branchNode.elseNode(), nodeOutputs);
    return nodeOutputs.get(branchNode.elseNode().nodeId());
  }

  private Optional<Map<String, Literal>> executeConditionally(ExecutionIfBlock ifBlock, Map<String, Literal> inputs,
      Map<String, Map<String, Literal>> nodeOutputs) {
    if (!evaluate(ifBlock.condition(), inputs)) {
      return Optional.empty();
    }

    context.executionListener().pending(ifBlock.thenNode());
    execute(ifBlock.thenNode(), nodeOutputs);
    return Optional.of(nodeOutputs.get(ifBlock.thenNode().nodeId()));
  }

  private boolean evaluate(BooleanExpression condition, Map<String, Literal> inputs) {
    switch (condition.kind()) {
      case CONJUNCTION:
        return evaluate(condition.conjunction(), inputs);
      case COMPARISON:
        return evaluate(condition.comparison(), inputs);
    }
    throw new AssertionError("Unexpected BooleanExpression.Kind: " + condition.kind());
  }

  private boolean evaluate(ConjunctionExpression conjunction, Map<String, Literal> inputs) {
    boolean leftValue = evaluate(conjunction.leftExpression(), inputs);
    boolean rightValue = evaluate(conjunction.rightExpression(), inputs);

    switch (conjunction.operator()) {
      case AND:
        return leftValue && rightValue;
      case OR:
        return leftValue || rightValue;
    }

    throw new AssertionError("Unexpected ConjunctionExpression.LogicalOperator: " + conjunction.operator());
  }

  private boolean evaluate(ComparisonExpression comparison, Map<String, Literal> inputs) {
    Primitive left = resolve(comparison.leftValue(), inputs);
    Primitive right = resolve(comparison.rightValue(), inputs);
    switch (comparison.operator()) {
      case EQ:
        return eq(left, right);
      case NEQ:
        return neq(left, right);
      case GT:
        return gt(left, right);
      case GTE:
        return gte(left, right);
      case LT:
        return lt(left, right);
      case LTE:
        return lte(left, right);
    }

    throw new AssertionError("Unexpected ComparisonExpression.Operator: " + comparison.operator());
  }

  private boolean gt(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp > 0);

  }

  private boolean gte(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp >= 0);
  }

  private boolean lt(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp < 0);

  }

  private boolean lte(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp <= 0);
  }

  private boolean compare(Primitive left, Primitive right, Predicate<Integer> cmp) {
    return cmp.test(compare(left, right));
  }

  private int compare(Primitive left, Primitive right) {
    switch (left.kind()) {
      case INTEGER_VALUE:
        return compareIntegers(left, right);
      case FLOAT_VALUE:
        return compareFloats(left, right);
      case STRING_VALUE:
        return compare(left, right, Kind.STRING_VALUE, this::asString);
      case BOOLEAN_VALUE:
        return compare(left, right, Kind.BOOLEAN_VALUE, this::asBoolean);
      case DATETIME:
        return compare(left, right, Kind.DATETIME, this::asDateTime);
      case DURATION:
        return compare(left, right, Kind.DURATION, this::asDuration);
      default:
        throw new AssertionError("Unexpected Primitive.Kind:" + left.kind());
    }
  }

  private int compareIntegers(Primitive left,
      Primitive right) {
    long integerLeft = asInteger(left);
    switch (right.kind()) {
      case INTEGER_VALUE:
        long integerRight = asInteger(left);
        return Long.compare(integerLeft, integerRight);
      case FLOAT_VALUE:
        // type coercion
        double floatRight = asFloat(left);
        return Double.compare((double) integerLeft, floatRight);
      default: // fall out
    }
    throwPrimitivesNotCompatible(left, right);
    return 0; // unreachable
  }

  private int compareFloats(Primitive left,
      Primitive right) {
    double floatLeft = asFloat(left);
    switch (right.kind()) {
      case INTEGER_VALUE:
        // type coercion
        long integerRight = asInteger(left);
        return Double.compare(floatLeft, (double) integerRight);
      case FLOAT_VALUE:
        double floatRight = asFloat(left);
        return Double.compare(floatLeft, floatRight);
      default: // fall out
    }
    throwPrimitivesNotCompatible(left, right);
    return 0; // unreachable
  }

  private <T extends Comparable<T>> int compare(Primitive left, Primitive right,
      Kind expectedKind, Function<Primitive, T> converter) {
    if (!(left.kind() == right.kind() && left.kind() == expectedKind)) {
      throwPrimitivesNotCompatible(left, right);
    }
    T valueLeft = converter.apply(left);
    T valueRight = converter.apply(right);

    return valueLeft.compareTo(valueRight);
  }

  private void throwPrimitivesNotCompatible(Primitive left, Primitive right) {
    throw new IllegalArgumentException(String.format("Operands are not comparable: [%s] <-> [%s]", left, right));
  }

  private long asInteger(Primitive primitive) {
    if (primitive.kind() != Kind.INTEGER_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not an integer", primitive));
    }
    return primitive.integerValue();
  }

  private double asFloat(Primitive primitive) {
    if (primitive.kind() != Kind.FLOAT_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a float", primitive));
    }
    return primitive.floatValue();
  }

  private String asString(Primitive primitive) {
    if (primitive.kind() != Kind.STRING_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a string", primitive));
    }
    return primitive.stringValue();
  }
  
  private boolean asBoolean(Primitive primitive) {
    if (primitive.kind() != Kind.BOOLEAN_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a boolean", primitive));
    }
    return primitive.booleanValue();
  }

  private Instant asDateTime(Primitive primitive) {
    if (primitive.kind() != Kind.DATETIME) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a datetime", primitive));
    }
    return primitive.datetime();
  }

  private Duration asDuration(Primitive primitive) {
    if (primitive.kind() != Kind.DURATION) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a duration", primitive));
    }
    return primitive.duration();
  }

  private boolean eq(Primitive left, Primitive right) {
    return Objects.equals(left, right);
  }

  private boolean neq(Primitive left, Primitive right) {
    return !eq(left, right);
  }

  private Primitive resolve(Operand operand, Map<String, Literal> inputs) {
    switch (operand.kind()) {
      case PRIMITIVE:
        return operand.primitive();
      case VAR:
        Literal literal = inputs.get(operand.var());
        if (literal == null) {
          throw new IllegalArgumentException("asassass"); //XXX
        } else if (literal.scalar() == null) {
          throw new IllegalArgumentException("asassass"); //XXX
        } else if (literal.scalar().primitive() == null) {
          throw new IllegalArgumentException("asassass"); //XXX
        }
        return literal.scalar().primitive();
    }
    return null;
  }

  Map<String, Literal> runWithRetries(ExecutionNode executionNode, Map<String, Literal> inputs) {
    ExecutionListener listener = context.executionListener();
    int attempts = executionNode.attempts();
    @Var int attempt = 0;

    if (attempts <= 0) {
      throw new IllegalStateException("invariant failed: attempts > 0");
    }

    while (true) {
      try {
        attempt++;

        return executionNode.runnableNode().run(inputs);
      } catch (Throwable e) {
        if (!isRecoverable(e) || attempt >= attempts) {
          listener.error(executionNode, inputs, e);
          throw e;
        } else {
          listener.retrying(executionNode, inputs, e, attempt);
        }
      }
    }
  }

  private static boolean isRecoverable(Throwable e) {
    if (e instanceof ContainerError) {
      ContainerError containerError = (ContainerError) e;

      return containerError.getKind() != ContainerError.Kind.NON_RECOVERABLE;
    }

    return true;
  }

  // TODO we need to take interface into account to do type casting
  static Map<String, Literal> getLiteralMap(
      Map<String, Map<String, Literal>> nodeOutputs, List<Binding> bindings) {
    return bindings.stream()
        .collect(toMap(Binding::var_, binding -> getLiteral(nodeOutputs, binding.binding())));
  }

  static Literal getLiteral(
      Map<String, Map<String, Literal>> nodeOutputs, BindingData bindingData) {
    switch (bindingData.kind()) {
      case SCALAR:
        return Literal.ofScalar(bindingData.scalar());

      case COLLECTION:
        return Literal.ofCollection(
            bindingData.collection().stream()
                .map(binding -> getLiteral(nodeOutputs, binding))
                .collect(toList()));

      case PROMISE:
        String nodeId = bindingData.promise().nodeId();
        Map<String, Literal> outputs = nodeOutputs.get(nodeId);

        requireNonNull(outputs, () -> String.format("missing output for node [%s]", nodeId));

        return outputs.get(bindingData.promise().var());

      case MAP:
        return Literal.ofMap(
            bindingData.map().entrySet().stream()
                .map(
                    entry ->
                        new SimpleImmutableEntry<>(
                            entry.getKey(), getLiteral(nodeOutputs, entry.getValue())))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    throw new AssertionError("Unexpected BindingData.Kind: " + bindingData.kind());
  }

  // Deprecated static methods
  @Deprecated
  @InlineMe(
      replacement =
          "new LocalEngine(ExecutionContext.builder().runnableTasks(runnableTasks)"
              + ".dynamicWorkflowTasks(dynamicWorkflowTasks).build()).compileAndExecute(template, inputs)",
      imports = {"org.flyte.localengine.ExecutionContext", "org.flyte.localengine.LocalEngine"})
  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, Literal> inputs) {
    return new LocalEngine(
            ExecutionContext.builder()
                .runnableTasks(runnableTasks)
                .dynamicWorkflowTasks(dynamicWorkflowTasks)
                .build())
        .compileAndExecute(template, inputs);
  }

  @Deprecated
  @InlineMe(
      replacement =
          "new LocalEngine(ExecutionContext.builder().runnableTasks(runnableTasks)"
              + ".dynamicWorkflowTasks(dynamicWorkflowTasks).workflowTemplates(workflowTemplates).build())"
              + ".compileAndExecute(template, inputs)",
      imports = {"org.flyte.localengine.ExecutionContext", "org.flyte.localengine.LocalEngine"})
  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, WorkflowTemplate> workflowTemplates,
      Map<String, Literal> inputs) {
    return new LocalEngine(
            ExecutionContext.builder()
                .runnableTasks(runnableTasks)
                .dynamicWorkflowTasks(dynamicWorkflowTasks)
                .workflowTemplates(workflowTemplates)
                .build())
        .compileAndExecute(template, inputs);
  }

  @Deprecated
  @InlineMe(
      replacement =
          "new LocalEngine(ExecutionContext.builder().runnableTasks(runnableTasks)"
              + ".dynamicWorkflowTasks(dynamicWorkflowTasks).executionListener(listener).build())"
              + ".compileAndExecute(template, inputs)",
      imports = {"org.flyte.localengine.ExecutionContext", "org.flyte.localengine.LocalEngine"})
  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, Literal> inputs,
      ExecutionListener listener) {
    return new LocalEngine(
            ExecutionContext.builder()
                .runnableTasks(runnableTasks)
                .dynamicWorkflowTasks(dynamicWorkflowTasks)
                .executionListener(listener)
                .build())
        .compileAndExecute(template, inputs);
  }

  @Deprecated
  @InlineMe(
      replacement =
          "new LocalEngine(ExecutionContext.builder().runnableTasks(runnableTasks)"
              + ".dynamicWorkflowTasks(dynamicWorkflowTasks).executionListener(listener)"
              + ".workflowTemplates(workflowTemplates).build()).compileAndExecute(template, inputs)",
      imports = {"org.flyte.localengine.ExecutionContext", "org.flyte.localengine.LocalEngine"})
  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, WorkflowTemplate> workflowTemplates,
      Map<String, Literal> inputs,
      ExecutionListener listener) {
    return new LocalEngine(
            ExecutionContext.builder()
                .runnableTasks(runnableTasks)
                .dynamicWorkflowTasks(dynamicWorkflowTasks)
                .executionListener(listener)
                .workflowTemplates(workflowTemplates)
                .build())
        .compileAndExecute(template, inputs);
  }
}
