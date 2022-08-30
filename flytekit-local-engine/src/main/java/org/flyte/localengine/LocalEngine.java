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
import static org.flyte.localengine.BooleanExpressionEvaluator.evaluate;

import com.google.errorprone.annotations.InlineMe;
import com.google.errorprone.annotations.Var;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.Literal;
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

  private Map<String, Literal> execute(
      ExecutionNode executionNode, Map<String, Map<String, Literal>> nodeOutputs) {
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
    return outputs;
  }

  private Map<String, Literal> executeConditionally(
      ExecutionBranchNode branchNode,
      Map<String, Literal> inputs,
      Map<String, Map<String, Literal>> nodeOutputs) {
    for (ExecutionIfBlock ifBlock : branchNode.ifNodes()) {
      Map<String, Literal> outputs;
      if ((outputs = executeConditionally(ifBlock, inputs, nodeOutputs)) != null) {
        return outputs;
      }
    }

    if (branchNode.elseNode() != null) {
      context.executionListener().pending(branchNode.elseNode());
      return execute(branchNode.elseNode(), nodeOutputs);
    }

    assert branchNode.error() != null;
    throw new IllegalArgumentException(
        String.format("No cases matched for branch node [%s]", branchNode.error().failedNodeId()));
  }

  private Map<String, Literal> executeConditionally(
      ExecutionIfBlock ifBlock,
      Map<String, Literal> inputs,
      Map<String, Map<String, Literal>> nodeOutputs) {
    if (!evaluate(ifBlock.condition(), inputs)) {
      return null;
    }

    context.executionListener().pending(ifBlock.thenNode());
    return execute(ifBlock.thenNode(), nodeOutputs);
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
