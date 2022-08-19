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

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.flyte.api.v1.Node.START_NODE_ID;

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

  @Deprecated
//  @InlineMe(
//      replacement =
//          "LocalEngine.compileAndExecute(template, runnableTasks, dynamicWorkflowTasks, emptyMap(), "
//              + "inputs, NoopExecutionListener.create())",
//      imports = {
//        "org.flyte.localengine.LocalEngine",
//        "org.flyte.localengine.NoopExecutionListener"
//      },
//      staticImports = "java.util.Collections.emptyMap")
  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, Literal> inputs) {
    return compileAndExecute(
        template,
        runnableTasks,
        dynamicWorkflowTasks,
        emptyMap(),
        emptyMap(),
        inputs,
        NoopExecutionListener.create());
  }

  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, WorkflowTemplate> workflows,
      Map<String, RunnableTask> mockLaunchPlans,
      Map<String, Literal> inputs) {
    return compileAndExecute(
        template,
        runnableTasks,
        dynamicWorkflowTasks,
        workflows,
        mockLaunchPlans,
        inputs,
        NoopExecutionListener.create());
  }

  @Deprecated
//  @InlineMe(
//      replacement =
//          "LocalEngine.compileAndExecute(template, runnableTasks, dynamicWorkflowTasks, emptyMap(), inputs, listener)",
//      imports = "org.flyte.localengine.LocalEngine",
//      staticImports = "java.util.Collections.emptyMap")
  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, RunnableTask> mockLaunchPlans,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, Literal> inputs,
      ExecutionListener listener) {
    return compileAndExecute(
        template, runnableTasks, dynamicWorkflowTasks, emptyMap(), mockLaunchPlans, inputs, listener);
  }

  public static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, WorkflowTemplate> workflows,
      Map<String, RunnableTask> mockLaunchPlans,
      Map<String, Literal> inputs,
      ExecutionListener listener) {
    List<ExecutionNode> executionNodes =
        ExecutionNodeCompiler.compile(
            template.nodes(), runnableTasks, dynamicWorkflowTasks, workflows, mockLaunchPlans);

    return execute(
        executionNodes,
        runnableTasks,
        dynamicWorkflowTasks,
        workflows,
        mockLaunchPlans,
        inputs,
        template.outputs(),
        listener);
  }

  static Map<String, Literal> execute(
      List<ExecutionNode> executionNodes,
      Map<String, RunnableTask> runnableTasks,
      Map<String, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<String, WorkflowTemplate> workflows,
      Map<String, RunnableTask> mockLaunchPlans,
      Map<String, Literal> workflowInputs,
      List<Binding> bindings,
      ExecutionListener listener) {

    executionNodes.forEach(listener::pending);

    Map<String, Map<String, Literal>> nodeOutputs = new HashMap<>();
    nodeOutputs.put(START_NODE_ID, workflowInputs);

    for (ExecutionNode executionNode : executionNodes) {
      Map<String, Literal> inputs = getLiteralMap(nodeOutputs, executionNode.bindings());

      listener.starting(executionNode, inputs);

      Map<String, Literal> outputs;
      if (executionNode.subWorkflow() != null) {
        outputs =
            compileAndExecute(
                executionNode.subWorkflow(),
                runnableTasks,
                dynamicWorkflowTasks,
                workflows,
                mockLaunchPlans,
                inputs,
                listener);
      } else {
        // this must be a task or a mock launch plan
        outputs = runWithRetries(executionNode, inputs, listener);
      }

      Map<String, Literal> previous = nodeOutputs.put(executionNode.nodeId(), outputs);

      if (previous != null) {
        throw new IllegalStateException("invariant failed");
      }

      listener.completed(executionNode, inputs, outputs);
    }

    return getLiteralMap(nodeOutputs, bindings);
  }

  static Map<String, Literal> runWithRetries(
      ExecutionNode executionNode, Map<String, Literal> inputs, ExecutionListener listener) {
    int attempts = executionNode.attempts();
    @Var int attempt = 0;

    if (attempts <= 0) {
      throw new IllegalStateException("invariant failed: attempts > 0");
    }

    while (true) {
      try {
        attempt++;

        return executionNode.runnableTask().run(inputs);
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
}
