/*
 * Copyright 2020 Spotify AB.
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
package org.flyte.jflyte;

import static org.flyte.api.v1.Node.START_NODE_ID;

import com.google.common.base.Verify;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.WorkflowTemplate;

public class LocalRunner {

  static Map<String, Literal> compileAndExecute(
      WorkflowTemplate template,
      Map<String, RunnableTask> runnableTasks,
      Map<String, Literal> inputs) {
    List<ExecutionNode> executionNodes =
        ExecutionNodeCompiler.compile(template.nodes(), runnableTasks);

    return execute(executionNodes, inputs, template.outputs());
  }

  static Map<String, Literal> execute(
      List<ExecutionNode> executionNodes,
      Map<String, Literal> workflowInputs,
      List<Binding> bindings) {

    Map<String, Map<String, Literal>> nodeOutputs = new HashMap<>();
    nodeOutputs.put(START_NODE_ID, workflowInputs);

    for (ExecutionNode executionNode : executionNodes) {
      Map<String, Literal> inputs = getLiteralMap(nodeOutputs, executionNode.bindings());

      Map<String, Literal> outputs = executionNode.runnableTask().run(inputs);
      Map<String, Literal> previous = nodeOutputs.put(executionNode.nodeId(), outputs);

      nodeOutputs.put(executionNode.nodeId(), outputs);

      Verify.verify(previous == null, "invariant failed");
    }

    return getLiteralMap(nodeOutputs, bindings);
  }

  // TODO we need to take interface into account to do type casting

  static Map<String, Literal> getLiteralMap(
      Map<String, Map<String, Literal>> nodeOutputs, List<Binding> bindings) {
    return bindings.stream()
        .collect(
            Collectors.toMap(Binding::var_, binding -> getLiteral(nodeOutputs, binding.binding())));
  }

  static Literal getLiteral(
      Map<String, Map<String, Literal>> nodeOutputs, BindingData bindingData) {
    switch (bindingData.kind()) {
      case SCALAR:
        return Literal.of(bindingData.scalar());

      case PROMISE:
        String nodeId = bindingData.promise().nodeId();
        Map<String, Literal> outputs = nodeOutputs.get(nodeId);

        Verify.verifyNotNull(outputs, "missing output for node [%s]", nodeId);

        return outputs.get(bindingData.promise().var());
    }

    throw new AssertionError("Unexpected BindingData.Kind: " + bindingData.kind());
  }
}
