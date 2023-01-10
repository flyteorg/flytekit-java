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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;
import org.junit.jupiter.api.Test;

public class SdkWorkflowWithSdkRemoteLaunchPlanTest {
  @Test
  void applyShouldReturnASdkWorkflowNode() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    new WorkflowExample().expand(builder);

    Node expectedNode =
        Node.builder()
            .id("some-node-id")
            .workflowNode(
                WorkflowNode.builder()
                    .reference(
                        WorkflowNode.Reference.ofLaunchPlanRef(
                            PartialLaunchPlanIdentifier.builder()
                                .domain("dev")
                                .project("project-a")
                                .name("SomeLaunchPlan")
                                .version("version")
                                .build()))
                    .build())
            .upstreamNodeIds(Collections.emptyList())
            .inputs(
                Arrays.asList(
                    Binding.builder()
                        .var_("a")
                        .binding(
                            BindingData.ofOutputReference(
                                OutputReference.builder().nodeId("start-node").var("a").build()))
                        .build(),
                    Binding.builder()
                        .var_("b")
                        .binding(
                            BindingData.ofOutputReference(
                                OutputReference.builder().nodeId("start-node").var("b").build()))
                        .build()))
            .build();
    WorkflowTemplate expected =
        WorkflowTemplate.builder()
            .metadata(WorkflowMetadata.builder().build())
            .interface_(expectedInterface())
            .outputs(expectedOutputs())
            .nodes(singletonList(expectedNode))
            .build();

    assertEquals(expected, builder.toIdlTemplate());
  }

  private TypedInterface expectedInterface() {
    Map<String, Variable> inputs = new HashMap<>();
    inputs.put("a", Variable.builder().literalType(LiteralTypes.INTEGER).description("").build());
    inputs.put("b", Variable.builder().literalType(LiteralTypes.INTEGER).description("").build());
    return TypedInterface.builder()
        .inputs(inputs)
        .outputs(
            singletonMap(
                "o", Variable.builder().literalType(LiteralTypes.BOOLEAN).description("").build()))
        .build();
  }

  private List<Binding> expectedOutputs() {
    return singletonList(
        Binding.builder()
            .var_("o")
            .binding(
                BindingData.ofOutputReference(
                    OutputReference.builder().var("o").nodeId("some-node-id").build()))
            .build());
  }

  public static class WorkflowExample extends SdkWorkflow<TestUnaryBooleanOutput> {
    public WorkflowExample() {
      super(new TestUnaryBooleanOutput.SdkType());
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData<Long> a = builder.inputOfInteger("a");
      SdkBindingData<Long> b = builder.inputOfInteger("b");

      SdkNode<TestUnaryBooleanOutput> node1 =
          builder.apply(
              "some-node-id", new TestSdkRemoteLaunchPlan().withInput("a", a).withInput("b", b));

      builder.output("o", node1.getOutputs().o());
    }
  }

  @SuppressWarnings("ExtendsAutoValue")
  static class TestSdkRemoteLaunchPlan
      extends SdkRemoteLaunchPlan<TestPairIntegerInput, TestUnaryBooleanOutput> {

    @Override
    public String domain() {
      return "dev";
    }

    @Override
    public String project() {
      return "project-a";
    }

    @Override
    public String name() {
      return "SomeLaunchPlan";
    }

    @Override
    public String version() {
      return "version";
    }

    @Override
    public SdkType<TestPairIntegerInput> inputs() {
      return new TestPairIntegerInput.SdkType();
    }

    @Override
    public SdkType<TestUnaryBooleanOutput> outputs() {
      return new TestUnaryBooleanOutput.SdkType();
    }

    static class Output {
      private final boolean c;

      public Output(boolean c) {
        this.c = c;
      }

      public boolean c() {
        return c;
      }
    }
  }
}
