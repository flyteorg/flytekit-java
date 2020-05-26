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
package org.flyte.flytekit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.flyte.flytekit.SdkWorkflowBuilder.literalOfInteger;
import static org.junit.jupiter.api.Assertions.*;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;
import org.junit.jupiter.api.Test;

class SdkWorkflowBuilderTest {

  @Test
  void testToIdlTemplate() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    new Times2Workflow().expand(builder);

    WorkflowTemplate expected =
        WorkflowTemplate.builder()
            .metadata(WorkflowMetadata.builder().build())
            .interface_(expectedInterface())
            .outputs(expectedOutputs())
            .nodes(expectedNodes())
            .build();
    assertEquals(expected, builder.toIdlTemplate());
  }

  private TypedInterface expectedInterface() {
    return TypedInterface.builder()
        .inputs(
            singletonMap(
                "in",
                Variable.builder()
                    .literalType(LiteralTypes.INTEGER)
                    .description("Enter value to square")
                    .build()))
        .outputs(
            singletonMap(
                "out",
                Variable.builder().literalType(LiteralTypes.INTEGER).description("").build()))
        .build();
  }

  private List<Binding> expectedOutputs() {
    return singletonList(
        Binding.builder()
            .var_("out")
            .binding(BindingData.of(OutputReference.builder().var("c").nodeId("square").build()))
            .build());
  }

  private List<Node> expectedNodes() {
    return singletonList(
        Node.builder()
            .id("square")
            .taskNode(
                TaskNode.builder()
                    .referenceId(
                        PartialTaskIdentifier.builder()
                            .name("org.flyte.flytekit.SdkWorkflowBuilderTest$MultiplicationTask")
                            .build())
                    .build())
            .inputs(
                Arrays.asList(
                    Binding.builder()
                        .var_("a")
                        .binding(
                            BindingData.of(
                                OutputReference.builder()
                                    .var("in")
                                    .nodeId(Node.START_NODE_ID)
                                    .build()))
                        .build(),
                    Binding.builder()
                        .var_("b")
                        .binding(BindingData.of(Scalar.of(Primitive.ofInteger(2L))))
                        .build()))
            .upstreamNodeIds(emptyList())
            .build());
  }

  private static class Times2Workflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData in = builder.inputOfInteger("in", "Enter value to square");
      SdkBindingData two = literalOfInteger(2L);
      SdkBindingData out =
          builder.mapOf("a", in, "b", two).apply("square", new MultiplicationTask()).getOutput("c");
      builder.output("out", out);
    }
  }

  static class MultiplicationTask
      extends SdkRunnableTask<MultiplicationTask.Input, MultiplicationTask.Output> {
    private static final long serialVersionUID = -1971936360636181781L;

    MultiplicationTask() {
      super(SdkTypes.autoValue(Input.class), SdkTypes.autoValue(Output.class));
    }

    @AutoValue
    abstract static class Input {
      abstract long a();

      abstract long b();
    }

    @AutoValue
    abstract static class Output {
      abstract long c();

      public static Output create(long c) {
        return new AutoValue_SdkWorkflowBuilderTest_MultiplicationTask_Output(c);
      }
    }

    @Override
    public Output run(Input input) {
      return Output.create(input.a() * input.b());
    }
  }
}
