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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.TaskNode;
import org.junit.jupiter.api.Test;

class SdkRemoteTaskTest {

  @Test
  void applyShouldReturnASdkTaskNode() {
    Map<String, SdkBindingData> inputs = new HashMap<>();
    inputs.put("a", SdkBindingData.ofInteger(1));
    inputs.put("b", SdkBindingData.ofString("2"));
    SdkRemoteTask<Input, Output> remoteTask =
        SdkRemoteTask.<Input, Output>builder()
            .domain("dev")
            .project("project-a")
            .name("LookupTask")
            .inputs(SdkTypes.autoValue(Input.class))
            .outputs(SdkTypes.autoValue(Output.class))
            .build();

    SdkNode node = remoteTask.apply(mock(SdkWorkflowBuilder.class), "lookup-endsong", inputs);

    assertThat(node.getNodeId(), is("lookup-endsong"));
    assertThat(
        node.toIdl(),
        is(
            Node.builder()
                .id("lookup-endsong")
                .taskNode(
                    TaskNode.builder()
                        .referenceId(
                            PartialTaskIdentifier.builder()
                                .domain("dev")
                                .project("project-a")
                                .name("LookupTask")
                                .build())
                        .build())
                .inputs(
                    Arrays.asList(
                        Binding.builder()
                            .var_("a")
                            .binding(BindingData.of(Scalar.of(Primitive.ofInteger(1))))
                            .build(),
                        Binding.builder()
                            .var_("b")
                            .binding(BindingData.of(Scalar.of(Primitive.ofString("2"))))
                            .build()))
                .build()));
    assertThat(
        node.getOutputs(),
        is(Collections.singletonMap("c", SdkBindingData.ofOutputReference("lookup-endsong", "c"))));
  }

  @AutoValue
  abstract static class Input {
    abstract long a();

    abstract String b();

    public static Input create(long a, String b) {
      return new AutoValue_SdkRemoteTaskTest_Input(a, b);
    }
  }

  @AutoValue
  abstract static class Output {
    abstract boolean c();

    public static Output create(boolean c) {
      return new AutoValue_SdkRemoteTaskTest_Output(c);
    }
  }
}
