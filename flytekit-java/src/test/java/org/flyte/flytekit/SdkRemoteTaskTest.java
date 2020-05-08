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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;

class SdkRemoteTaskTest {

  @Test
  void applyShouldReturnASdkTaskNode() {
    SdkWorkflowBuilder builder = mock(SdkWorkflowBuilder.class);
    Map<String, SdkBindingData> inputs = new HashMap<>();
    inputs.put("a", SdkBindingData.ofInteger(1));
    inputs.put("b", SdkBindingData.ofInteger(2));
    SdkRemoteTask<Input, Output> remoteTask =
        SdkRemoteTask.<Input, Output>builder()
            .domain("dev")
            .project("project-a")
            .name("LookupTask")
            .inputs(SdkTypes.autoValue(Input.class))
            .outputs(SdkTypes.autoValue(Output.class))
            .build();

    SdkNode node = remoteTask.apply(builder, "lookup-endsong", inputs);

    assertThat(
        node,
        is(
            new SdkTaskNode(
                builder,
                "lookup-endsong",
                PartialTaskIdentifier.builder()
                    .domain("dev")
                    .project("project-a")
                    .name("LookupTask")
                    .build(),
                inputs,
                Collections.singletonMap(
                    "c",
                    Variable.builder()
                        .literalType(LiteralType.builder().simpleType(SimpleType.INTEGER).build())
                        .description("")
                        .build()))));
  }

  @AutoValue
  abstract static class Input {
    abstract long a();

    abstract long b();

    public static Input create(long a, long b) {
      return new AutoValue_SdkRemoteTaskTest_Input(a, b);
    }
  }

  @AutoValue
  abstract static class Output {
    abstract long c();

    public static Output create(long c) {
      return new AutoValue_SdkRemoteTaskTest_Output(c);
    }
  }
}
