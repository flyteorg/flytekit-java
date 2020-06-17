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

import static org.flyte.flytekit.SdkBindingData.ofString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.value.AutoValue;
import org.junit.jupiter.api.Test;

public class SdkBindingTest {

  public static final SdkRemoteTask<Void, FakeOutput> FAKE_TASK =
      SdkRemoteTask.create(
          "development",
          "flytetester",
          "FakeTask",
          SdkTypes.nulls(),
          SdkTypes.autoValue(FakeOutput.class));

  @Test
  public void testPutDuplicates() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> builder.mapOf("name1", ofString("value1"), "name1", ofString("value2")));

    assertEquals("Duplicate input: [name1]", e.getMessage());
  }

  @Test
  public void testPutNodesWithDuplicateOutputs() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkNode node1 = builder.apply("node-1", FAKE_TASK);
    SdkNode node2 = builder.apply("node-2", FAKE_TASK);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> builder.tupleOf(node1, node2));

    assertEquals("Duplicate inputs: [output1]", e.getMessage());
  }

  @AutoValue
  abstract static class FakeOutput {
    abstract String output1();
  }
}
