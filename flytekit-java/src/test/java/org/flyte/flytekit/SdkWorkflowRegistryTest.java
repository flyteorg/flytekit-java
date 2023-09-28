/*
 * Copyright 2020-2023 Flyte Authors
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
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import org.junit.jupiter.api.Test;

public class SdkWorkflowRegistryTest {

  @Test
  public void testLoadAll() {
    SdkWorkflow<Void, Void> workflow1 = new TestWorkflow();
    SdkWorkflow<Void, Void> workflow2 = new TestWorkflow();
    SdkWorkflow<Void, Void> workflow3 = new TestWorkflow();

    List<SdkWorkflow<?, ?>> workflows =
        SdkWorkflowRegistry.loadAll(
            List.of(
                new SimpleSdkWorkflowRegistry(List.of(workflow1)),
                new SimpleSdkWorkflowRegistry(List.of(workflow2, workflow3))));

    assertThat(workflows, containsInAnyOrder(workflow1, workflow2, workflow3));
  }

  static class SimpleSdkWorkflowRegistry extends SdkWorkflowRegistry {
    private final List<SdkWorkflow<?, ?>> workflows;

    public SimpleSdkWorkflowRegistry(List<SdkWorkflow<?, ?>> workflows) {
      this.workflows = workflows;
    }

    @Override
    public List<SdkWorkflow<?, ?>> getWorkflows() {
      return workflows;
    }
  }

  private static class TestWorkflow extends SdkWorkflow<Void, Void> {
    private TestWorkflow() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public Void expand(SdkWorkflowBuilder builder, Void noInput) {
      return null;
    }
  }
}
