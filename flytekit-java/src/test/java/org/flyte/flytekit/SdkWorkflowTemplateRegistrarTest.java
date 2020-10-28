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
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.junit.jupiter.api.Test;

public class SdkWorkflowTemplateRegistrarTest {

  @Test
  public void testLoad() {
    SdkConfig sdkConfig =
        SdkConfig.builder().domain("domain").project("project").version("version").build();

    List<SdkWorkflow> sdkWorkflows =
        Arrays.asList(new TestWorkflow("workflow1"), new TestWorkflow("workflow2"));

    Map<WorkflowIdentifier, WorkflowTemplate> workflows =
        new SdkWorkflowTemplateRegistrar().load(sdkConfig, sdkWorkflows);

    // not testing idl conversion logic because it's tested in SdkWorkflowBuilderTest

    assertThat(
        workflows.keySet(),
        containsInAnyOrder(
            WorkflowIdentifier.builder()
                .name("workflow1")
                .domain("domain")
                .project("project")
                .version("version")
                .build(),
            WorkflowIdentifier.builder()
                .name("workflow2")
                .domain("domain")
                .project("project")
                .version("version")
                .build()));
  }

  private static class TestWorkflow extends SdkWorkflow {
    private final String name;

    private TestWorkflow(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {}
  }
}
