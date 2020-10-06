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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.WorkflowIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class IdentifierRewriteTest {

  @Mock private FlyteAdminClient client;
  private IdentifierRewrite rewriter;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
    rewriter =
        IdentifierRewrite.builder()
            .adminClient(client)
            .project("rewritten-project")
            .domain("rewritten-domain")
            .version("rewritten-version")
            .build();
  }

  @Test
  void shouldRewriteWorkflowIdentifierWithLatestIdAndDomainForRewriter() {
    when(client.fetchLatestWorkflowId(
            NamedEntityIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-workflow")
                .build()))
        .thenReturn(
            WorkflowIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-workflow")
                .version("latest-version")
                .build());

    LaunchPlan rewrittenLaunchPlan =
        rewriter.apply(
            launchPlan(
                PartialWorkflowIdentifier.builder()
                    .project("external-project")
                    .name("external-workflow")
                    .build()));

    assertThat(
        rewrittenLaunchPlan,
        equalTo(
            launchPlan(
                PartialWorkflowIdentifier.builder()
                    .project("external-project")
                    .domain("rewritten-domain")
                    .name("external-workflow")
                    .version("latest-version")
                    .build())));
  }

  @Test
  void shouldRewriteWorkflowIdentifierWithValuesFromRewriterWhenOnlyNameIsSet() {
    LaunchPlan rewrittenLaunchPlan =
        rewriter.apply(
            launchPlan(PartialWorkflowIdentifier.builder().name("internal-workflow").build()));

    assertThat(
        rewrittenLaunchPlan,
        equalTo(
            launchPlan(
                PartialWorkflowIdentifier.builder()
                    .project("rewritten-project")
                    .domain("rewritten-domain")
                    .name("internal-workflow")
                    .version("rewritten-version")
                    .build())));
  }

  @Test
  void shouldNotRewriteWorkflowIdentifierWhenVersionIsSet() {
    LaunchPlan rewrittenLaunchPlan =
        rewriter.apply(
            launchPlan(
                PartialWorkflowIdentifier.builder()
                    .project("external-project")
                    .domain("external-domain")
                    .name("external-workflow")
                    .version("external-version")
                    .build()));

    assertThat(
        rewrittenLaunchPlan,
        equalTo(
            launchPlan(
                PartialWorkflowIdentifier.builder()
                    .project("external-project")
                    .domain("external-domain")
                    .name("external-workflow")
                    .version("external-version")
                    .build())));
    verifyNoInteractions(client);
  }

  @Test
  void shouldRewriteWorkflowsAllowingPingingVersionForWorkflowsSameProject() {
    LaunchPlan rewrittenLaunchPlan =
        rewriter.apply(
            launchPlan(
                PartialWorkflowIdentifier.builder()
                    .name("internal-workflow")
                    .version("pinned-version")
                    .build()));

    assertThat(
        rewrittenLaunchPlan,
        equalTo(
            launchPlan(
                PartialWorkflowIdentifier.builder()
                    .project("rewritten-project")
                    .domain("rewritten-domain")
                    .name("internal-workflow")
                    .version("pinned-version")
                    .build())));
    verifyNoInteractions(client);
  }

  private LaunchPlan launchPlan(PartialWorkflowIdentifier workflowId) {
    return LaunchPlan.builder()
        .name("launch-plan-name")
        .workflowId(workflowId)
        .fixedInputs(
            Collections.singletonMap(
                "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("bar")))))
        .build();
  }
}
