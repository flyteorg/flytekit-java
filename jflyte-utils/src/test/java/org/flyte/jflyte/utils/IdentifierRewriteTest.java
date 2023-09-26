/*
 * Copyright 2020-2023 Flyte Authors.
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
package org.flyte.jflyte.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.Parameter;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowNode;
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
  void shouldRewriteTaskIdentifierWithLatestIdAndDomainForRewriter() {
    when(client.fetchLatestTaskId(
            NamedEntityIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-task")
                .build()))
        .thenReturn(
            TaskIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-task")
                .version("latest-version")
                .build());

    PartialTaskIdentifier rewrittenTaskId =
        rewriter.apply(
            PartialTaskIdentifier.builder()
                .project("external-project")
                .name("external-task")
                .build());

    assertThat(
        rewrittenTaskId,
        equalTo(
            PartialTaskIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-task")
                .version("latest-version")
                .build()));
  }

  @Test
  void shouldRewriteTaskIdentifierWithValuesFromRewriterWhenOnlyNameIsSet() {
    PartialTaskIdentifier rewrittenTaskId =
        rewriter.apply(PartialTaskIdentifier.builder().name("internal-task").build());

    assertThat(
        rewrittenTaskId,
        equalTo(
            PartialTaskIdentifier.builder()
                .project("rewritten-project")
                .domain("rewritten-domain")
                .name("internal-task")
                .version("rewritten-version")
                .build()));
  }

  @Test
  void shouldNotRewriteTaskIdentifierWhenVersionIsSet() {
    PartialTaskIdentifier rewrittenTaskId =
        rewriter.apply(
            PartialTaskIdentifier.builder()
                .project("external-project")
                .domain("external-domain")
                .name("external-task")
                .version("external-version")
                .build());

    assertThat(
        rewrittenTaskId,
        equalTo(
            PartialTaskIdentifier.builder()
                .project("external-project")
                .domain("external-domain")
                .name("external-task")
                .version("external-version")
                .build()));
    verifyNoInteractions(client);
  }

  @Test
  void shouldRewriteTaskAllowingPingingVersionForTasksSameProject() {
    PartialTaskIdentifier rewrittenTaskId =
        rewriter.apply(
            PartialTaskIdentifier.builder()
                .name("internal-task")
                .version("pinned-version")
                .build());

    assertThat(
        rewrittenTaskId,
        equalTo(
            PartialTaskIdentifier.builder()
                .project("rewritten-project")
                .domain("rewritten-domain")
                .name("internal-task")
                .version("pinned-version")
                .build()));
    verifyNoInteractions(client);
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

  @Test
  void shouldRewriteBranchNodes() {
    ComparisonExpression comparison =
        ComparisonExpression.builder()
            .operator(ComparisonExpression.Operator.EQ)
            .leftValue(Operand.ofVar("a"))
            .rightValue(Operand.ofVar("b"))
            .build();

    BooleanExpression condition = BooleanExpression.ofComparison(comparison);
    PartialTaskIdentifier partialReference =
        PartialTaskIdentifier.builder().name("task-name").build();
    PartialTaskIdentifier rewrittenReference =
        PartialTaskIdentifier.builder()
            .name("task-name")
            .domain("rewritten-domain")
            .version("rewritten-version")
            .project("rewritten-project")
            .build();

    TaskNode partialTaskNode = TaskNode.builder().referenceId(partialReference).build();
    TaskNode rewrittenTaskNode = TaskNode.builder().referenceId(rewrittenReference).build();

    Node partialNode =
        Node.builder()
            .id("node-1")
            .inputs(ImmutableList.of())
            .upstreamNodeIds(ImmutableList.of())
            .taskNode(partialTaskNode)
            .build();

    Node rewrittenNode =
        Node.builder()
            .id("node-1")
            .inputs(ImmutableList.of())
            .upstreamNodeIds(ImmutableList.of())
            .taskNode(rewrittenTaskNode)
            .build();

    IfBlock partialIfBlock = IfBlock.builder().condition(condition).thenNode(partialNode).build();

    IfBlock rewrittenIfBlock =
        IfBlock.builder().condition(condition).thenNode(rewrittenNode).build();

    BranchNode partialBranchNode =
        BranchNode.builder()
            .ifElse(
                IfElseBlock.builder()
                    .case_(partialIfBlock)
                    .other(ImmutableList.of(partialIfBlock))
                    .elseNode(partialNode)
                    .build())
            .build();

    BranchNode rewrittenBranchNode =
        BranchNode.builder()
            .ifElse(
                IfElseBlock.builder()
                    .case_(rewrittenIfBlock)
                    .other(ImmutableList.of(rewrittenIfBlock))
                    .elseNode(rewrittenNode)
                    .build())
            .build();

    assertThat(rewriter.visitor().visitBranchNode(partialBranchNode), equalTo(rewrittenBranchNode));
  }

  @Test
  void shouldRewriteLaunchPlanIdentifierWithLatestIdAndDomainForRewriter() {
    when(client.fetchLatestLaunchPlanId(
            NamedEntityIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-lp")
                .build()))
        .thenReturn(
            LaunchPlanIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-lp")
                .version("latest-version")
                .build());

    PartialLaunchPlanIdentifier rewrittenLaunchPlanId =
        rewriter.apply(
            PartialLaunchPlanIdentifier.builder()
                .project("external-project")
                .name("external-lp")
                .build());

    assertThat(
        rewrittenLaunchPlanId,
        equalTo(
            PartialLaunchPlanIdentifier.builder()
                .project("external-project")
                .domain("rewritten-domain")
                .name("external-lp")
                .version("latest-version")
                .build()));
  }

  @Test
  void shouldRewriteWorkflowNodeForSubWorkflowRef() {
    PartialWorkflowIdentifier identifier = PartialWorkflowIdentifier.builder().name("name").build();

    PartialWorkflowIdentifier rewrittenIdentifier =
        PartialWorkflowIdentifier.builder()
            .name("name")
            .domain("rewritten-domain")
            .version("rewritten-version")
            .project("rewritten-project")
            .build();

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(identifier))
            .build();

    assertThat(
        rewriter.visitor().visitWorkflowNode(workflowNode),
        equalTo(
            WorkflowNode.builder()
                .reference(WorkflowNode.Reference.ofSubWorkflowRef(rewrittenIdentifier))
                .build()));
  }

  @Test
  void shouldRewriteWorkflowNodeForLaunchPlanRef() {
    PartialLaunchPlanIdentifier identifier =
        PartialLaunchPlanIdentifier.builder().name("name").build();

    PartialLaunchPlanIdentifier rewrittenIdentifier =
        PartialLaunchPlanIdentifier.builder()
            .name("name")
            .domain("rewritten-domain")
            .version("rewritten-version")
            .project("rewritten-project")
            .build();

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofLaunchPlanRef(identifier))
            .build();

    assertThat(
        rewriter.visitor().visitWorkflowNode(workflowNode),
        equalTo(
            WorkflowNode.builder()
                .reference(WorkflowNode.Reference.ofLaunchPlanRef(rewrittenIdentifier))
                .build()));
  }

  private LaunchPlan launchPlan(PartialWorkflowIdentifier workflowId) {
    Primitive defaultPrimitive = Primitive.ofStringValue("default-bar");
    return LaunchPlan.builder()
        .name("launch-plan-name")
        .workflowId(workflowId)
        .fixedInputs(
            Collections.singletonMap(
                "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("bar")))))
        .defaultInputs(
            Collections.singletonMap(
                "default-foo",
                Parameter.create(
                    Variable.builder()
                        .description("")
                        .literalType(LiteralType.ofSimpleType(SimpleType.STRING))
                        .build(),
                    Literal.ofScalar(Scalar.ofPrimitive(defaultPrimitive)))))
        .build();
  }
}
