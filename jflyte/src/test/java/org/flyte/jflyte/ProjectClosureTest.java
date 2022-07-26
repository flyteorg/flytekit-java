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
package org.flyte.jflyte;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.flyte.api.v1.Resources.ResourceName.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.ContainerTask;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.flytekit.SdkTypes;
import org.junit.jupiter.api.Test;

public class ProjectClosureTest {

  @Test
  public void testMerge() {
    Struct source =
        Struct.of(
            ImmutableMap.of(
                "a", Struct.Value.ofStringValue("a0"),
                "b", Struct.Value.ofStringValue("b0")));

    Struct target =
        Struct.of(
            ImmutableMap.of(
                "b", Struct.Value.ofStringValue("b1"),
                "c", Struct.Value.ofStringValue("c1")));

    Struct expected =
        Struct.of(
            ImmutableMap.of(
                "a", Struct.Value.ofStringValue("a0"),
                "b", Struct.Value.ofStringValue("b0"),
                "c", Struct.Value.ofStringValue("c1")));

    assertThat(expected.fields().size(), equalTo(3));

    assertThat(ProjectClosure.merge(source, target), equalTo(expected));
  }

  @Test
  public void testCollectSubWorkflows() {
    TypedInterface emptyInterface =
        TypedInterface.builder().inputs(ImmutableMap.of()).outputs(ImmutableMap.of()).build();

    WorkflowMetadata emptyMetadata = WorkflowMetadata.builder().build();

    PartialWorkflowIdentifier rewrittenSubWorkflowRef =
        PartialWorkflowIdentifier.builder()
            .project("project")
            .name("name")
            .version("version")
            .domain("domain")
            .build();

    WorkflowIdentifier subWorkflowRef =
        WorkflowIdentifier.builder()
            .project("project")
            .name("name")
            .version("version")
            .domain("domain")
            .build();

    WorkflowIdentifier otherSubWorkflowRef =
        WorkflowIdentifier.builder()
            .project("project")
            .name("other-name")
            .version("version")
            .domain("domain")
            .build();

    PartialWorkflowIdentifier rewrittenNestedSubWorkflowRef =
        PartialWorkflowIdentifier.builder()
            .project("project")
            .name("nested")
            .version("version")
            .domain("domain")
            .build();

    WorkflowIdentifier nestedSubWorkflowRef =
        WorkflowIdentifier.builder()
            .project("project")
            .name("nested")
            .version("version")
            .domain("domain")
            .build();

    PartialWorkflowIdentifier rewrittenNestedOtherSubWorkflowRef =
        PartialWorkflowIdentifier.builder()
            .project("project")
            .name("nested-other")
            .version("version")
            .domain("domain")
            .build();

    WorkflowIdentifier nestedOtherSubWorkflowRef =
        WorkflowIdentifier.builder()
            .project("project")
            .name("nested-other")
            .version("version")
            .domain("domain")
            .build();

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(rewrittenSubWorkflowRef))
            .build();

    WorkflowNode nestedWorkflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(rewrittenNestedSubWorkflowRef))
            .build();

    WorkflowNode nestedOtherWorkflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(rewrittenNestedOtherSubWorkflowRef))
            .build();

    WorkflowTemplate emptyWorkflowTemplate =
        WorkflowTemplate.builder()
            .interface_(emptyInterface)
            .metadata(emptyMetadata)
            .nodes(ImmutableList.of())
            .outputs(ImmutableList.of())
            .build();

    WorkflowTemplate nestedWorkflowTemplate =
        WorkflowTemplate.builder()
            .interface_(emptyInterface)
            .metadata(emptyMetadata)
            .nodes(
                ImmutableList.of(
                    Node.builder()
                        .id("nested-node")
                        .inputs(ImmutableList.of())
                        .upstreamNodeIds(ImmutableList.of())
                        .workflowNode(nestedOtherWorkflowNode)
                        .build()))
            .outputs(ImmutableList.of())
            .build();

    Operand opTrue = Operand.ofPrimitive(Primitive.ofBooleanValue(true));
    BooleanExpression exprTrue =
        BooleanExpression.ofComparison(
            ComparisonExpression.builder()
                .leftValue(opTrue)
                .rightValue(opTrue)
                .operator(ComparisonExpression.Operator.EQ)
                .build());

    List<Node> nodes =
        ImmutableList.of(
            Node.builder()
                .id("node-1")
                .inputs(ImmutableList.of())
                .upstreamNodeIds(ImmutableList.of())
                .workflowNode(workflowNode)
                .build(),
            // Same sub-workflow
            Node.builder()
                .id("node-2")
                .inputs(ImmutableList.of())
                .upstreamNodeIds(ImmutableList.of())
                .workflowNode(workflowNode)
                .build(),
            // Sub-workflow which has a nested sub-workflow in branch (nestedOtherWorkflowNode)
            Node.builder()
                .id("node-3")
                .inputs(ImmutableList.of())
                .upstreamNodeIds(ImmutableList.of())
                .branchNode(
                    BranchNode.builder()
                        .ifElse(
                            IfElseBlock.builder()
                                .case_(
                                    IfBlock.builder()
                                        .condition(exprTrue)
                                        .thenNode(
                                            Node.builder()
                                                .id("node-4")
                                                .inputs(ImmutableList.of())
                                                .upstreamNodeIds(ImmutableList.of())
                                                .workflowNode(nestedWorkflowNode)
                                                .build())
                                        .build())
                                .other(ImmutableList.of())
                                .build())
                        .build())
                .build());
    // nestedOtherWorkflowNode is not in the previous list because
    // that node belongs to the template of a sub-workflow

    Map<WorkflowIdentifier, WorkflowTemplate> allWorkflows =
        ImmutableMap.of(
            subWorkflowRef, emptyWorkflowTemplate,
            otherSubWorkflowRef, emptyWorkflowTemplate,
            nestedSubWorkflowRef, nestedWorkflowTemplate,
            nestedOtherSubWorkflowRef, emptyWorkflowTemplate);

    Map<WorkflowIdentifier, WorkflowTemplate> collectedSubWorkflows =
        ProjectClosure.collectSubWorkflows(nodes, allWorkflows);

    assertThat(
        collectedSubWorkflows,
        equalTo(
            ImmutableMap.of(
                subWorkflowRef,
                emptyWorkflowTemplate,
                nestedSubWorkflowRef,
                nestedWorkflowTemplate,
                nestedOtherSubWorkflowRef,
                emptyWorkflowTemplate)));
  }

  @Test
  public void testCheckCycles() {
    TypedInterface emptyInterface =
        TypedInterface.builder().inputs(ImmutableMap.of()).outputs(ImmutableMap.of()).build();

    WorkflowMetadata emptyMetadata = WorkflowMetadata.builder().build();

    PartialWorkflowIdentifier rewrittenWorkflowRef =
        PartialWorkflowIdentifier.builder()
            .project("project")
            .name("name")
            .version("version")
            .domain("domain")
            .build();

    PartialWorkflowIdentifier otherRewrittenWorkflowRef =
        PartialWorkflowIdentifier.builder()
            .project("project")
            .name("other-name")
            .version("version")
            .domain("domain")
            .build();

    WorkflowIdentifier workflowRef =
        WorkflowIdentifier.builder()
            .project("project")
            .name("name")
            .version("version")
            .domain("domain")
            .build();

    WorkflowIdentifier otherWorkflowRef =
        WorkflowIdentifier.builder()
            .project("project")
            .name("other-name")
            .version("version")
            .domain("domain")
            .build();

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(rewrittenWorkflowRef))
            .build();

    WorkflowNode otherWorkflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(otherRewrittenWorkflowRef))
            .build();

    WorkflowTemplate workflowTemplate =
        WorkflowTemplate.builder()
            .interface_(emptyInterface)
            .metadata(emptyMetadata)
            .nodes(
                ImmutableList.of(
                    Node.builder()
                        .id("sub-1")
                        .inputs(ImmutableList.of())
                        .upstreamNodeIds(ImmutableList.of())
                        .workflowNode(otherWorkflowNode)
                        .build()))
            .outputs(ImmutableList.of())
            .build();

    WorkflowTemplate otherWorkflowTemplate =
        WorkflowTemplate.builder()
            .interface_(emptyInterface)
            .metadata(emptyMetadata)
            .nodes(
                ImmutableList.of(
                    Node.builder()
                        .id("sub-2")
                        .inputs(ImmutableList.of())
                        .upstreamNodeIds(ImmutableList.of())
                        .workflowNode(workflowNode)
                        .build()))
            .outputs(ImmutableList.of())
            .build();

    Map<WorkflowIdentifier, WorkflowTemplate> allWorkflows =
        ImmutableMap.of(workflowRef, workflowTemplate, otherWorkflowRef, otherWorkflowTemplate);

    // workflow -> otherWorkflow -> workflow
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> ProjectClosure.checkCycles(allWorkflows));

    assertEquals(
        "Workflow [WorkflowIdentifier{name=name, domain=domain, project=project, version=version}] "
            + "cannot have itself as a node, directly or indirectly",
        exception.getMessage());
  }

  @Test
  public void testSerialize() {
    Map<String, ByteString> output = new HashMap<>();

    LaunchPlanIdentifier id0 =
        LaunchPlanIdentifier.builder()
            .domain("placeholder")
            .project("placeholder")
            .name("name0")
            .version("placeholder")
            .build();

    LaunchPlanIdentifier id1 =
        LaunchPlanIdentifier.builder()
            .domain("placeholder")
            .project("placeholder")
            .name("name1")
            .version("placeholder")
            .build();

    LaunchPlan launchPlan =
        LaunchPlan.builder()
            .name("name")
            .workflowId(
                PartialWorkflowIdentifier.builder()
                    .name("name")
                    .project("placeholder")
                    .domain("placeholder")
                    .version("placeholder")
                    .build())
            .build();

    ProjectClosure closure =
        ProjectClosure.builder()
            .workflowSpecs(emptyMap())
            .taskSpecs(emptyMap())
            .launchPlans(
                ImmutableMap.of(
                    id0, launchPlan,
                    id1, launchPlan))
            .build();

    closure.serialize(output::put);

    assertThat(output.keySet(), containsInAnyOrder("0_name0_3.pb", "1_name1_3.pb"));
  }

  @Test
  public void testCreateTaskTemplateForRunnableTask() {
    // given
    RunnableTask task = createRunnableTask(null);
    String image = "my-image";
    Resources expectedResources = Resources.builder().build();

    // when
    TaskTemplate result = ProjectClosure.createTaskTemplateForRunnableTask(task, image);

    // then
    Container container = result.container();
    assertNotNull(container);
    assertThat(container.image(), equalTo(image));
    assertThat(container.resources(), equalTo(expectedResources));
    assertThat(container.env(), equalTo(emptyList()));
    assertThat(
        result.interface_(),
        equalTo(
            TypedInterface.builder()
                .inputs(SdkTypes.nulls().getVariableMap())
                .outputs(SdkTypes.nulls().getVariableMap())
                .build()));
    assertThat(result.custom(), equalTo(Struct.of(emptyMap())));
    assertThat(result.retries(), equalTo(RetryStrategy.builder().retries(0).build()));
    assertThat(result.type(), equalTo("java-task"));
    assertThat(result.cache(), equalTo(true));
    assertThat(result.cacheSerializable(), equalTo(true));
    assertThat(result.cacheVersion(), equalTo("0.0.1"));
  }

  @Test
  public void testCreateTaskTemplateForRunnableTaskWithResources() {
    // given
    Resources expectedResources =
        Resources.builder()
            .limits(ImmutableMap.of(MEMORY, "16G"))
            .requests(ImmutableMap.of(CPU, "4"))
            .build();

    RunnableTask task = createRunnableTask(expectedResources);
    String image = "my-image";

    // when
    TaskTemplate result = ProjectClosure.createTaskTemplateForRunnableTask(task, image);

    // then
    Container container = result.container();
    assertNotNull(container);
    assertThat(container.image(), equalTo(image));
    assertThat(container.resources(), equalTo(expectedResources));
    assertThat(
        container.env(),
        equalTo(ImmutableList.of(KeyValuePair.of("JAVA_TOOL_OPTIONS", "-Xmx16G"))));
    assertThat(
        result.interface_(),
        equalTo(
            TypedInterface.builder()
                .inputs(SdkTypes.nulls().getVariableMap())
                .outputs(SdkTypes.nulls().getVariableMap())
                .build()));
    assertThat(result.custom(), equalTo(Struct.of(emptyMap())));
    assertThat(result.retries(), equalTo(RetryStrategy.builder().retries(0).build()));
    assertThat(result.type(), equalTo("java-task"));
    assertThat(result.cache(), equalTo(true));
    assertThat(result.cacheSerializable(), equalTo(true));
    assertThat(result.cacheVersion(), equalTo("0.0.1"));
  }

  @Test
  public void testCreateTaskTemplateForContainerTask() {
    // given
    Resources expectedResources =
        Resources.builder()
            .limits(ImmutableMap.of(MEMORY, "16G"))
            .requests(ImmutableMap.of(CPU, "4"))
            .build();
    String image = "test-image";
    List<String> args = ImmutableList.of("test", "--verbose");
    List<String> command = ImmutableList.of("bin", "program");
    List<KeyValuePair> env = ImmutableList.of(KeyValuePair.of("KEY", "VALUE"));
    ContainerTask task = createContainerTask(expectedResources, image, args, command, env);

    // when
    TaskTemplate result = ProjectClosure.createTaskTemplateForContainerTask(task);

    // then
    Container container = result.container();
    assertNotNull(container);
    assertThat(container.image(), equalTo(image));
    assertThat(container.resources(), equalTo(expectedResources));
    assertThat(container.env(), equalTo(env));
    assertThat(container.args(), equalTo(args));
    assertThat(container.command(), equalTo(command));
    assertThat(
        result.interface_(),
        equalTo(
            TypedInterface.builder()
                .inputs(SdkTypes.nulls().getVariableMap())
                .outputs(SdkTypes.nulls().getVariableMap())
                .build()));
    assertThat(result.custom(), equalTo(Struct.of(emptyMap())));
    assertThat(result.retries(), equalTo(RetryStrategy.builder().retries(0).build()));
    assertThat(result.type(), equalTo("raw-container"));
  }

  private RunnableTask createRunnableTask(Resources expectedResources) {
    return new RunnableTask() {
      @Override
      public String getName() {
        return "my-test-task";
      }

      @Override
      public TypedInterface getInterface() {
        return TypedInterface.builder()
            .inputs(SdkTypes.nulls().getVariableMap())
            .outputs(SdkTypes.nulls().getVariableMap())
            .build();
      }

      @Override
      public Map<String, Literal> run(Map<String, Literal> inputs) {
        System.out.println("Hello World");
        return null;
      }

      @Override
      public RetryStrategy getRetries() {
        return RetryStrategy.builder().retries(0).build();
      }

      @Override
      public Resources getResources() {
        if (expectedResources == null) {
          return RunnableTask.super.getResources();
        } else {
          return expectedResources;
        }
      }

      @Override
      public boolean isCached() {
        return true;
      }

      @Override
      public String getCacheVersion() {
        return "0.0.1";
      }

      @Override
      public boolean isCacheSerializable() {
        return true;
      }
    };
  }

  private ContainerTask createContainerTask(
      Resources resources,
      String image,
      List<String> args,
      List<String> command,
      List<KeyValuePair> env) {
    return new ContainerTask() {
      @Override
      public String getName() {
        return "test-container-task";
      }

      @Override
      public TypedInterface getInterface() {
        return TypedInterface.builder()
            .inputs(SdkTypes.nulls().getVariableMap())
            .outputs(SdkTypes.nulls().getVariableMap())
            .build();
      }

      @Override
      public RetryStrategy getRetries() {
        return RetryStrategy.builder().retries(0).build();
      }

      @Override
      public Resources getResources() {
        return resources;
      }

      @Override
      public String getImage() {
        return image;
      }

      @Override
      public List<String> getArgs() {
        return args;
      }

      @Override
      public List<String> getCommand() {
        return command;
      }

      @Override
      public List<KeyValuePair> getEnv() {
        return env;
      }
    };
  }
}
