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
import static org.flyte.jflyte.MoreCollectors.mapValues;
import static org.flyte.jflyte.MoreCollectors.toUnmodifiableList;
import static org.flyte.jflyte.MoreCollectors.toUnmodifiableMap;
import static org.flyte.jflyte.QuantityUtil.asJavaQuantity;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.DynamicWorkflowTaskRegistrar;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.LaunchPlanRegistrar;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.Resources.ResourceName;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
abstract class ProjectClosure {

  private static final Logger LOG = LoggerFactory.getLogger(ProjectClosure.class);

  abstract Map<TaskIdentifier, TaskSpec> taskSpecs();

  abstract Map<WorkflowIdentifier, WorkflowSpec> workflowSpecs();

  abstract Map<LaunchPlanIdentifier, LaunchPlan> launchPlans();

  ProjectClosure applyCustom(JFlyteCustom custom) {
    Map<TaskIdentifier, TaskSpec> rewrittenTaskSpecs =
        mapValues(taskSpecs(), x -> applyCustom(x, custom));

    return ProjectClosure.builder()
        .workflowSpecs(workflowSpecs())
        .launchPlans(launchPlans())
        .taskSpecs(rewrittenTaskSpecs)
        .build();
  }

  void serialize(BiConsumer<String, ByteString> output) {
    int size = taskSpecs().size() + launchPlans().size() + workflowSpecs().size();
    int sizeDigits = (int) (Math.log10(size) + 1);
    AtomicInteger counter = new AtomicInteger();

    taskSpecs()
        .forEach(
            (id, spec) -> {
              int i = counter.getAndIncrement();
              String filename = String.format("%0" + sizeDigits + "d_%s_1.pb", i, id.name());

              output.accept(filename, ProtoUtil.serialize(spec).toByteString());
            });

    workflowSpecs()
        .forEach(
            (id, spec) -> {
              int i = counter.getAndIncrement();
              String filename = String.format("%0" + sizeDigits + "d_%s_2.pb", i, id.name());

              output.accept(filename, ProtoUtil.serialize(id, spec).toByteString());
            });

    launchPlans()
        .forEach(
            (id, spec) -> {
              int i = counter.getAndIncrement();
              String filename = String.format("%0" + sizeDigits + "d_%s_3.pb", i, id.name());

              output.accept(filename, ProtoUtil.serialize(spec).toByteString());
            });
  }

  private static TaskSpec applyCustom(TaskSpec taskSpec, JFlyteCustom custom) {
    Struct rewrittenCustom = merge(custom.serializeToStruct(), taskSpec.taskTemplate().custom());
    TaskTemplate rewrittenTaskTemplate =
        taskSpec.taskTemplate().toBuilder().custom(rewrittenCustom).build();

    return TaskSpec.create(rewrittenTaskTemplate);
  }

  static ProjectClosure loadAndStage(
      String packageDir,
      ExecutionConfig config,
      Supplier<ArtifactStager> stagerSupplier,
      FlyteAdminClient adminClient) {
    IdentifierRewrite rewrite =
        IdentifierRewrite.builder()
            .adminClient(adminClient)
            .domain(config.domain())
            .project(config.project())
            .version(config.version())
            .build();

    // before we run anything, switch class loader, because we will be touching user classes;
    // setting it in thread context will give us access to the right class loader
    ClassLoader packageClassLoader = ClassLoaders.forDirectory(new File(packageDir));

    ProjectClosure closure = ProjectClosure.load(config, rewrite, packageClassLoader);

    List<Artifact> artifacts;
    if (!closure.taskSpecs().isEmpty()) {
      artifacts = stagePackageFiles(stagerSupplier.get(), packageDir);
    } else {
      artifacts = emptyList();
      LOG.info(
          "Skipping artifact staging because there are no runnable tasks or dynamic workflow tasks");
    }

    JFlyteCustom custom = JFlyteCustom.builder().artifacts(artifacts).build();

    return closure.applyCustom(custom);
  }

  private static List<Artifact> stagePackageFiles(ArtifactStager stager, String packageDir) {
    try (Stream<Path> fileStream = Files.list(Paths.get(packageDir))) {
      List<String> files =
          fileStream.map(x -> x.toFile().getAbsolutePath()).collect(toUnmodifiableList());

      return stager.stageFiles(files);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static ProjectClosure load(
      ExecutionConfig config, IdentifierRewrite rewrite, ClassLoader packageClassLoader) {
    Map<String, String> env =
        ImmutableMap.<String, String>builder()
            // we keep JFLYTE_ only for backwards-compatibility
            .put("JFLYTE_DOMAIN", config.domain())
            .put("JFLYTE_PROJECT", config.project())
            .put("JFLYTE_VERSION", config.version())
            .put("FLYTE_INTERNAL_DOMAIN", config.domain())
            .put("FLYTE_INTERNAL_PROJECT", config.project())
            .put("FLYTE_INTERNAL_VERSION", config.version())
            .build();

    // 1. load classes, and create templates
    Map<TaskIdentifier, RunnableTask> runnableTasks =
        ClassLoaders.withClassLoader(
            packageClassLoader, () -> Registrars.loadAll(RunnableTaskRegistrar.class, env));

    Map<TaskIdentifier, DynamicWorkflowTask> dynamicWorkflowTasks =
        ClassLoaders.withClassLoader(
            packageClassLoader, () -> Registrars.loadAll(DynamicWorkflowTaskRegistrar.class, env));

    Map<WorkflowIdentifier, WorkflowTemplate> workflows =
        ClassLoaders.withClassLoader(
            packageClassLoader, () -> Registrars.loadAll(WorkflowTemplateRegistrar.class, env));

    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans =
        ClassLoaders.withClassLoader(
            packageClassLoader, () -> Registrars.loadAll(LaunchPlanRegistrar.class, env));

    return load(config, rewrite, runnableTasks, dynamicWorkflowTasks, workflows, launchPlans);
  }

  static ProjectClosure load(
      ExecutionConfig config,
      IdentifierRewrite rewrite,
      Map<TaskIdentifier, RunnableTask> runnableTasks,
      Map<TaskIdentifier, DynamicWorkflowTask> dynamicWorkflowTasks,
      Map<WorkflowIdentifier, WorkflowTemplate> workflowTemplates,
      Map<LaunchPlanIdentifier, LaunchPlan> launchPlans) {
    Map<TaskIdentifier, TaskTemplate> taskTemplates =
        createTaskTemplates(config, runnableTasks, dynamicWorkflowTasks);

    // 2. rewrite workflows and launch plans
    Map<WorkflowIdentifier, WorkflowTemplate> rewrittenWorkflowTemplates =
        mapValues(workflowTemplates, rewrite::apply);
    Map<LaunchPlanIdentifier, LaunchPlan> rewrittenLaunchPlans =
        mapValues(launchPlans, rewrite::apply);

    checkCycles(rewrittenWorkflowTemplates);

    // 3. create specs for registration
    Map<WorkflowIdentifier, WorkflowSpec> workflowSpecs =
        mapValues(
            rewrittenWorkflowTemplates,
            workflowTemplate -> {
              Map<WorkflowIdentifier, WorkflowTemplate> subWorkflows =
                  collectSubWorkflows(workflowTemplate.nodes(), rewrittenWorkflowTemplates);

              return WorkflowSpec.builder()
                  .workflowTemplate(workflowTemplate)
                  .subWorkflows(subWorkflows)
                  .build();
            });

    Map<TaskIdentifier, TaskSpec> taskSpecs = mapValues(taskTemplates, TaskSpec::create);

    return ProjectClosure.builder()
        .taskSpecs(taskSpecs)
        .workflowSpecs(workflowSpecs)
        .launchPlans(rewrittenLaunchPlans)
        .build();
  }

  @VisibleForTesting
  static void checkCycles(Map<WorkflowIdentifier, WorkflowTemplate> allWorkflows) {
    Optional<WorkflowIdentifier> cycle =
        allWorkflows.keySet().stream()
            .filter(
                workflowId ->
                    checkCycles(
                        workflowId,
                        allWorkflows,
                        /*beingVisited=*/ new HashSet<>(),
                        /*visited=*/ new HashSet<>()))
            .findFirst();
    if (cycle.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Workflow [%s] cannot have itself as a node, directly or indirectly", cycle.get()));
    }
  }

  static boolean checkCycles(
      WorkflowIdentifier workflowId,
      Map<WorkflowIdentifier, WorkflowTemplate> allWorkflows,
      Set<WorkflowIdentifier> beingVisited,
      Set<WorkflowIdentifier> visited) {

    beingVisited.add(workflowId);
    WorkflowTemplate workflow = allWorkflows.get(workflowId);

    List<Node> nodes =
        workflow.nodes().stream().flatMap(ProjectClosure::flatBranch).collect(toUnmodifiableList());

    for (Node node : nodes) {
      if (isSubWorkflowNode(node)) {
        PartialWorkflowIdentifier partialSubWorkflowId =
            Objects.requireNonNull(node.workflowNode()).reference().subWorkflowRef();
        WorkflowIdentifier subWorkflowId =
            WorkflowIdentifier.builder()
                .project(partialSubWorkflowId.project())
                .name(partialSubWorkflowId.name())
                .domain(partialSubWorkflowId.domain())
                .version(partialSubWorkflowId.version())
                .build();
        if (beingVisited.contains(subWorkflowId) // backward edge
            || (!visited.contains(subWorkflowId)
                && checkCycles(subWorkflowId, allWorkflows, beingVisited, visited))) {
          return true;
        }
      }
    }

    beingVisited.remove(workflowId);
    visited.add(workflowId);
    return false;
  }

  @VisibleForTesting
  static Map<WorkflowIdentifier, WorkflowTemplate> collectSubWorkflows(
      List<Node> rewrittenNodes, Map<WorkflowIdentifier, WorkflowTemplate> allWorkflows) {
    return collectSubWorkflowIds(rewrittenNodes).stream()
        // all identifiers should be rewritten at this point
        .map(
            workflowId ->
                WorkflowIdentifier.builder()
                    .project(workflowId.project())
                    .name(workflowId.name())
                    .domain(workflowId.domain())
                    .version(workflowId.version())
                    .build())
        .distinct()
        .flatMap(
            workflowId -> {
              WorkflowTemplate subWorkflow = allWorkflows.get(workflowId);

              if (subWorkflow == null) {
                throw new NoSuchElementException(
                    "Can't find referenced sub-workflow " + workflowId);
              }

              Map<WorkflowIdentifier, WorkflowTemplate> nestedSubWorkflows =
                  collectSubWorkflows(subWorkflow.nodes(), allWorkflows);

              return Stream.concat(
                  Stream.of(Maps.immutableEntry(workflowId, subWorkflow)),
                  nestedSubWorkflows.entrySet().stream());
            })
        .collect(toUnmodifiableMap());
  }

  static Map<TaskIdentifier, TaskTemplate> collectTasks(
      List<Node> rewrittenNodes, Map<TaskIdentifier, TaskTemplate> allTasks) {
    return collectTaskIds(rewrittenNodes).stream()
        // all identifiers should be rewritten at this point
        .map(
            taskId ->
                TaskIdentifier.builder()
                    .project(taskId.project())
                    .name(taskId.name())
                    .domain(taskId.domain())
                    .version(taskId.version())
                    .build())
        .distinct()
        .map(
            taskId -> {
              TaskTemplate taskTemplate = allTasks.get(taskId);

              if (taskTemplate == null) {
                throw new NoSuchElementException("Can't find referenced task " + taskId);
              }

              return Maps.immutableEntry(taskId, taskTemplate);
            })
        .collect(toUnmodifiableMap());
  }

  private static List<PartialTaskIdentifier> collectTaskIds(List<Node> rewrittenNodes) {
    return rewrittenNodes.stream()
        .filter(x -> x.taskNode() != null)
        .map(x -> x.taskNode().referenceId())
        .collect(toUnmodifiableList());
  }

  static Map<TaskIdentifier, TaskTemplate> createTaskTemplates(
      ExecutionConfig config,
      Map<TaskIdentifier, RunnableTask> runnableTasks,
      Map<TaskIdentifier, DynamicWorkflowTask> dynamicWorkflowTasks) {
    Map<TaskIdentifier, TaskTemplate> taskTemplates = new HashMap<>();

    runnableTasks.forEach(
        (id, task) -> {
          TaskTemplate taskTemplate = createTaskTemplateForRunnableTask(task, config.image());

          taskTemplates.put(id, taskTemplate);
        });

    dynamicWorkflowTasks.forEach(
        (id, task) -> {
          TaskTemplate taskTemplate = createTaskTemplateForDynamicWorkflow(task, config.image());

          taskTemplates.put(id, taskTemplate);
        });

    return taskTemplates;
  }

  @VisibleForTesting
  static TaskTemplate createTaskTemplateForRunnableTask(RunnableTask task, String image) {
    Resources resources = task.getResources();
    Container container =
        Container.builder()
            .command(ImmutableList.of())
            .args(
                ImmutableList.of(
                    "jflyte",
                    "execute",
                    "--task",
                    task.getName(),
                    "--inputs",
                    "{{.input}}",
                    "--outputPrefix",
                    "{{.outputPrefix}}",
                    "--taskTemplatePath",
                    "{{.taskTemplatePath}}"))
            .image(image)
            .env(javaToolOptionsEnv(resources).map(ImmutableList::of).orElse(ImmutableList.of()))
            .resources(resources)
            .build();

    return TaskTemplate.builder()
        .container(container)
        .interface_(task.getInterface())
        .retries(task.getRetries())
        .type(task.getType())
        .custom(task.getCustom())
        .build();
  }

  private static Optional<KeyValuePair> javaToolOptionsEnv(Resources resources) {
    Map<ResourceName, String> limits = resources.limits();
    if (limits == null || !limits.containsKey(ResourceName.MEMORY)) {
      return Optional.empty();
    }
    String maxMemory = asJavaQuantity(limits.get(ResourceName.MEMORY));
    return Optional.of(KeyValuePair.of("JAVA_TOOL_OPTIONS", "-Xmx" + maxMemory));
  }

  private static TaskTemplate createTaskTemplateForDynamicWorkflow(
      DynamicWorkflowTask task, String image) {
    Container container =
        Container.builder()
            .command(ImmutableList.of())
            .args(
                ImmutableList.of(
                    "jflyte",
                    "execute-dynamic-workflow",
                    "--task",
                    task.getName(),
                    "--inputs",
                    "{{.input}}",
                    "--outputPrefix",
                    "{{.outputPrefix}}",
                    "--taskTemplatePath",
                    "{{.taskTemplatePath}}"))
            .image(image)
            .env(emptyList())
            .build();

    return TaskTemplate.builder()
        .container(container)
        .interface_(task.getInterface())
        .retries(task.getRetries())
        .type("container")
        .custom(Struct.of(emptyMap()))
        .build();
  }

  @VisibleForTesting
  static Struct merge(Struct source, Struct target) {
    Map<String, Struct.Value> fields = new HashMap<>(target.fields());
    fields.putAll(source.fields());

    return Struct.of(Collections.unmodifiableMap(fields));
  }

  private static List<PartialWorkflowIdentifier> collectSubWorkflowIds(List<Node> rewrittenNodes) {
    return rewrittenNodes.stream()
        .flatMap(ProjectClosure::flatBranch)
        .filter(ProjectClosure::isSubWorkflowNode)
        .map(x -> Objects.requireNonNull(x.workflowNode()).reference().subWorkflowRef())
        .collect(toUnmodifiableList());
  }

  private static Stream<Node> flatBranch(Node node) {
    if (node.branchNode() == null) {
      return Stream.of(node);
    }
    IfElseBlock ifElseBlock = node.branchNode().ifElse();
    return Stream.concat(
            ifElseBlock.other().stream().map(IfBlock::thenNode),
            Stream.of(ifElseBlock.case_().thenNode(), ifElseBlock.elseNode()))
        .filter(Objects::nonNull)
        // Nested branch
        .flatMap(ProjectClosure::flatBranch);
  }

  private static boolean isSubWorkflowNode(Node node) {
    return node.workflowNode() != null
        && node.workflowNode().reference().kind() == WorkflowNode.Reference.Kind.SUB_WORKFLOW_REF;
  }

  static Builder builder() {
    return new AutoValue_ProjectClosure.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder taskSpecs(Map<TaskIdentifier, TaskSpec> taskSpecs);

    abstract Builder launchPlans(Map<LaunchPlanIdentifier, LaunchPlan> launchPlans);

    abstract Builder workflowSpecs(Map<WorkflowIdentifier, WorkflowSpec> workflowSpecs);

    abstract ProjectClosure build();
  }
}
