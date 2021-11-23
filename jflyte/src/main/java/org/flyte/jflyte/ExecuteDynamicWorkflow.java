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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.flyte.jflyte.ClassLoaders.withClassLoader;
import static org.flyte.jflyte.MoreCollectors.mapValues;
import static org.flyte.jflyte.MoreCollectors.toUnmodifiableList;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.DynamicJobSpec;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.DynamicWorkflowTaskRegistrar;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;
import org.flyte.jflyte.api.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** Handler for "execute-dynamic-workflow" command. */
@CommandLine.Command(name = "execute-dynamic-workflow")
public class ExecuteDynamicWorkflow implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(ExecuteDynamicWorkflow.class);

  @Option(
      names = {"--task"},
      required = true)
  private String task;

  @Option(
      names = {"--inputs"},
      required = true)
  private String inputs;

  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"--outputPrefix"},
      required = true)
  private String outputPrefix;

  @Option(
      names = {"--taskTemplatePath"},
      required = true)
  private String taskTemplatePath;

  @Override
  public Integer call() {
    execute();
    return 0;
  }

  private void execute() {
    Config config = Config.load();
    ExecutionConfig executionConfig = ExecutionConfig.load();

    Collection<ClassLoader> modules = ClassLoaders.forModuleDir(config.moduleDir()).values();
    Map<String, FileSystem> fileSystems = FileSystemLoader.loadFileSystems(modules);

    FileSystem outputFs = FileSystemLoader.getFileSystem(fileSystems, outputPrefix);
    ProtoWriter protoWriter = new ProtoWriter(outputPrefix, outputFs);

    try {
      FileSystem inputFs = FileSystemLoader.getFileSystem(fileSystems, inputs);
      ProtoReader protoReader = new ProtoReader(inputFs);

      TaskTemplate taskTemplate = protoReader.getTaskTemplate(taskTemplatePath);
      ClassLoader packageClassLoader = PackageLoader.load(fileSystems, taskTemplate);

      Map<String, String> env = getEnv();
      Map<WorkflowIdentifier, WorkflowTemplate> workflowTemplates =
          ClassLoaders.withClassLoader(
              packageClassLoader, () -> Registrars.loadAll(WorkflowTemplateRegistrar.class, env));

      Map<TaskIdentifier, RunnableTask> runnableTasks =
          ClassLoaders.withClassLoader(
              packageClassLoader, () -> Registrars.loadAll(RunnableTaskRegistrar.class, env));

      Map<TaskIdentifier, DynamicWorkflowTask> dynamicWorkflowTasks =
          ClassLoaders.withClassLoader(
              packageClassLoader,
              () -> Registrars.loadAll(DynamicWorkflowTaskRegistrar.class, env));

      // before we run anything, switch class loader, otherwise,
      // ServiceLoaders and other things wouldn't work, for instance,
      // FileSystemRegister in Apache Beam

      // we don't take the whole "custom" field, but only jflyte part, for that we ser-de it
      Struct custom = JFlyteCustom.deserializeFromStruct(taskTemplate.custom()).serializeToStruct();

      // all tasks already have staged jars, we can reuse 'jflyte' custom from current task to get
      // it
      Map<TaskIdentifier, TaskTemplate> taskTemplates =
          mapValues(
              ProjectClosure.createTaskTemplates(
                  executionConfig, runnableTasks, dynamicWorkflowTasks),
              template ->
                  template
                      .toBuilder()
                      .custom(ProjectClosure.merge(template.custom(), custom))
                      .build());

      DynamicJobSpec futures =
          withClassLoader(
              packageClassLoader,
              () -> {
                Map<String, Literal> input = protoReader.getInput(inputs);
                DynamicWorkflowTask task = getDynamicWorkflowTask(this.task);

                return task.run(input);
              });

      DynamicJobSpec rewrittenFutures =
          rewrite(executionConfig, futures, taskTemplates, workflowTemplates);

      if (rewrittenFutures.nodes().isEmpty()) {
        Map<String, Literal> outputs = getLiteralMap(rewrittenFutures.outputs());

        protoWriter.writeOutputs(outputs);
      } else {
        protoWriter.writeFutures(rewrittenFutures);
      }
    } catch (ContainerError e) {
      LOG.error("failed to run dynamic workflow", e);

      protoWriter.writeError(ProtoUtil.serializeContainerError(e));
    } catch (Throwable e) {
      LOG.error("failed to run dynamic workflow", e);

      protoWriter.writeError(ProtoUtil.serializeThrowable(e));
    }
  }

  static DynamicJobSpec rewrite(
      ExecutionConfig config,
      DynamicJobSpec spec,
      Map<TaskIdentifier, TaskTemplate> taskTemplates,
      Map<WorkflowIdentifier, WorkflowTemplate> workflowTemplates) {

    DynamicWorkflowIdentifierRewrite rewrite = new DynamicWorkflowIdentifierRewrite(config);

    List<Node> rewrittenNodes =
        spec.nodes().stream().map(rewrite::visitNode).collect(toUnmodifiableList());

    Map<WorkflowIdentifier, WorkflowTemplate> usedSubWorkflows =
        ProjectClosure.collectSubWorkflows(rewrittenNodes, workflowTemplates);

    Map<TaskIdentifier, TaskTemplate> usedTaskTemplates =
        ProjectClosure.collectTasks(rewrittenNodes, taskTemplates);

    // FIXME one sub-workflow can use more sub-workflows, we should recursively collect used tasks
    // and workflows

    Map<WorkflowIdentifier, WorkflowTemplate> rewrittenUsedSubWorkflows =
        mapValues(usedSubWorkflows, rewrite::visitWorkflowTemplate);

    return spec.toBuilder()
        .nodes(rewrittenNodes)
        .subWorkflows(
            ImmutableMap.<WorkflowIdentifier, WorkflowTemplate>builder()
                .putAll(spec.subWorkflows())
                .putAll(rewrittenUsedSubWorkflows)
                .build())
        .tasks(
            ImmutableMap.<TaskIdentifier, TaskTemplate>builder()
                .putAll(spec.tasks())
                .putAll(usedTaskTemplates)
                .build())
        .build();
  }

  static class DynamicWorkflowIdentifierRewrite extends WorkflowNodeVisitor {
    private final ExecutionConfig config;

    DynamicWorkflowIdentifierRewrite(ExecutionConfig config) {
      this.config = config;
    }

    @Override
    PartialTaskIdentifier visitTaskIdentifier(PartialTaskIdentifier value) {
      if (value.project() == null && value.domain() == null && value.version() == null) {
        return PartialTaskIdentifier.builder()
            .name(value.name())
            .project(config.project())
            .domain(config.domain())
            .version(config.version())
            .build();
      }

      throw new IllegalArgumentException(
          "Dynamic workflow tasks don't support remote tasks: " + value);
    }

    @Override
    PartialWorkflowIdentifier visitWorkflowIdentifier(PartialWorkflowIdentifier value) {
      if (value.project() == null && value.domain() == null && value.version() == null) {
        return PartialWorkflowIdentifier.builder()
            .name(value.name())
            .project(config.project())
            .domain(config.domain())
            .version(config.version())
            .build();
      }

      // in these cases all referenced workflows are sub-workflows, and we can't include
      // templates for tasks used in them

      throw new IllegalArgumentException(
          "Dynamic workflow tasks don't support remote workflows: " + value);
    }

    @Override
    PartialLaunchPlanIdentifier visitLaunchPlanIdentifier(PartialLaunchPlanIdentifier value) {
      if (value.project() == null && value.domain() == null && value.version() == null) {
        return PartialLaunchPlanIdentifier.builder()
            .name(value.name())
            .project(config.project())
            .domain(config.domain())
            .version(config.version())
            .build();
      }

      // we don't need to fetch anything, so we can use this reference, because
      // for launch plans we don't need to include task and workflow templates into closure
      if (value.project() != null && value.domain() != null && value.version() != null) {
        return value;
      }

      throw new IllegalArgumentException(
          "Dynamic workflow tasks don't support remote launch plans: " + value);
    }
  }

  private static DynamicWorkflowTask getDynamicWorkflowTask(String name) {
    // be careful not to pass extra
    Map<String, String> env = getEnv();
    Map<TaskIdentifier, DynamicWorkflowTask> dynamicWorkflows =
        Registrars.loadAll(DynamicWorkflowTaskRegistrar.class, env);

    for (Map.Entry<TaskIdentifier, DynamicWorkflowTask> entry : dynamicWorkflows.entrySet()) {
      if (entry.getKey().name().equals(name)) {
        return entry.getValue();
      }
    }

    throw new IllegalArgumentException("Dynamic workflow task not found: " + name);
  }

  private static Map<String, String> getEnv() {
    return System.getenv().entrySet().stream()
        // we keep JFLYTE_ only for backwards-compatibility
        .filter(x -> x.getKey().startsWith("JFLYTE_") || x.getKey().startsWith("FLYTE_"))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static Map<String, Literal> getLiteralMap(List<Binding> bindings) {
    return bindings.stream()
        .collect(toMap(Binding::var_, binding -> getLiteral(binding.binding())));
  }

  static Literal getLiteral(BindingData bindingData) {
    switch (bindingData.kind()) {
      case SCALAR:
        return Literal.ofScalar(bindingData.scalar());

      case COLLECTION:
        return Literal.ofCollection(
            bindingData.collection().stream()
                .map(ExecuteDynamicWorkflow::getLiteral)
                .collect(toList()));

      case PROMISE:
        throw new IllegalArgumentException(
            "invariant failed, workflows without nodes can't have promises");

      case MAP:
        return Literal.ofMap(
            bindingData.map().entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), getLiteral(entry.getValue())))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    throw new AssertionError("Unexpected BindingData.Kind: " + bindingData.kind());
  }
}
