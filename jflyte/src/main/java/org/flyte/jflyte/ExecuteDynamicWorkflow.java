/*
 * Copyright 2021-2023 Flyte Authors.
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
import static org.flyte.jflyte.utils.ClassLoaders.withClassLoader;
import static org.flyte.jflyte.utils.MoreCollectors.mapValues;
import static org.flyte.jflyte.utils.MoreCollectors.toUnmodifiableList;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.ContainerTask;
import org.flyte.api.v1.ContainerTaskRegistrar;
import org.flyte.api.v1.DynamicJobSpec;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.DynamicWorkflowTaskRegistrar;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.utils.ClassLoaders;
import org.flyte.jflyte.utils.Config;
import org.flyte.jflyte.utils.ExecutionConfig;
import org.flyte.jflyte.utils.FileSystemLoader;
import org.flyte.jflyte.utils.FlyteAdminClient;
import org.flyte.jflyte.utils.IdentifierRewrite;
import org.flyte.jflyte.utils.JFlyteCustom;
import org.flyte.jflyte.utils.PackageLoader;
import org.flyte.jflyte.utils.ProjectClosure;
import org.flyte.jflyte.utils.ProtoReader;
import org.flyte.jflyte.utils.ProtoUtil;
import org.flyte.jflyte.utils.ProtoWriter;
import org.flyte.jflyte.utils.Registrars;
import org.flyte.jflyte.utils.WorkflowNodeVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** Handler for "execute-dynamic-workflow" command. */
@CommandLine.Command(name = "execute-dynamic-workflow")
public class ExecuteDynamicWorkflow implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(ExecuteDynamicWorkflow.class);

  // A container task usually has limited CPU resource allocated, so using CPU core to derive
  // parallelism does not make much sense
  private static final int LOAD_PARALLELISM = 32;

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

      ExecutorService executorService = new ForkJoinPool(LOAD_PARALLELISM);
      ClassLoader packageClassLoader;
      try {
        packageClassLoader = PackageLoader.load(fileSystems, taskTemplate, executorService);
      } finally {
        executorService.shutdownNow();
      }

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

      Map<TaskIdentifier, ContainerTask> containerTasks =
          ClassLoaders.withClassLoader(
              packageClassLoader, () -> Registrars.loadAll(ContainerTaskRegistrar.class, env));

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
                  executionConfig, runnableTasks, dynamicWorkflowTasks, containerTasks),
              template ->
                  template.toBuilder()
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
          rewrite(config, executionConfig, futures, taskTemplates, workflowTemplates);

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

  private static DynamicJobSpec rewrite(
      Config config,
      ExecutionConfig executionConfig,
      DynamicJobSpec spec,
      Map<TaskIdentifier, TaskTemplate> allTaskTemplates,
      Map<WorkflowIdentifier, WorkflowTemplate> allWorkflowTemplates) {

    try (FlyteAdminClient flyteAdminClient =
        FlyteAdminClient.create(config.platformUrl(), config.platformInsecure(), null)) {

      WorkflowNodeVisitor workflowNodeVisitor =
          IdentifierRewrite.builder()
              .domain(executionConfig.domain())
              .project(executionConfig.project())
              .version(executionConfig.version())
              .adminClient(flyteAdminClient)
              .build()
              .visitor();
      Function<List<Node>, List<Node>> nodesRewriter =
          nodes -> nodes.stream().map(workflowNodeVisitor::visitNode).collect(toUnmodifiableList());

      Map<WorkflowIdentifier, WorkflowTemplate> allUsedSubWorkflows =
          collectAllUsedSubWorkflows(
              spec.nodes(), allWorkflowTemplates, workflowNodeVisitor, nodesRewriter);

      Map<TaskIdentifier, TaskTemplate> allUsedTaskTemplates = new HashMap<>();
      List<Node> rewrittenNodes =
          collectAllUsedTaskTemplates(
              spec,
              allTaskTemplates,
              nodesRewriter,
              allUsedTaskTemplates,
              flyteAdminClient,
              allUsedSubWorkflows);

      return spec.toBuilder()
          .nodes(rewrittenNodes)
          .subWorkflows(
              ImmutableMap.<WorkflowIdentifier, WorkflowTemplate>builder()
                  .putAll(spec.subWorkflows())
                  .putAll(allUsedSubWorkflows)
                  .build())
          .tasks(
              ImmutableMap.<TaskIdentifier, TaskTemplate>builder()
                  .putAll(spec.tasks())
                  .putAll(allUsedTaskTemplates)
                  .build())
          .build();
    }
  }

  private static List<Node> collectAllUsedTaskTemplates(
      DynamicJobSpec spec,
      Map<TaskIdentifier, TaskTemplate> allTaskTemplates,
      Function<List<Node>, List<Node>> nodesRewriter,
      Map<TaskIdentifier, TaskTemplate> allUsedTaskTemplates,
      FlyteAdminClient flyteAdminClient,
      Map<WorkflowIdentifier, WorkflowTemplate> allUsedSubWorkflows) {

    Map<TaskIdentifier, TaskTemplate> cache = new HashMap<>();

    // collect directly used task templates
    List<Node> rewrittenNodes =
        collectTaskTemplates(
            spec.nodes(),
            nodesRewriter,
            allUsedTaskTemplates,
            allTaskTemplates,
            flyteAdminClient,
            cache);

    // collect task templates used by subworkflows
    allUsedSubWorkflows
        .values()
        .forEach(
            workflowTemplate ->
                collectTaskTemplates(
                    workflowTemplate.nodes(),
                    nodesRewriter,
                    allUsedTaskTemplates,
                    allTaskTemplates,
                    flyteAdminClient,
                    cache));

    return rewrittenNodes;
  }

  private static Map<WorkflowIdentifier, WorkflowTemplate> collectAllUsedSubWorkflows(
      List<Node> nodes,
      Map<WorkflowIdentifier, WorkflowTemplate> workflowTemplates,
      WorkflowNodeVisitor workflowNodeVisitor,
      Function<List<Node>, List<Node>> nodesRewriter) {

    Map<WorkflowIdentifier, WorkflowTemplate> allUsedSubWorkflows =
        ProjectClosure.collectSubWorkflows(nodes, workflowTemplates, nodesRewriter);
    return mapValues(allUsedSubWorkflows, workflowNodeVisitor::visitWorkflowTemplate);
  }

  private static List<Node> collectTaskTemplates(
      List<Node> nodes,
      Function<List<Node>, List<Node>> nodesRewriter,
      Map<TaskIdentifier, TaskTemplate> allUsedTaskTemplates,
      Map<TaskIdentifier, TaskTemplate> allTaskTemplates,
      FlyteAdminClient flyteAdminClient,
      Map<TaskIdentifier, TaskTemplate> cache) {

    List<Node> rewrittenNodes = nodesRewriter.apply(nodes);

    Map<TaskIdentifier, TaskTemplate> usedTaskTemplates =
        ProjectClosure.collectDynamicWorkflowTasks(
            rewrittenNodes, allTaskTemplates, id -> fetchTaskTemplate(flyteAdminClient, id, cache));
    allUsedTaskTemplates.putAll(usedTaskTemplates);

    return rewrittenNodes;
  }

  // note that there are cases we are making an unnecessary network call because we might have
  // already got the task template when resolving the latest task version, but since it is also
  // possible that user has provided a version for a remote task, and in that case we would not need
  // to resolve the latest version, so we need to make this call;
  // we accept the additional cost because it should be rare to have remote tasks in a dynamic
  // workflow
  private static TaskTemplate fetchTaskTemplate(
      FlyteAdminClient flyteAdminClient,
      TaskIdentifier id,
      Map<TaskIdentifier, TaskTemplate> cache) {
    return cache.computeIfAbsent(
        id,
        taskIdentifier -> {
          LOG.info("fetching task template remotely for {}", id);

          return flyteAdminClient.fetchLatestTaskTemplate(
              NamedEntityIdentifier.builder()
                  .domain(id.domain())
                  .project(id.project())
                  .name(id.name())
                  .build());
        });
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
