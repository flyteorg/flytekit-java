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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.flyte.jflyte.TokenSourceFactoryLoader.getTokenSource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.LaunchPlanRegistrar;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.TokenSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Registers all workflows on classpath. */
@Command(name = "workflows")
public class RegisterWorkflows implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterWorkflows.class);

  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"-p", "--project"},
      description = "Flyte project to use. You can have more than one project per repo",
      required = true)
  private String project;

  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"-d", "--domain"},
      description = "This is usually development, staging, or production",
      required = true)
  private String domain;

  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"-v", "--version"},
      description = "This is version to apply globally for this context",
      required = true)
  private String version;

  @Option(
      names = {"-cp", "--classpath"},
      description = "Directory with packaged jars",
      required = true)
  private String packageDir;

  @Nullable
  @Option(
      names = {"-am", "--auth-mode"},
      description = "Authentication method used to retrieve token",
      required = false)
  private String authMode;

  @Override
  public Integer call() {
    Config config = Config.load();
    Collection<ClassLoader> modules = ClassLoaders.forModuleDir(config.moduleDir()).values();

    TokenSource tokenSource = (authMode == null) ? null : getTokenSource(modules, authMode);

    try (FlyteAdminClient adminClient =
        FlyteAdminClient.create(config.platformUrl(), config.platformInsecure(), tokenSource)) {
      registerAll(config, adminClient, modules);
    }

    return 0;
  }

  private ArtifactStager getArtifactStager(Config config, Collection<ClassLoader> modules) {
    try {
      String stagingLocation = config.stagingLocation();

      if (stagingLocation == null) {
        throw new IllegalArgumentException(
            "Environment variable 'FLYTE_STAGING_LOCATION' isn't set");
      }

      URI stagingUri = new URI(stagingLocation);
      Map<String, FileSystem> fileSystems = FileSystemLoader.loadFileSystems(modules);
      FileSystem stagingFileSystem = FileSystemLoader.getFileSystem(fileSystems, stagingUri);

      return new ArtifactStager(stagingLocation, stagingFileSystem);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to parse stagingLocation", e);
    }
  }

  private static TaskTemplate createTaskTemplate(
      RunnableTask task,
      String indexFileLocation,
      Struct defaultCustom,
      String image,
      List<KeyValuePair> env) {
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
                    "--indexFileLocation",
                    indexFileLocation))
            .image(image)
            .env(env)
            .build();

    // task's custom takes precedence over defaultCustom, that is "jflyte" struct
    // so anything in task's custom can override it
    Struct custom = merge(task.getCustom(), defaultCustom);

    return TaskTemplate.builder()
        .container(container)
        .interface_(task.getInterface())
        .retries(task.getRetries())
        .type(task.getType())
        .custom(custom)
        .build();
  }

  private void registerAll(
      Config config, FlyteAdminClient adminClient, Collection<ClassLoader> modules) {
    ClassLoader packageClassLoader = ClassLoaders.forDirectory(new File(packageDir));

    Map<String, String> env =
        ImmutableMap.of(
            "JFLYTE_DOMAIN", domain,
            "JFLYTE_PROJECT", project,
            "JFLYTE_VERSION", version);

    // before we run anything, switch class loader, because we will be touching user classes;
    // setting it in thread context will give us access to the right class loader

    Map<TaskIdentifier, RunnableTask> tasks =
        ClassLoaders.withClassLoader(
            packageClassLoader, () -> Registrars.loadAll(RunnableTaskRegistrar.class, env));

    Map<WorkflowIdentifier, WorkflowTemplate> workflows =
        ClassLoaders.withClassLoader(
            packageClassLoader, () -> Registrars.loadAll(WorkflowTemplateRegistrar.class, env));

    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans =
        ClassLoaders.withClassLoader(
            packageClassLoader, () -> Registrars.loadAll(LaunchPlanRegistrar.class, env));

    IdentifierRewrite identifierRewrite =
        IdentifierRewrite.builder()
            .adminClient(adminClient)
            .domain(domain)
            .project(project)
            .version(version)
            .build();

    registerTasks(config, adminClient, tasks, env, modules);

    Map<WorkflowIdentifier, WorkflowTemplate> rewrittenWorkflows =
        workflows.entrySet().stream()
            .collect(toMap(Map.Entry::getKey, x -> identifierRewrite.apply(x.getValue())));

    for (Map.Entry<WorkflowIdentifier, WorkflowTemplate> entry : rewrittenWorkflows.entrySet()) {
      WorkflowIdentifier workflowId = entry.getKey();
      WorkflowTemplate rewrittenParent = entry.getValue();

      Map<WorkflowIdentifier, WorkflowTemplate> subWorkflows =
          collectSubWorkflows(rewrittenParent, rewrittenWorkflows);

      adminClient.createWorkflow(workflowId, rewrittenParent, subWorkflows);
    }

    for (Map.Entry<LaunchPlanIdentifier, LaunchPlan> entry : launchPlans.entrySet()) {
      LaunchPlanIdentifier launchPlanId = entry.getKey();
      LaunchPlan launchPlan = identifierRewrite.apply(entry.getValue());

      adminClient.createLaunchPlan(launchPlanId, launchPlan);
    }
  }

  @VisibleForTesting
  static Map<WorkflowIdentifier, WorkflowTemplate> collectSubWorkflows(
      WorkflowTemplate rewrittenParent, Map<WorkflowIdentifier, WorkflowTemplate> allWorkflows) {
    return collectSubWorkflowIds(rewrittenParent).stream()
        // all identifiers should be rewritten at this point
        .map(
            workflowId ->
                WorkflowIdentifier.builder()
                    .project(workflowId.project())
                    .name(workflowId.name())
                    .domain(workflowId.domain())
                    .version(workflowId.version())
                    .build())
        .map(
            workflowId -> {
              WorkflowTemplate subWorkflow = allWorkflows.get(workflowId);

              if (subWorkflow == null) {
                throw new NoSuchElementException("Can't find sub-workflow " + workflowId);
              }

              return Maps.immutableEntry(workflowId, subWorkflow);
            })
        .collect(
            collectingAndThen(
                toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::unmodifiableMap));
  }

  private static List<PartialWorkflowIdentifier> collectSubWorkflowIds(WorkflowTemplate parent) {
    return parent.nodes().stream()
        .filter(x -> x.workflowNode() != null)
        .filter(
            x ->
                x.workflowNode().reference().kind() == WorkflowNode.Reference.Kind.SUB_WORKFLOW_REF)
        .map(x -> x.workflowNode().reference().subWorkflowRef())
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));
  }

  private void registerTasks(
      Config config,
      FlyteAdminClient adminClient,
      Map<TaskIdentifier, RunnableTask> tasks,
      Map<String, String> env,
      Collection<ClassLoader> modules) {

    if (tasks.isEmpty()) {
      LOG.info("Skipping artifact staging because there are no runnable tasks");
      return;
    }

    List<KeyValuePair> envList =
        env.entrySet().stream()
            .map(entry -> KeyValuePair.of(entry.getKey(), entry.getValue()))
            .collect(toList());

    ArtifactStager stager = getArtifactStager(config, modules);

    List<Artifact> artifacts = stagePackageFiles(stager, packageDir);
    Artifact indexFile = stageIndexFile(stager, artifacts);

    for (Map.Entry<TaskIdentifier, RunnableTask> entry : tasks.entrySet()) {
      TaskIdentifier taskId = entry.getKey();
      RunnableTask task = entry.getValue();

      Struct defaultCustom = serializeToStruct(indexFile.location(), artifacts);

      TaskTemplate taskTemplate =
          createTaskTemplate(
              task,
              /* indexFileLocation= */ indexFile.location(),
              /* defaultCustom= */ defaultCustom,
              /* image= */ config.image(),
              /* env= */ envList);

      adminClient.createTask(taskId, taskTemplate);
    }
  }

  private static List<Artifact> stagePackageFiles(ArtifactStager stager, String packageDir) {
    try (Stream<Path> fileStream = Files.list(Paths.get(packageDir))) {
      List<String> files = fileStream.map(x -> x.toFile().getAbsolutePath()).collect(toList());

      return stager.stageFiles(files);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Artifact stageIndexFile(ArtifactStager stager, List<Artifact> packageArtifacts) {
    // TODO use json, or something. json is nice because it's human-readable
    // TODO add crc32c checksums along with location
    String content =
        packageArtifacts.stream()
            .sorted(Comparator.comparing(Artifact::location))
            .map(Artifact::location)
            .collect(Collectors.joining("\n"));

    ByteSource contentBytes = ByteSource.wrap(content.getBytes(Charsets.UTF_8));

    Artifact indexArtifact = stager.getArtifact("classpath", contentBytes);

    stager.stageArtifact(indexArtifact, contentBytes);

    return indexArtifact;
  }

  @VisibleForTesting
  static Struct serializeToStruct(String indexFileLocation, List<Artifact> artifacts) {
    Struct jflyte =
        Struct.of(
            ImmutableMap.of(
                "index_file_location",
                Struct.Value.ofStringValue(indexFileLocation),
                "artifacts",
                Struct.Value.ofListValue(
                    artifacts.stream()
                        .map(RegisterWorkflows::serializeToStruct)
                        .map(Struct.Value::ofStructValue)
                        .collect(collectingAndThen(toList(), Collections::unmodifiableList)))));

    return Struct.of(ImmutableMap.of("jflyte", Struct.Value.ofStructValue(jflyte)));
  }

  @VisibleForTesting
  static Struct merge(Struct source, Struct target) {
    Map<String, Struct.Value> fields = new HashMap<>(target.fields());
    fields.putAll(source.fields());

    return Struct.of(Collections.unmodifiableMap(fields));
  }

  private static Struct serializeToStruct(Artifact artifact) {
    // we don't add size because we aren't sure if we want to expose it
    return Struct.of(
        ImmutableMap.of(
            "name", Struct.Value.ofStringValue(artifact.name()),
            "location", Struct.Value.ofStringValue(artifact.location())));
  }
}
