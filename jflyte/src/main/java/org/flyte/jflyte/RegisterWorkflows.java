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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.FileSystemRegistrar;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Registers all workflows on classpath. */
@Command(name = "workflows")
public class RegisterWorkflows implements Callable<Integer> {

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

  @Override
  public Integer call() {
    Config config = Config.load();
    ArtifactStager stager = getArtifactStager(config);

    try (FlyteAdminClient adminClient =
        FlyteAdminClient.create(config.platformUrl(), config.platformInsecure())) {
      registerAll(stager, config.image(), adminClient);
    }

    return 0;
  }

  private ArtifactStager getArtifactStager(Config config) {
    try {
      URI stagingUri = new URI(config.stagingLocation());
      ClassLoader pluginClassLoader = ClassLoaders.forDirectory(config.pluginDir());

      FileSystem stagingFileSystem =
          FileSystemRegistrar.getFileSystem(stagingUri.getScheme(), pluginClassLoader);

      return new ArtifactStager(config.stagingLocation(), stagingFileSystem);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to parse stagingLocation", e);
    }
  }

  private TaskTemplate createTaskTemplate(
      RunnableTask task, String indexFileLocation, String image, List<KeyValuePair> env) {
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

    return TaskTemplate.create(container, task.getInterface());
  }

  private void registerAll(ArtifactStager stager, String image, FlyteAdminClient adminClient) {
    ClassLoader packageClassLoader = ClassLoaders.forDirectory(packageDir);

    List<Artifact> artifacts = stagePackageFiles(stager, packageDir);
    Artifact indexFile = stageIndexFile(stager, artifacts);

    Map<String, String> env =
        ImmutableMap.of(
            "JFLYTE_DOMAIN", domain,
            "JFLYTE_PROJECT", project,
            "JFLYTE_VERSION", version);

    List<KeyValuePair> envList =
        env.entrySet().stream()
            .map(entry -> KeyValuePair.of(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());

    // before we run anything, switch class loader, because we will be touching user classes;
    // setting it in thread context will give us access to the right class loader
    ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(packageClassLoader);

    Map<TaskIdentifier, RunnableTask> tasks;
    Map<WorkflowIdentifier, WorkflowTemplate> workflows;
    try {
      tasks = Registrars.loadAll(RunnableTaskRegistrar.class, env);
      workflows = Registrars.loadAll(WorkflowTemplateRegistrar.class, env);
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextClassLoader);
    }

    IdentifierRewrite identifierRewrite =
        IdentifierRewrite.builder()
            .adminClient(adminClient)
            .domain(domain)
            .project(project)
            .version(version)
            .build();

    for (Map.Entry<TaskIdentifier, RunnableTask> entry : tasks.entrySet()) {
      TaskIdentifier taskId = entry.getKey();
      RunnableTask task = entry.getValue();

      TaskTemplate taskTemplate =
          createTaskTemplate(
              task,
              /* indexFileLocation= */ indexFile.location(),
              /* image= */ image,
              /* env= */ envList);

      adminClient.createTask(taskId, taskTemplate);
    }

    for (Map.Entry<WorkflowIdentifier, WorkflowTemplate> entry : workflows.entrySet()) {
      WorkflowIdentifier workflowId = entry.getKey();
      WorkflowTemplate workflowTemplate = identifierRewrite.apply(entry.getValue());

      adminClient.createWorkflow(workflowId, workflowTemplate);

      // for each workflow, create default launch plan
      LaunchPlanIdentifier launchPlanId =
          LaunchPlanIdentifier.create(
              workflowId.domain(), workflowId.project(), workflowId.name(), workflowId.version());

      adminClient.createLaunchPlan(launchPlanId, workflowId);
    }
  }

  private static List<Artifact> stagePackageFiles(ArtifactStager stager, String packageDir) {
    try (Stream<Path> fileStream = Files.list(Paths.get(packageDir))) {
      List<String> files =
          fileStream.map(x -> x.toFile().getAbsolutePath()).collect(Collectors.toList());

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
}
