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
package org.flyte.jflyte;

import static org.flyte.jflyte.utils.TokenSourceFactoryLoader.getTokenSource;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.flyte.jflyte.api.TokenSource;
import org.flyte.jflyte.utils.ArtifactStager;
import org.flyte.jflyte.utils.ClassLoaders;
import org.flyte.jflyte.utils.Config;
import org.flyte.jflyte.utils.ExecutionConfig;
import org.flyte.jflyte.utils.FlyteAdminClient;
import org.flyte.jflyte.utils.ProjectClosure;
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

    ExecutorService executorService = new ForkJoinPool();

    try (FlyteAdminClient adminClient =
        FlyteAdminClient.create(config.platformUrl(), config.platformInsecure(), tokenSource)) {
      Supplier<ArtifactStager> stagerSupplier =
          () -> ArtifactStager.create(config, modules, executorService);

      ExecutionConfig executionConfig =
          ExecutionConfig.builder()
              .domain(domain)
              .version(version)
              .project(project)
              .image(config.image())
              .build();

      ProjectClosure closure =
          ProjectClosure.loadAndStage(packageDir, executionConfig, stagerSupplier, adminClient);

      closure.taskSpecs().forEach((id, spec) -> adminClient.createTask(id, spec.taskTemplate()));

      closure
          .workflowSpecs()
          .forEach(
              (id, spec) ->
                  adminClient.createWorkflow(id, spec.workflowTemplate(), spec.subWorkflows()));

      closure.launchPlans().forEach(adminClient::createLaunchPlan);
    } finally {
      executorService.shutdownNow();
    }

    return 0;
  }
}
