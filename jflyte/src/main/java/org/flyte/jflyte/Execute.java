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

import static org.flyte.jflyte.utils.ClassLoaders.withClassLoader;
import static org.flyte.jflyte.utils.MoreCollectors.toUnmodifiableMap;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.utils.ClassLoaders;
import org.flyte.jflyte.utils.Config;
import org.flyte.jflyte.utils.FileSystemLoader;
import org.flyte.jflyte.utils.PackageLoader;
import org.flyte.jflyte.utils.ProtoReader;
import org.flyte.jflyte.utils.ProtoUtil;
import org.flyte.jflyte.utils.ProtoWriter;
import org.flyte.jflyte.utils.Registrars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Handler for "execute" command. */
@Command(name = "execute")
public class Execute implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(Execute.class);

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

      // before we run anything, switch class loader, otherwise,
      // ServiceLoaders and other things wouldn't work, for instance,
      // FileSystemRegister in Apache Beam
      Map<String, Literal> outputs =
          withClassLoader(
              packageClassLoader,
              () -> {
                Map<String, Literal> input = protoReader.getInput(inputs);
                RunnableTask runnableTask = getTask(task);

                return runnableTask.run(input);
              });

      protoWriter.writeOutputs(outputs);
    } catch (ContainerError e) {
      LOG.error("failed to run task", e);

      protoWriter.writeError(ProtoUtil.serializeContainerError(e));
    } catch (Throwable e) {
      LOG.error("failed to run task", e);

      protoWriter.writeError(ProtoUtil.serializeThrowable(e));
    }
  }

  private static Map<String, String> getEnv() {
    return System.getenv().entrySet().stream()
        // we keep JFLYTE_ only for backwards-compatibility
        .filter(x -> x.getKey().startsWith("JFLYTE_") || x.getKey().startsWith("FLYTE_"))
        .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static RunnableTask getTask(String name) {
    // be careful not to pass extra
    Map<String, String> env = getEnv();
    Map<TaskIdentifier, RunnableTask> tasks = Registrars.loadAll(RunnableTaskRegistrar.class, env);

    for (Map.Entry<TaskIdentifier, RunnableTask> entry : tasks.entrySet()) {
      if (entry.getKey().name().equals(name)) {
        return entry.getValue();
      }
    }

    throw new IllegalArgumentException("Task not found: " + name);
  }
}
