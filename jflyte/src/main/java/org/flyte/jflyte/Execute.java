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

import static org.flyte.jflyte.ClassLoaders.withClassLoader;
import static org.flyte.jflyte.MoreCollectors.toUnmodifiableMap;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.jflyte.api.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Handler for "execute" command. */
@Command(name = "execute")
public class Execute implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(Execute.class);

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

    FileSystem inputFs = FileSystemLoader.getFileSystem(fileSystems, inputs);
    FileSystem outputFs = FileSystemLoader.getFileSystem(fileSystems, outputPrefix);
    ProtoReaderWriter protoReaderWriter = new ProtoReaderWriter(outputPrefix, inputFs, outputFs);

    TaskTemplate taskTemplate = protoReaderWriter.getTaskTemplate(taskTemplatePath);
    ClassLoader packageClassLoader = PackageLoader.load(fileSystems, taskTemplate);

    try {
      // before we run anything, switch class loader, otherwise,
      // ServiceLoaders and other things wouldn't work, for instance,
      // FileSystemRegister in Apache Beam

      Map<String, Literal> outputs =
          withClassLoader(
              packageClassLoader,
              () -> {
                Map<String, Literal> input = protoReaderWriter.getInput(inputs);
                RunnableTask runnableTask = getTask(task);

                return runnableTask.run(input);
              });

      protoReaderWriter.writeOutputs(outputs);
    } catch (ContainerError e) {
      LOG.error("failed to run task", e);

      protoReaderWriter.writeError(ProtoUtil.serializeContainerError(e));
    } catch (Throwable e) {
      LOG.error("failed to run task", e);

      protoReaderWriter.writeError(ProtoUtil.serializeThrowable(e));
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
