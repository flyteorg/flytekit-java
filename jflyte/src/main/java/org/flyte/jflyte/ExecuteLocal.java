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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Unmatched;

/** Handler for "execute-local" command. */
@Command(name = "execute-local")
public class ExecuteLocal implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecuteLocal.class);

  @Option(
      names = {"--workflow"},
      required = true)
  private String workflowName;

  @Option(
      names = {"-cp", "--classpath"},
      description = "Directory with packaged jars",
      required = false)
  private String packageDir;

  @Unmatched private String[] inputArgs;

  @Override
  public Integer call() {
    List<ClassLoader> modules = loadModules();

    Map<String, String> env =
        ImmutableMap.of(
            "JFLYTE_DOMAIN", "development",
            "JFLYTE_VERSION", "test",
            "JFLYTE_PROJECT", "flytetester");

    Map<String, RunnableTask> tasks =
        modules.stream()
            .flatMap(module -> loadTasks(module, env).entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<String, WorkflowTemplate> workflows =
        modules.stream()
            .flatMap(module -> loadWorkflows(module, env).entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    WorkflowTemplate workflow =
        Preconditions.checkNotNull(
            workflows.get(workflowName), "workflow not found [%s]", workflowName);

    CommandLine.Model.CommandSpec spec = CommandLine.Model.CommandSpec.create();
    spec.usageMessage().customSynopsis(getCustomSynopsis());
    workflow
        .interface_()
        .inputs()
        .forEach((name, variable) -> spec.addOption(getOption(name, variable)));

    Map<String, Literal> inputs = parseInputs(spec, workflow.interface_().inputs(), inputArgs);

    try {
      LocalRunner.compileAndExecute(workflow, tasks, inputs);

      return 0;
    } catch (Throwable e) {
      return handleException(e);
    }
  }

  protected int handleException(Throwable e) {
    LOG.error("Unhandled exception", e);

    return -1;
  }

  private String getCustomSynopsis() {
    StringBuilder sb = new StringBuilder();

    sb.append("jflyte execute-local --workflow=").append(workflowName);

    if (packageDir != null) {
      sb.append(" ").append("--classpath=").append(packageDir);
    }

    return sb.toString();
  }

  private List<ClassLoader> loadModules() {
    Config config = Config.load();
    List<ClassLoader> modules = ClassLoaders.forModuleDir(config.moduleDir());

    if (packageDir != null) {
      ClassLoader packageClassLoader = ClassLoaders.forDirectory(new File(packageDir));

      return ImmutableList.<ClassLoader>builder().addAll(modules).add(packageClassLoader).build();
    } else {
      return modules;
    }
  }

  static Map<String, RunnableTask> loadTasks(ClassLoader classLoader, Map<String, String> env) {
    Map<String, RunnableTask> tasks = withClassLoader(classLoader, () -> Modules.loadTasks(env));

    return tasks.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> new RunnableTaskWithClassLoader(entry.getValue(), classLoader)));
  }

  static Map<String, WorkflowTemplate> loadWorkflows(
      ClassLoader classLoader, Map<String, String> env) {
    return withClassLoader(classLoader, () -> Modules.loadWorkflows(env));
  }

  /** Wraps RunnableTask to change ClassLoader before running. */
  private static class RunnableTaskWithClassLoader implements RunnableTask {
    private final RunnableTask runnableTask;
    private final ClassLoader classLoader;

    private RunnableTaskWithClassLoader(RunnableTask runnableTask, ClassLoader classLoader) {
      this.runnableTask = runnableTask;
      this.classLoader = classLoader;
    }

    @Override
    public String getName() {
      return runnableTask.getName();
    }

    @Override
    public TypedInterface getInterface() {
      return runnableTask.getInterface();
    }

    @Override
    public Map<String, Literal> run(Map<String, Literal> inputs) {
      return withClassLoader(classLoader, () -> runnableTask.run(inputs));
    }
  }

  /**
   * Override to customize how options are parsed, and provide default values if necessary.
   *
   * @param name option name
   * @param variable variable
   * @return option spec
   */
  protected CommandLine.Model.OptionSpec getOption(String name, Variable variable) {
    // FIXME we assume for now that there are only simple types, because everything else isn't
    // implemented, we should improve error message once we support other cases
    SimpleType simpleType = variable.literalType().simpleType();

    CommandLine.Model.OptionSpec.Builder builder =
        CommandLine.Model.OptionSpec.builder("--" + name)
            .converters(new LiteralTypeConverter(simpleType))
            .required(true);

    if (variable.description() != null) {
      builder.description(variable.description());
    }

    return builder.build();
  }

  static Map<String, Literal> parseInputs(
      CommandLine.Model.CommandSpec spec, Map<String, Variable> variableMap, String[] inputs) {
    CommandLine.ParseResult result =
        new CommandLine(spec).parseArgs(inputs != null ? inputs : new String[0]);

    return variableMap.keySet().stream()
        .map(name -> Maps.immutableEntry(name, result.matchedOptionValue(name, (Literal) null)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
