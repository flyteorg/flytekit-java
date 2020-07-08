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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.flyte.jflyte.ClassLoaders.withClassLoader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SortedSetMultimap;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.RunnableTask;
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
    Map<String, ClassLoader> modules = loadModules();

    Map<String, String> env =
        ImmutableMap.of(
            "JFLYTE_DOMAIN", "development",
            "JFLYTE_VERSION", "test",
            "JFLYTE_PROJECT", "flytetester");

    Map<String, RunnableTask> tasks = loadAll(modules, ExecuteLocal::loadTasks, env);
    Map<String, WorkflowTemplate> workflows = loadAll(modules, ExecuteLocal::loadWorkflows, env);

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

  private Map<String, ClassLoader> loadModules() {
    Config config = Config.load();
    Map<String, ClassLoader> modules = ClassLoaders.forModuleDir(config.moduleDir());

    if (packageDir != null) {
      ClassLoader packageClassLoader = ClassLoaders.forDirectory(new File(packageDir));

      return ImmutableMap.<String, ClassLoader>builder()
          .putAll(modules)
          .put(packageDir, packageClassLoader)
          .build();
    } else {
      return modules;
    }
  }

  @VisibleForTesting
  static <ItemT> Map<String, ItemT> loadAll(
      Map<String, ClassLoader> modules,
      BiFunction<ClassLoader, Map<String, String>, Map<String, ItemT>> loader,
      Map<String, String> env) {
    Map<String, Map<String, ItemT>> loadedItemsBySource =
        modules.entrySet().stream()
            .map(mod -> Maps.immutableEntry(mod.getKey(), loader.apply(mod.getValue(), env)))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    verifyNoDuplicateItems(loadedItemsBySource);

    return loadedItemsBySource.values().stream()
        .flatMap(items -> items.entrySet().stream())
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static <ItemT> void verifyNoDuplicateItems(
      Map<String, Map<String, ItemT>> itemsBySources) {
    SortedSetMultimap<String, String> sourcesByItemId =
        MultimapBuilder.treeKeys().treeSetValues().build();
    itemsBySources.forEach(
        (source, items) -> items.keySet().forEach(itemId -> sourcesByItemId.put(itemId, source)));

    List<String> duplicateItemsId =
        sourcesByItemId.keySet().stream()
            .filter(itemId -> sourcesByItemId.get(itemId).size() > 1)
            .collect(toList());

    if (!duplicateItemsId.isEmpty()) {
      String errorMessage =
          duplicateItemsId.stream()
              .map(itemId -> String.format("{%s -> %s}", itemId, sourcesByItemId.get(itemId)))
              .collect(joining("; ", "Found duplicate items among the modules: ", ""));
      throw new RuntimeException(errorMessage);
    }
  }

  static Map<String, RunnableTask> loadTasks(ClassLoader classLoader, Map<String, String> env) {
    Map<String, RunnableTask> tasks = withClassLoader(classLoader, () -> Modules.loadTasks(env));

    return tasks.entrySet().stream()
        .collect(
            toMap(
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
   * Override to provide default values if necessary.
   *
   * @param name option name
   * @param variable variable
   * @return option spec
   */
  protected CommandLine.Model.OptionSpec getOption(String name, Variable variable) {
    String defaultValue = getDefaultValue(name);

    CommandLine.Model.OptionSpec.Builder builder =
        CommandLine.Model.OptionSpec.builder("--" + name)
            .converters(getLiteralTypeConverter(name, variable))
            .required(defaultValue == null);

    if (defaultValue != null) {
      builder.defaultValue(defaultValue);
    }

    if (variable.description() != null) {
      builder.description(variable.description());
    }

    return builder.build();
  }

  private LiteralTypeConverter getLiteralTypeConverter(String name, Variable variable) {
    LiteralType literalType = variable.literalType();
    switch (literalType.getKind()) {
      case SIMPLE_TYPE:
        return new LiteralTypeConverter(literalType.simpleType());

      case SCHEMA_TYPE:
      case COLLECTION_TYPE:
      case MAP_VALUE_TYPE:
      case BLOB_TYPE:
        // TODO: implements other types
        throw new IllegalArgumentException(
            String.format("Type of [%s] input parameter is not supported: %s", name, literalType));
    }
    throw new AssertionError("Unexpected LiteralType.Kind: " + literalType.getKind());
  }

  /**
   * Override to provide default values if necessary.
   *
   * @param name parameter name
   * @return literal
   */
  @Nullable
  protected String getDefaultValue(String name) {
    return null;
  }

  protected Map<String, Literal> parseInputs(
      CommandLine.Model.CommandSpec spec, Map<String, Variable> variableMap, String[] inputs) {
    CommandLine.ParseResult result =
        new CommandLine(spec).parseArgs(inputs != null ? inputs : new String[0]);

    return variableMap.entrySet().stream()
        .map(
            kv -> {
              String name = kv.getKey();
              Literal defaultValue = getDefaultValueAsLiteral(name, kv.getValue());

              return Maps.immutableEntry(name, result.matchedOptionValue(name, defaultValue));
            })
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Nullable
  private Literal getDefaultValueAsLiteral(String name, Variable variable) {
    String defaultValueString = getDefaultValue(name);
    if (defaultValueString == null) {
      return null;
    }

    return getLiteralTypeConverter(name, variable).convert(defaultValueString);
  }
}
