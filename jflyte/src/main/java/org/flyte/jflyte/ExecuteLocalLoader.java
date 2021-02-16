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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SortedSetMultimap;
import com.google.protobuf.Struct;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.WorkflowTemplate;

/** Utility class to load modules for execute-local mode. */
public class ExecuteLocalLoader {
  public static Map<String, ClassLoader> loadModules(String packageDir) {
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

  public static Map<String, RunnableTask> loadTasks(
      Map<String, ClassLoader> modules, Map<String, String> env) {
    return loadAll(modules, ExecuteLocalLoader::loadTasks, env);
  }

  public static Map<String, WorkflowTemplate> loadWorkflows(
      Map<String, ClassLoader> modules, Map<String, String> env) {
    return loadAll(modules, ExecuteLocalLoader::loadWorkflows, env);
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

    @Override
    public RetryStrategy getRetries() {
      return runnableTask.getRetries();
    }

    @Override
    public String getType() {
      return runnableTask.getType();
    }

    @Override
    public Struct getCustom() {
      return runnableTask.getCustom();
    }
  }
}
