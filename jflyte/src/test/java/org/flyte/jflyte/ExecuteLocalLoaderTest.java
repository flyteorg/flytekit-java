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

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.flyte.flytekit.OutputTransformer;
import org.flyte.flytekit.NopOutputTransformer;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTypes;
import org.junit.jupiter.api.Test;

public class ExecuteLocalLoaderTest {
  @Test
  void testLoadAll_withoutDuplicates() {
    ClassLoader classLoader1 = new TestClassLoader();
    ClassLoader classLoader2 = new TestClassLoader();
    TestTask task1 = new TestTask();
    TestTask task2 = new TestTask();

    Map<String, ClassLoader> modules =
        ImmutableMap.of("source1", classLoader1, "source2", classLoader2);
    Map<ClassLoader, Map<String, SdkRunnableTask<?, ?, ? extends OutputTransformer>>>
        tasksPerClassLoader =
            ImmutableMap.of(
                classLoader1, ImmutableMap.of("task1", task1),
                classLoader2, ImmutableMap.of("task2", task2));

    Map<String, SdkRunnableTask<?, ?, ? extends OutputTransformer>> tasksByName =
        ExecuteLocalLoader.loadAll(modules, (cl, __) -> tasksPerClassLoader.get(cl), emptyMap());

    assertEquals(ImmutableMap.of("task1", task1, "task2", task2), tasksByName);
  }

  @Test
  void testLoadAll_withDuplicates() {
    ClassLoader classLoader1 = new TestClassLoader();
    ClassLoader classLoader2 = new TestClassLoader();
    ClassLoader classLoader3 = new TestClassLoader();
    TestTask duplicateTask1 = new TestTask();
    TestTask duplicateTask2 = new TestTask();
    TestTask uniqueTask = new TestTask();

    Map<String, ClassLoader> modules =
        ImmutableMap.of("source1", classLoader1, "source2", classLoader2, "source3", classLoader3);
    Map<ClassLoader, Map<String, SdkRunnableTask<?, ?, ? extends OutputTransformer>>>
        tasksPerClassLoader =
            ImmutableMap.of(
                classLoader1, ImmutableMap.of("dupTask1", duplicateTask1, "unqTask", uniqueTask),
                classLoader2,
                    ImmutableMap.of("dupTask1", duplicateTask1, "dupTask2", duplicateTask2),
                classLoader3, ImmutableMap.of("dupTask2", duplicateTask2));

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ExecuteLocalLoader.loadAll(
                    modules, (cl, __) -> tasksPerClassLoader.get(cl), emptyMap()));

    assertEquals(
        "Found duplicate items among the modules: "
            + "{dupTask1 -> [source1, source2]}; {dupTask2 -> [source2, source3]}",
        exception.getMessage());
  }

  private static class TestTask extends SdkRunnableTask<Void, Void, NopOutputTransformer> {
    private static final long serialVersionUID = -2949483398581210936L;

    private TestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public Void run(Void input) {
      return null;
    }
  }

  private static class TestClassLoader extends ClassLoader {}
}
