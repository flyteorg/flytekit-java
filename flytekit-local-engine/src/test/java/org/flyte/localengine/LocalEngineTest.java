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
package org.flyte.localengine;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static org.flyte.localengine.TestingListener.ofCompleted;
import static org.flyte.localengine.TestingListener.ofError;
import static org.flyte.localengine.TestingListener.ofPending;
import static org.flyte.localengine.TestingListener.ofRetrying;
import static org.flyte.localengine.TestingListener.ofStarting;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Registrar;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;
import org.flyte.localengine.examples.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LocalEngineTest {

  @Test
  void testFibonacci() {
    String workflowName = new FibonacciWorkflow().getName();

    Literal fib0 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(0L)));
    Literal fib1 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1L)));
    Literal fib2 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1L)));
    Literal fib3 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(2L)));
    Literal fib4 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3L)));
    Literal fib5 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(5L)));

    Map<String, WorkflowTemplate> workflows = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();

    TestingListener listener = new TestingListener();

    Map<String, Literal> outputs =
        new LocalEngine(
                ExecutionContext.builder().runnableTasks(tasks).executionListener(listener).build())
            .compileAndExecute(
                workflows.get(workflowName), ImmutableMap.of("fib0", fib0, "fib1", fib1));

    assertEquals(ImmutableMap.of("fib4", fib4, "fib5", fib5), outputs);
    assertEquals(
        ImmutableList.<List<Object>>builder()
            .add(ofPending("fib-2"))
            .add(ofPending("fib-3"))
            .add(ofPending("fib-4"))
            .add(ofPending("fib-5"))
            .add(ofStarting("fib-2", ImmutableMap.of("a", fib0, "b", fib1)))
            .add(
                ofCompleted(
                    "fib-2", ImmutableMap.of("a", fib0, "b", fib1), ImmutableMap.of("c", fib2)))
            .add(ofStarting("fib-3", ImmutableMap.of("a", fib1, "b", fib2)))
            .add(
                ofCompleted(
                    "fib-3", ImmutableMap.of("a", fib1, "b", fib2), ImmutableMap.of("c", fib3)))
            .add(ofStarting("fib-4", ImmutableMap.of("a", fib2, "b", fib3)))
            .add(
                ofCompleted(
                    "fib-4", ImmutableMap.of("a", fib2, "b", fib3), ImmutableMap.of("c", fib4)))
            .add(ofStarting("fib-5", ImmutableMap.of("a", fib3, "b", fib4)))
            .add(
                ofCompleted(
                    "fib-5", ImmutableMap.of("a", fib3, "b", fib4), ImmutableMap.of("c", fib5)))
            .build(),
        listener.actions);
  }

  @Test
  public void testBindingCollection() {
    String workflowName = new ListWorkflow().getName();

    Map<String, WorkflowTemplate> workflows = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();
    WorkflowTemplate workflow = workflows.get(workflowName);

    Map<String, Literal> outputs =
        new LocalEngine(ExecutionContext.builder().runnableTasks(tasks).build())
            .compileAndExecute(workflow, ImmutableMap.of());

    // 3 = 1 + 2, 7 = 3 + 4
    Literal i3 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3)));
    Literal i7 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7)));

    assertEquals(ImmutableMap.of("list", Literal.ofCollection(ImmutableList.of(i3, i7))), outputs);
  }

  @Test
  public void testBindingMap() {
    String workflowName = new MapWorkflow().getName();

    Map<String, WorkflowTemplate> workflows = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();
    WorkflowTemplate workflow = workflows.get(workflowName);

    Map<String, Literal> outputs =
        new LocalEngine(ExecutionContext.builder().runnableTasks(tasks).build())
            .compileAndExecute(workflow, ImmutableMap.of());

    // 3 = 1 + 2, 7 = 3 + 4
    Literal i3 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3)));
    Literal i7 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7)));

    assertEquals(ImmutableMap.of("map", Literal.ofMap(ImmutableMap.of("e", i3, "f", i7))), outputs);
  }

  @Test
  public void testStructWorkflow() {
    String workflowName = new StructWorkflow().getName();

    Map<String, WorkflowTemplate> workflows = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();
    WorkflowTemplate workflow = workflows.get(workflowName);

    Literal inputStructLiteral =
        Literal.ofScalar(
            Scalar.ofGeneric(
                Struct.of(
                    ImmutableMap.of(
                        "someKey1", Struct.Value.ofStringValue("some_value_1"),
                        "someKey2", Struct.Value.ofBoolValue(true)))));

    Map<String, Literal> inputs =
        ImmutableMap.of(
            "someString",
            Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("some_string_value"))),
            "someStruct",
            inputStructLiteral);

    Map<String, Literal> outputs =
        new LocalEngine(ExecutionContext.builder().runnableTasks(tasks).build())
            .compileAndExecute(workflow, inputs);

    Literal expectedOutput =
        Literal.ofScalar(
            Scalar.ofGeneric(
                Struct.of(
                    ImmutableMap.of(
                        "someKey1", Struct.Value.ofStringValue("some_value_1-output"),
                        "someKey2", Struct.Value.ofBoolValue(true)))));
    assertEquals(expectedOutput, outputs.get("outputStructData"));
  }

  @Test
  public void testRetryableTask_completed() {
    String workflowName = new RetryableWorkflow().getName();

    Map<String, WorkflowTemplate> workflows = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();
    WorkflowTemplate workflow = workflows.get(workflowName);

    TestingListener listener = new TestingListener();

    // make sure we don't run two tests in parallel
    synchronized (RetryableTask.class) {
      RetryableTask.ATTEMPTS_BEFORE_SUCCESS.set(5L);
      RetryableTask.ATTEMPTS.set(0L);

      new LocalEngine(
              ExecutionContext.builder().runnableTasks(tasks).executionListener(listener).build())
          .compileAndExecute(workflow, ImmutableMap.of());

      assertEquals(
          ImmutableList.<List<Object>>builder()
              .add(ofPending("node-1"))
              .add(ofStarting("node-1", ImmutableMap.of()))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 1))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 2))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 3))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 4))
              .add(ofCompleted("node-1", ImmutableMap.of(), ImmutableMap.of()))
              .build(),
          listener.actions);

      // will finish on attempt number 5 according to ATTEMPTS_BEFORE_SUCCESS
      assertEquals(5L, RetryableTask.ATTEMPTS.get());
    }
  }

  @Test
  public void testRetryableTask_failed() {
    String workflowName = new RetryableWorkflow().getName();

    Map<String, WorkflowTemplate> workflows = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();
    WorkflowTemplate workflow = workflows.get(workflowName);

    TestingListener listener = new TestingListener();

    // make sure we don't run two tests in parallel
    synchronized (RetryableTask.class) {
      // will never succeed within retry limit
      RetryableTask.ATTEMPTS_BEFORE_SUCCESS.set(10);
      RetryableTask.ATTEMPTS.set(0L);

      RuntimeException e =
          Assertions.assertThrows(
              RuntimeException.class,
              () ->
                  new LocalEngine(
                          ExecutionContext.builder()
                              .runnableTasks(tasks)
                              .executionListener(listener)
                              .build())
                      .compileAndExecute(workflow, ImmutableMap.of()));

      assertEquals("oops", e.getMessage());

      assertEquals(
          ImmutableList.<List<Object>>builder()
              .add(ofPending("node-1"))
              .add(ofStarting("node-1", ImmutableMap.of()))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 1))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 2))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 3))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 4))
              .add(ofRetrying("node-1", ImmutableMap.of(), "oops", /* attempt= */ 5))
              .add(ofError("node-1", ImmutableMap.of(), "oops"))
              .build(),
          listener.actions);

      // getRetries() returns 5, so we have 6 attempts/executions total
      assertEquals(6L, RetryableTask.ATTEMPTS.get());
    }
  }

  @Test
  public void testNestedSubWorkflow() {
    String workflowName = new NestedSubWorkflow().getName();

    Literal a = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3L)));
    Literal b = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(6L)));
    Literal c = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7L)));
    Literal result = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(16L)));
    Literal outerA = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(9L)));
    Literal outerB = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7L)));
    Literal outerC = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(9L)));
    Literal innerA = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(9L)));
    Literal innerB = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7L)));

    Map<String, WorkflowTemplate> workflowTemplates = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();

    TestingListener listener = new TestingListener();

    Map<String, Literal> outputs =
        new LocalEngine(
                ExecutionContext.builder()
                    .runnableTasks(tasks)
                    .executionListener(listener)
                    .workflowTemplates(workflowTemplates)
                    .build())
            .compileAndExecute(
                workflowTemplates.get(workflowName), ImmutableMap.of("a", a, "b", b, "c", c));

    assertEquals(ImmutableMap.of("result", result), outputs);
    assertEquals(
        ImmutableList.<List<Object>>builder()
            .add(ofPending("nested-workflow"))
            .add(ofStarting("nested-workflow", ImmutableMap.of("a", a, "b", b, "c", c)))
            .add(ofPending("outer-sum-a-b"))
            .add(ofPending("outer-sum-ab-c"))
            .add(ofStarting("outer-sum-a-b", ImmutableMap.of("a", a, "b", b)))
            .add(
                ofCompleted(
                    "outer-sum-a-b", ImmutableMap.of("a", a, "b", b), ImmutableMap.of("c", outerC)))
            .add(ofStarting("outer-sum-ab-c", ImmutableMap.of("a", outerA, "b", outerB)))
            .add(ofPending("inner-sum-a-b"))
            .add(ofStarting("inner-sum-a-b", ImmutableMap.of("a", innerA, "b", innerB)))
            .add(
                ofCompleted(
                    "inner-sum-a-b",
                    ImmutableMap.of("a", outerA, "b", outerB),
                    ImmutableMap.of("c", result)))
            .add(
                ofCompleted(
                    "outer-sum-ab-c",
                    ImmutableMap.of("a", outerA, "b", outerB),
                    ImmutableMap.of("result", result)))
            .add(
                ofCompleted(
                    "nested-workflow",
                    ImmutableMap.of("a", a, "b", b, "c", c),
                    ImmutableMap.of("result", result)))
            .build(),
        listener.actions);
  }

  @Test
  void testBranchNodes() {
    String workflowName = new CollatzConjectureStepWorkflow().getName();

    Literal x = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7L)));
    Literal expectedResult = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(22L)));

    Map<String, WorkflowTemplate> workflowTemplates = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();

    TestingListener listener = new TestingListener();

    Map<String, Literal> outputs =
        new LocalEngine(
                ExecutionContext.builder()
                    .runnableTasks(tasks)
                    .executionListener(listener)
                    .workflowTemplates(workflowTemplates)
                    .build())
            .compileAndExecute(workflowTemplates.get(workflowName), ImmutableMap.of("x", x));

    assertEquals(ImmutableMap.of("nextX", expectedResult), outputs);
    assertEquals(
        ImmutableList.<List<Object>>builder()
            .add(ofPending("nested-workflow"))
            .add(ofStarting("nested-workflow", emptyMap()))
            .add(ofPending("outer-sum-a-b"))
            .add(ofPending("outer-sum-ab-c"))
            .add(ofStarting("outer-sum-a-b", emptyMap()))
            .build(),
        listener.actions);
  }

  private static Map<String, WorkflowTemplate> loadWorkflows() {
    Map<String, String> env =
        ImmutableMap.of(
            "FLYTE_INTERNAL_DOMAIN", "development",
            "FLYTE_INTERNAL_VERSION", "test",
            "FLYTE_INTERNAL_PROJECT", "flytetester");

    Map<WorkflowIdentifier, WorkflowTemplate> registrarWorkflows =
        loadAll(WorkflowTemplateRegistrar.class, env);

    return registrarWorkflows.entrySet().stream()
        .collect(toMap(x -> x.getKey().name(), Map.Entry::getValue));
  }

  private static Map<String, RunnableTask> loadTasks() {
    Map<String, String> env =
        ImmutableMap.of(
            "FLYTE_INTERNAL_DOMAIN", "development",
            "FLYTE_INTERNAL_VERSION", "test",
            "FLYTE_INTERNAL_PROJECT", "flytetester");

    Map<TaskIdentifier, RunnableTask> registrarRunnableTasks =
        loadAll(RunnableTaskRegistrar.class, env);

    return registrarRunnableTasks.entrySet().stream()
        .collect(toMap(x -> x.getKey().name(), Map.Entry::getValue));
  }

  static <K, V, T extends Registrar<K, V>> Map<K, V> loadAll(
      Class<T> registrarClass, Map<String, String> env) {
    ServiceLoader<T> loader = ServiceLoader.load(registrarClass);

    Map<K, V> items = new HashMap<>();

    for (T registrar : loader) {
      for (Map.Entry<K, V> entry : registrar.load(env).entrySet()) {
        V previous = items.put(entry.getKey(), entry.getValue());

        if (previous != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Discovered a duplicate item [%s] [%s] [%s]",
                  entry.getKey(), entry.getValue(), previous));
        }
      }
    }

    return items;
  }
}
