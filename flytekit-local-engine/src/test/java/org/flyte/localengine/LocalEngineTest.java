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

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.flyte.flytekit.SdkBindingData.ofInteger;
import static org.flyte.flytekit.SdkConditions.eq;
import static org.flyte.flytekit.SdkConditions.when;
import static org.flyte.localengine.TestingListener.ofCompleted;
import static org.flyte.localengine.TestingListener.ofError;
import static org.flyte.localengine.TestingListener.ofPending;
import static org.flyte.localengine.TestingListener.ofRetrying;
import static org.flyte.localengine.TestingListener.ofStarting;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;
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
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;
import org.flyte.localengine.examples.CollatzConjectureStepWorkflow;
import org.flyte.localengine.examples.FibonacciWorkflow;
import org.flyte.localengine.examples.ListWorkflow;
import org.flyte.localengine.examples.MapWorkflow;
import org.flyte.localengine.examples.NestedSubWorkflow;
import org.flyte.localengine.examples.RetryableTask;
import org.flyte.localengine.examples.RetryableWorkflow;
import org.flyte.localengine.examples.StructWorkflow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class LocalEngineTest {

  @Test
  void testFibonacci() {
    Literal fib0 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(0L)));
    Literal fib1 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1L)));
    Literal fib2 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1L)));
    Literal fib3 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(2L)));
    Literal fib4 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3L)));
    Literal fib5 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(5L)));

    WorkflowTemplate workflowTemplate = new FibonacciWorkflow().toIdlTemplate();
    Map<String, RunnableTask> tasks = loadTasks();

    TestingListener listener = new TestingListener();

    Map<String, Literal> outputs =
        new LocalEngine(
                ExecutionContext.builder().runnableTasks(tasks).executionListener(listener).build())
            .compileAndExecute(workflowTemplate, ImmutableMap.of("fib0", fib0, "fib1", fib1));

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
                    "fib-2", ImmutableMap.of("a", fib0, "b", fib1), ImmutableMap.of("o", fib2)))
            .add(ofStarting("fib-3", ImmutableMap.of("a", fib1, "b", fib2)))
            .add(
                ofCompleted(
                    "fib-3", ImmutableMap.of("a", fib1, "b", fib2), ImmutableMap.of("o", fib3)))
            .add(ofStarting("fib-4", ImmutableMap.of("a", fib2, "b", fib3)))
            .add(
                ofCompleted(
                    "fib-4", ImmutableMap.of("a", fib2, "b", fib3), ImmutableMap.of("o", fib4)))
            .add(ofStarting("fib-5", ImmutableMap.of("a", fib3, "b", fib4)))
            .add(
                ofCompleted(
                    "fib-5", ImmutableMap.of("a", fib3, "b", fib4), ImmutableMap.of("o", fib5)))
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
    WorkflowTemplate workflow = new MapWorkflow().toIdlTemplate();
    Map<String, RunnableTask> tasks = loadTasks();

    Map<String, Literal> outputs =
        new LocalEngine(ExecutionContext.builder().runnableTasks(tasks).build())
            .compileAndExecute(workflow, ImmutableMap.of());

    // 3 = 1 + 2, 7 = 3 + 4
    Literal i3 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3)));
    Literal i7 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7)));

    assertEquals(ImmutableMap.of("map", Literal.ofMap(ImmutableMap.of("e", i3, "f", i7))), outputs);
  }

  //TODO: Enable this test when the struct will be supported
  @Disabled
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
    WorkflowTemplate workflow = new RetryableWorkflow().toIdlTemplate();
    Map<String, RunnableTask> tasks = loadTasks();

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
    Literal outerO = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(9L)));
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

    assertEquals(ImmutableMap.of("o", result), outputs);
    assertEquals(
        ImmutableList.<List<Object>>builder()
            .add(ofPending("nested-workflow"))
            .add(ofStarting("nested-workflow", ImmutableMap.of("a", a, "b", b, "c", c)))
            .add(ofPending("outer-sum-a-b"))
            .add(ofPending("outer-sum-ab-c"))
            .add(ofStarting("outer-sum-a-b", ImmutableMap.of("a", a, "b", b)))
            .add(
                ofCompleted(
                    "outer-sum-a-b", ImmutableMap.of("a", a, "b", b), ImmutableMap.of("o", outerO)))
            .add(ofStarting("outer-sum-ab-c", ImmutableMap.of("a", outerA, "b", outerB)))
            .add(ofPending("inner-sum-a-b"))
            .add(ofStarting("inner-sum-a-b", ImmutableMap.of("a", innerA, "b", innerB)))
            .add(
                ofCompleted(
                    "inner-sum-a-b",
                    ImmutableMap.of("a", outerA, "b", outerB),
                    ImmutableMap.of("o", result)))
            .add(
                ofCompleted(
                    "outer-sum-ab-c",
                    ImmutableMap.of("a", outerA, "b", outerB),
                    ImmutableMap.of("o", result)))
            .add(
                ofCompleted(
                    "nested-workflow",
                    ImmutableMap.of("a", a, "b", b, "c", c),
                    ImmutableMap.of("o", result)))
            .build(),
        listener.actions);
  }

  @ParameterizedTest
  @MethodSource("testBranchNodesProvider")
  void testBranchNodes(long x, long expected, List<List<Object>> expectedEvents) {
    String workflowName = new CollatzConjectureStepWorkflow().getName();

    Literal xLiteral = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(x)));
    Literal expectedLiteral =
        Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(expected)));

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
            .compileAndExecute(workflowTemplates.get(workflowName), ImmutableMap.of("x", xLiteral));

    assertEquals(ImmutableMap.of("nextX", expectedLiteral), outputs);
    assertEquals(expectedEvents, listener.actions);
  }

  @ParameterizedTest
  @MethodSource("testBranchNodesCasesProvider")
  void testBranchNodesCases(long x, List<List<Object>> expectedEvents) {
    String workflowName = new TestCaseExhaustivenessWorkflow().getName();

    Literal xLiteral = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(x)));

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
            .compileAndExecute(workflowTemplates.get(workflowName), ImmutableMap.of("x", xLiteral));

    assertEquals(ImmutableMap.of("nextX", xLiteral), outputs);
    for (int i = 0; i < expectedEvents.size(); i++) {
      List<Object> expected = expectedEvents.get(i);
      List<Object> actual = listener.actions.get(i);

      assertEquals(expected, actual, "" + i);
    }
    assertEquals(expectedEvents, listener.actions);
  }

  @Test
  void testBranchNodesMatchedNoCases() {
    String workflowName = new TestCaseExhaustivenessWorkflow().getName();

    Literal xLiteral = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(9999)));

    Map<String, WorkflowTemplate> workflowTemplates = loadWorkflows();
    Map<String, RunnableTask> tasks = loadTasks();

    TestingListener listener = new TestingListener();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new LocalEngine(
                        ExecutionContext.builder()
                            .runnableTasks(tasks)
                            .executionListener(listener)
                            .workflowTemplates(workflowTemplates)
                            .build())
                    .compileAndExecute(
                        workflowTemplates.get(workflowName), ImmutableMap.of("x", xLiteral)));

    assertEquals("No cases matched for branch node [decide]", ex.getMessage());
  }

  public static Stream<Arguments> testBranchNodesCasesProvider() {
    Literal one = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1L)));
    Literal two = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(2L)));
    return Stream.of(
        Arguments.of(
            1L,
            ImmutableList.<List<Object>>builder()
                .add(ofPending("decide"))
                .add(ofStarting("decide", ImmutableMap.of("$0", one, "$1", one)))
                .add(ofPending("eq_1"))
                .add(ofStarting("eq_1", ImmutableMap.of("x", one)))
                .add(ofCompleted("eq_1", singletonMap("x", one), singletonMap("x", one)))
                .add(
                    ofCompleted(
                        "decide", ImmutableMap.of("$0", one, "$1", one), singletonMap("x", one)))
                .build()),
        Arguments.of(
            2L,
            ImmutableList.<List<Object>>builder()
                .add(ofPending("decide"))
                .add(ofStarting("decide", ImmutableMap.of("$0", two, "$1", two)))
                .add(ofPending("eq_2"))
                .add(ofStarting("eq_2", singletonMap("x", two)))
                .add(ofCompleted("eq_2", singletonMap("x", two), singletonMap("x", two)))
                .add(
                    ofCompleted(
                        "decide", ImmutableMap.of("$0", two, "$1", two), singletonMap("x", two)))
                .build()));
  }

  public static Stream<Arguments> testBranchNodesProvider() {
    Literal oddX = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(7L)));
    Literal odd3XPlus1 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(22L)));
    Literal evenX = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(6L)));
    Literal evenXDividedBy2 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3L)));
    Literal literal2 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(2L)));
    Literal literalFalse = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(false)));
    Literal literalTrue = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(true)));

    return Stream.of(
        Arguments.of(
            7L,
            22L,
            ImmutableList.<List<Object>>builder()
                .add(ofPending("is_odd"))
                .add(ofPending("decide"))
                .add(ofStarting("is_odd", singletonMap("x", oddX)))
                .add(
                    ofCompleted(
                        "is_odd", singletonMap("x", oddX), singletonMap("res", literalFalse)))
                .add(ofStarting("decide", singletonMap("$0", literalFalse)))
                .add(ofPending("was_odd"))
                .add(ofStarting("was_odd", singletonMap("x", oddX)))
                .add(
                    ofCompleted(
                        "was_odd", singletonMap("x", oddX), singletonMap("o", odd3XPlus1)))
                .add(
                    ofCompleted(
                        "decide",
                        singletonMap("$0", literalFalse),
                        singletonMap("o", odd3XPlus1)))
                .build()),
        Arguments.of(
            6L,
            3L,
            ImmutableList.<List<Object>>builder()
                .add(ofPending("is_odd"))
                .add(ofPending("decide"))
                .add(ofStarting("is_odd", singletonMap("x", evenX)))
                .add(
                    ofCompleted(
                        "is_odd", singletonMap("x", evenX), singletonMap("res", literalTrue)))
                .add(ofStarting("decide", singletonMap("$0", literalTrue)))
                .add(ofPending("was_even"))
                .add(ofStarting("was_even", ImmutableMap.of("num", evenX, "den", literal2)))
                .add(
                    ofCompleted(
                        "was_even",
                        ImmutableMap.of("num", evenX, "den", literal2),
                        singletonMap("o", evenXDividedBy2)))
                .add(
                    ofCompleted(
                        "decide",
                        singletonMap("$0", literalTrue),
                        singletonMap("o", evenXDividedBy2)))
                .build()));
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

  @AutoService(SdkWorkflow.class)
  public static class TestCaseExhaustivenessWorkflow
      extends SdkWorkflow<TestCaseExhaustivenessWorkflow.NoOpType> {

    public TestCaseExhaustivenessWorkflow() {
      super(JacksonSdkType.of(NoOpType.class));
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData<Long> x = builder.inputOfInteger("x");
      SdkBindingData<Long> nextX =
          builder
              .apply(
                  "decide",
                  when("eq_1", eq(ofInteger(1L), x), NoOp.of(x))
                      .when("eq_2", eq(ofInteger(2L), x), NoOp.of(x)))
              .getOutputs()
              .x();

      builder.output("nextX", nextX);
    }

    @AutoService(SdkRunnableTask.class)
    public static class NoOp extends SdkRunnableTask<NoOpType, NoOpType> {
      private static final long serialVersionUID = 327687642904919547L;

      public NoOp() {
        super(JacksonSdkType.of(NoOpType.class), JacksonSdkType.of(NoOpType.class));
      }

      @Override
      public NoOpType run(NoOpType input) {
        return NoOpType.create(input.x());
      }

      static SdkTransform<NoOpType> of(SdkBindingData<Long> x) {
        return new NoOp().withInput("x", x);
      }
    }

    @AutoValue
    public abstract static class NoOpType {
      abstract SdkBindingData<Long> x();

      public static NoOpType create(SdkBindingData<Long> x) {
        return new AutoValue_LocalEngineTest_TestCaseExhaustivenessWorkflow_NoOpType(
            x);
      }
    }
  }
}
