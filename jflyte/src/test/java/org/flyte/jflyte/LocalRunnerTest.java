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

import static org.flyte.jflyte.TestingListener.ofCompleted;
import static org.flyte.jflyte.TestingListener.ofPending;
import static org.flyte.jflyte.TestingListener.ofStarting;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.jflyte.examples.FibonacciWorkflow;
import org.junit.jupiter.api.Test;

class LocalRunnerTest {

  @Test
  void testFibonacci() {
    Map<String, String> env =
        ImmutableMap.of(
            "JFLYTE_DOMAIN", "development",
            "JFLYTE_VERSION", "test",
            "JFLYTE_PROJECT", "flytetester");

    String workflowName = new FibonacciWorkflow().getName();

    Literal fib0 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(0L)));
    Literal fib1 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(1L)));
    Literal fib2 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(1L)));
    Literal fib3 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(2L)));
    Literal fib4 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(3L)));
    Literal fib5 = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(5L)));

    Map<String, WorkflowTemplate> workflows = Modules.loadWorkflows(env);
    Map<String, RunnableTask> tasks = Modules.loadTasks(env);
    WorkflowTemplate workflow = workflows.get(workflowName);

    TestingListener listener = new TestingListener();

    Map<String, Literal> outputs =
        LocalRunner.compileAndExecute(
            workflow, tasks, ImmutableMap.of("fib0", fib0, "fib1", fib1), listener);

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
}
