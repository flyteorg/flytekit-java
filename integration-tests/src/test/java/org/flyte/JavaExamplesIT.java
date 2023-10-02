/*
 * Copyright 2021-2023 Flyte Authors.
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
package org.flyte;

import static org.flyte.FlyteContainer.CLIENT;
import static org.flyte.examples.FlyteEnvironment.STAGING_DOMAIN;
import static org.flyte.utils.Literal.ofIntegerMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.core.Literals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JavaExamplesIT {
  private static final String CLASSPATH_EXAMPLES = "flytekit-examples/target/lib";
  private static final String CLASSPATH_EXAMPLES_SCALA = "flytekit-examples-scala/target/lib";

  @BeforeAll
  public static void beforeAll() {
    CLIENT.registerWorkflows(CLASSPATH_EXAMPLES);
    CLIENT.registerWorkflows(CLASSPATH_EXAMPLES_SCALA, STAGING_DOMAIN);
  }

  @Test
  public void testSumTask() {
    Literals.LiteralMap output =
        CLIENT.createTaskExecution(
            "org.flyte.examples.SumTask",
            ofIntegerMap(
                ImmutableMap.of(
                    "a", 4L,
                    "b", 16L)));

    assertThat(output, equalTo(ofIntegerMap(ImmutableMap.of("c", 20L))));
  }

  @Test
  public void testFibonacciWorkflow() {
    Literals.LiteralMap output =
        CLIENT.createExecution(
            "org.flyte.examples.FibonacciWorkflow",
            ofIntegerMap(
                ImmutableMap.of(
                    "fib0", 0L,
                    "fib1", 1L)));

    assertThat(output, equalTo(ofIntegerMap(ImmutableMap.of("fib5", 5L))));
  }

  @Test
  public void testDynamicFibonacciWorkflow() {
    Literals.LiteralMap output =
        CLIENT.createExecution(
            "org.flyte.examples.DynamicFibonacciWorkflow", ofIntegerMap(ImmutableMap.of("n", 2L)));

    assertThat(output, equalTo(ofIntegerMap(ImmutableMap.of("output", 1L))));
  }
}
