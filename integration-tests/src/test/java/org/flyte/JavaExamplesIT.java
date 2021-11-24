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
package org.flyte;

import static org.flyte.utils.Literal.ofIntegerMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.core.Literals;
import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;
import org.flyte.utils.FlyteSandboxClient;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class JavaExamplesIT {

  private static final FlyteSandboxClient CLIENT = FlyteSandboxClient.create();
  private static final String CLASSPATH = "flytekit-examples/target/lib";

  @BeforeAll
  public static void beforeAll() {
    CLIENT.registerWorkflows(CLASSPATH);
  }

  @Test
  @Ignore
  // FIXME doesn't work, seems like a bug in flyteadmin:
  // Invalid value: "srxnub62yu-orgflyteexamplesSumTask-0": a lowercase RFC 1123 subdomain must
  // consist of lower case
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
            "org.flyte.examples.DynamicFibonacciWorkflow", ofIntegerMap(ImmutableMap.of("n", 6L)));

    assertThat(output, equalTo(ofIntegerMap(ImmutableMap.of("output", 8L))));
  }

  @Test
  public void testSerializeWorkflows() throws IOException {
    File parent = new File("target/integration-tests");
    TemporaryFolder folder = new TemporaryFolder(parent);

    try {
      folder.create();

      CLIENT.serializeWorkflows(CLASSPATH, folder.getRoot().getAbsolutePath());

      boolean hasFibonacciWorkflow =
          Stream.of(folder.getRoot().list())
              .anyMatch(x -> x.endsWith("_org.flyte.examples.FibonacciWorkflow_2.pb"));

      assertThat(hasFibonacciWorkflow, equalTo(true));
    } finally {
      folder.delete();
    }
  }
}
