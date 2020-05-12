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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.flyte.api.v1.*;
import org.flyte.jflyte.examples.FibonacciWorkflow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LocalRunnerTest {

  @Test
  void testFibonacci() {
    String workflowName = new FibonacciWorkflow().getName();

    Map<String, Literal> outputs = LocalRunner.executeWorkflow(workflowName);

    // FIXME flytekit-java doesn't yet support outputs
    Assertions.assertEquals(ImmutableMap.of(), outputs);
  }
}
