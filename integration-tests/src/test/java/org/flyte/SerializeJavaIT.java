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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.util.stream.Stream;
import org.flyte.utils.FlyteSandboxClient;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SerializeJavaIT {
  private static final FlyteSandboxClient CLIENT = FlyteSandboxClient.create();
  private static final String CLASSPATH = "flytekit-examples/target/lib";
  @TempDir File tempDir;

  @Test
  @Disabled
  public void testSerializeWorkflows() {
    try {
      CLIENT.serializeWorkflows(CLASSPATH, tempDir.getAbsolutePath());

      boolean hasFibonacciWorkflow =
          Stream.of(tempDir.list())
              .anyMatch(x -> x.endsWith("_org.flyte.examples.FibonacciWorkflow_2.pb"));

      assertThat(hasFibonacciWorkflow, equalTo(true));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
