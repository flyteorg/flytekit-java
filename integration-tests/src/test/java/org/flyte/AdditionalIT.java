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

import static org.flyte.FlyteContainer.CLIENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.core.Literals;
import org.flyte.utils.Literal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdditionalIT {
  @BeforeAll
  public static void beforeAll() {
    CLIENT.registerWorkflows("integration-tests/target/lib");
  }

  @ParameterizedTest
  @CsvSource({
    "0,0,0,0,a == b && c == d",
    "0,1,0,0,a < b && c == d",
    "1,0,0,0,a > b && c == d",
    "0,0,1,0,a == b && c > d",
    "0,1,1,0,a < b && c > d",
    "1,0,1,0,a > b && c > d",
    "0,0,0,1,a == b && c < d",
    "0,1,0,1,a < b && c < d",
    "1,0,0,1,a > b && c < d",
  })
  public void testBranchNodeWorkflow(long a, long b, long c, long d, String expected) {
    Literals.LiteralMap output =
        CLIENT.createExecution(
            "org.flyte.integrationtests.BranchNodeWorkflow",
            Literal.ofIntegerMap(
                ImmutableMap.of(
                    "a", a,
                    "b", b,
                    "c", c,
                    "d", d)));

    assertThat(output, equalTo(Literal.ofStringMap(ImmutableMap.of("value", expected))));
  }

  @ParameterizedTest
  @CsvSource({
    "table-exists,true",
    "non-existent,false",
  })
  @Disabled("Not supporting struct with the strongly typed implementation.")
  public void testStructs(String name, boolean expected) {
    Literals.LiteralMap output =
        CLIENT.createExecution(
            "org.flyte.integrationtests.structs.MockPipelineWorkflow",
            Literal.ofStringMap(ImmutableMap.of("tableName", name)));

    assertThat(output, equalTo(Literal.ofBooleanMap(ImmutableMap.of("exists", expected))));
  }
}
