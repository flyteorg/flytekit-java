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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class ExecuteLocalTest {

  @Test
  public void testParseInputs_string() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("string", variableOf(SimpleType.STRING)),
            new String[] {"--string=string_value"});

    assertEquals(ImmutableMap.of("string", literalOf(Primitive.ofString("string_value"))), inputs);
  }

  @Test
  public void testParseInputs_integer() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("integer", variableOf(SimpleType.INTEGER)),
            new String[] {"--integer=42"});

    assertEquals(ImmutableMap.of("integer", literalOf(Primitive.ofInteger(42))), inputs);
  }

  @Test
  public void testParseInputs_float() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("float", variableOf(SimpleType.FLOAT)),
            new String[] {"--float=42.4"});

    double inputFloat = inputs.get("float").scalar().primitive().float_();

    assertEquals(42.4, inputFloat, 0.00001);
  }

  @Test
  public void testParseInputs_dateTime_isoDate() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("datetime", variableOf(SimpleType.DATETIME)),
            new String[] {"--datetime=2020-02-01"});

    Instant expected = LocalDate.of(2020, 2, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    assertEquals(ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(expected))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_epochDate() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("datetime", variableOf(SimpleType.DATETIME)),
            new String[] {"--datetime=1970-01-01"});

    assertEquals(
        ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(Instant.EPOCH))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_epochZonedDateTime() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("datetime", variableOf(SimpleType.DATETIME)),
            new String[] {"--datetime=1970-01-01T01:00:00+01:00"});

    assertEquals(
        ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(Instant.EPOCH))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_epochZonedDate() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("datetime", variableOf(SimpleType.DATETIME)),
            new String[] {"--datetime=1970-01-02+01:00"});

    Instant expected = Instant.EPOCH.plus(Duration.ofHours(23));

    assertEquals(ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(expected))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_isoInstantUtc() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("datetime", variableOf(SimpleType.DATETIME)),
            new String[] {"--datetime=2020-02-01T03:04:05Z"});

    Instant expected = LocalDateTime.of(2020, 2, 1, 3, 4, 5).toInstant(ZoneOffset.UTC);

    assertEquals(ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(expected))), inputs);
  }

  @Test
  public void testParseInputs_boolean() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of(
                "true_arg", variableOf(SimpleType.BOOLEAN),
                "false_arg", variableOf(SimpleType.BOOLEAN)),
            new String[] {"--true_arg=true", "--false_arg=false"});

    assertEquals(
        ImmutableMap.of(
            "true_arg", literalOf(Primitive.ofBoolean(true)),
            "false_arg", literalOf(Primitive.ofBoolean(false))),
        inputs);
  }

  @Test
  public void testParseInputs_duration() {
    Map<String, Literal> inputs =
        ExecuteLocal.parseInputs(
            CommandLine.Model.CommandSpec.create(),
            ImmutableMap.of("duration", variableOf(SimpleType.DURATION)),
            new String[] {"--duration=P1DT2H"});

    assertEquals(
        ImmutableMap.of(
            "duration", literalOf(Primitive.ofDuration(Duration.ofDays(1).plusHours(2)))),
        inputs);
  }

  @Test
  public void testParseInputs_missingArgument() {
    CommandLine.ParameterException exception =
        Assertions.assertThrows(
            CommandLine.ParameterException.class,
            () ->
                ExecuteLocal.parseInputs(
                    CommandLine.Model.CommandSpec.create(),
                    ImmutableMap.of("arg", variableOf(SimpleType.STRING)),
                    new String[0]));

    assertEquals("Missing required option '--arg'", exception.getMessage());
  }

  @Test
  public void testParseInputs_invalidArgument() {
    CommandLine.ParameterException exception =
        Assertions.assertThrows(
            CommandLine.ParameterException.class,
            () ->
                ExecuteLocal.parseInputs(
                    CommandLine.Model.CommandSpec.create(),
                    ImmutableMap.of("arg", variableOf(SimpleType.INTEGER)),
                    new String[] {"--arg=string_value"}));

    assertEquals(
        "Invalid value for option '--arg': 'string_value' is not a long", exception.getMessage());
  }

  private static Literal literalOf(Primitive primitive) {
    return Literal.of(Scalar.of(primitive));
  }

  private static Variable variableOf(SimpleType simpleType) {
    return Variable.builder()
        .description("description")
        .literalType(LiteralType.builder().simpleType(simpleType).build())
        .build();
  }
}
