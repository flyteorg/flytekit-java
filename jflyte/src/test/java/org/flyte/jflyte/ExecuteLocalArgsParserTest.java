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

import static org.flyte.jflyte.ApiUtils.createVar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SchemaType;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

public class ExecuteLocalArgsParserTest {
  @Test
  public void testParseInputs_string() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("string", createVar(SimpleType.STRING)),
            new String[] {"--string=string_value"});

    assertEquals(
        ImmutableMap.of("string", literalOf(Primitive.ofStringValue("string_value"))), inputs);
  }

  @Test
  public void testParseInputs_integer() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("integer", createVar(SimpleType.INTEGER)),
            new String[] {"--integer=42"});

    assertEquals(ImmutableMap.of("integer", literalOf(Primitive.ofIntegerValue(42))), inputs);
  }

  @Test
  public void testParseInputs_float() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("float", createVar(SimpleType.FLOAT)), new String[] {"--float=42.4"});

    double inputFloat = inputs.get("float").scalar().primitive().floatValue();

    assertEquals(42.4, inputFloat, 0.00001);
  }

  @Test
  public void testParseInputs_dateTime_isoDate() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("datetime", createVar(SimpleType.DATETIME)),
            new String[] {"--datetime=2020-02-01"});

    Instant expected = LocalDate.of(2020, 2, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    assertEquals(ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(expected))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_epochDate() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("datetime", createVar(SimpleType.DATETIME)),
            new String[] {"--datetime=1970-01-01"});

    assertEquals(
        ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(Instant.EPOCH))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_epochZonedDateTime() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("datetime", createVar(SimpleType.DATETIME)),
            new String[] {"--datetime=1970-01-01T01:00:00+01:00"});

    assertEquals(
        ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(Instant.EPOCH))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_epochZonedDate() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("datetime", createVar(SimpleType.DATETIME)),
            new String[] {"--datetime=1970-01-02+01:00"});

    Instant expected = Instant.EPOCH.plus(Duration.ofHours(23));

    assertEquals(ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(expected))), inputs);
  }

  @Test
  public void testParseInputs_dateTime_isoInstantUtc() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("datetime", createVar(SimpleType.DATETIME)),
            new String[] {"--datetime=2020-02-01T03:04:05Z"});

    Instant expected = LocalDateTime.of(2020, 2, 1, 3, 4, 5).toInstant(ZoneOffset.UTC);

    assertEquals(ImmutableMap.of("datetime", literalOf(Primitive.ofDatetime(expected))), inputs);
  }

  @Test
  public void testParseInputs_boolean() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of(
                "true_arg", createVar(SimpleType.BOOLEAN),
                "false_arg", createVar(SimpleType.BOOLEAN)),
            new String[] {"--true_arg=true", "--false_arg=false"});

    assertEquals(
        ImmutableMap.of(
            "true_arg", literalOf(Primitive.ofBooleanValue(true)),
            "false_arg", literalOf(Primitive.ofBooleanValue(false))),
        inputs);
  }

  @Test
  public void testParseInputs_duration() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("duration", createVar(SimpleType.DURATION)),
            new String[] {"--duration=P1DT2H"});

    assertEquals(
        ImmutableMap.of(
            "duration", literalOf(Primitive.ofDuration(Duration.ofDays(1).plusHours(2)))),
        inputs);
  }

  @ParameterizedTest
  @MethodSource("provideArgumentsFor_testParseInputs_unsupportedTypes")
  public void testParseInputs_unsupportedTypes(LiteralType unsupportedType) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                parseInputs(
                    ImmutableMap.of("unsupported", createVar(unsupportedType)),
                    new String[] {"--duration=foo"}));

    assertEquals(
        "Type of [unsupported] input parameter is not supported: " + unsupportedType,
        exception.getMessage());
  }

  static Stream<LiteralType> provideArgumentsFor_testParseInputs_unsupportedTypes() {
    LiteralType simpleType = LiteralType.ofSimpleType(SimpleType.INTEGER);
    return Stream.of(
        LiteralType.ofBlobType(
            BlobType.builder()
                .format("avro")
                .dimensionality(BlobType.BlobDimensionality.SINGLE)
                .build()),
        LiteralType.ofCollectionType(simpleType),
        LiteralType.ofMapValueType(simpleType),
        LiteralType.ofSchemaType(
            SchemaType.builder()
                .columns(
                    Collections.singletonList(
                        SchemaType.Column.builder()
                            .name("foo")
                            .type(SchemaType.ColumnType.INTEGER)
                            .build()))
                .build()));
  }

  @ParameterizedTest
  @MethodSource("provideArgumentsFor_testParseInputs_unsupportedTypes")
  public void testParseInputs_unsupported_defaultValues(LiteralType unsupportedType) {
    ExecuteLocalArgsParser parser = new ArgsParserWithDefaultValues(ImmutableMap.of("arg", "foo"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                parser.parseInputs(
                    "flyte",
                    ImmutableMap.of("arg", createVar(unsupportedType)),
                    ImmutableList.of()));

    assertEquals(
        "Type of [arg] input parameter is not supported: " + unsupportedType,
        exception.getMessage());
  }

  @Test
  public void testParseInputs_missingArgument() {
    CommandLine.ParameterException exception =
        assertThrows(
            CommandLine.ParameterException.class,
            () -> parseInputs(ImmutableMap.of("arg", createVar(SimpleType.STRING)), new String[0]));

    assertEquals("Missing required option: '--arg'", exception.getMessage());
  }

  @Test
  public void testParseInputs_invalidArgument() {
    CommandLine.ParameterException exception =
        assertThrows(
            CommandLine.ParameterException.class,
            () ->
                parseInputs(
                    ImmutableMap.of("arg", createVar(SimpleType.INTEGER)),
                    new String[] {"--arg=string_value"}));

    assertEquals(
        "Invalid value for option '--arg': 'string_value' is not a long", exception.getMessage());
  }

  @Test
  public void testParseInputs_defaultValues() {
    ArgsParserWithDefaultValues parser =
        new ArgsParserWithDefaultValues(
            ImmutableMap.<String, String>builder()
                .put("i", "1")
                .put("f", "2.0")
                .put("s", "3")
                .put("b", "true")
                .put("t", LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC).toString())
                .put("d", "P1D")
                .build());
    ImmutableMap<String, Literal> expected =
        ImmutableMap.<String, Literal>builder()
            .put("i", literalOf(Primitive.ofIntegerValue(1)))
            .put("f", literalOf(Primitive.ofFloatValue(2.0)))
            .put("s", literalOf(Primitive.ofStringValue("3")))
            .put("b", literalOf(Primitive.ofBooleanValue(true)))
            .put("t", literalOf(Primitive.ofDatetime(Instant.EPOCH)))
            .put("d", literalOf(Primitive.ofDuration(Duration.ofDays(1))))
            .build();

    Map<String, Literal> inputs =
        parser.parseInputs(
            "jflyte",
            ImmutableMap.<String, Variable>builder()
                .put("i", createVar(SimpleType.INTEGER))
                .put("f", createVar(SimpleType.FLOAT))
                .put("s", createVar(SimpleType.STRING))
                .put("b", createVar(SimpleType.BOOLEAN))
                .put("t", createVar(SimpleType.DATETIME))
                .put("d", createVar(SimpleType.DURATION))
                .build(),
            ImmutableList.of());

    assertEquals(expected, inputs);
  }

  private static class ArgsParserWithDefaultValues extends ExecuteLocalArgsParser {

    private final ImmutableMap<String, String> defaultValues;

    public ArgsParserWithDefaultValues(ImmutableMap<String, String> defaultValues) {
      this.defaultValues = defaultValues;
    }

    @Nullable
    @Override
    protected String getDefaultValue(String name) {
      return defaultValues.get(name);
    }
  }

  private static Literal literalOf(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  private static Map<String, Literal> parseInputs(
      Map<String, Variable> variableMap, String[] args) {
    ExecuteLocalArgsParser parser = new ExecuteLocalArgsParser();

    return parser.parseInputs("jflyte", variableMap, Arrays.asList(args));
  }
}
