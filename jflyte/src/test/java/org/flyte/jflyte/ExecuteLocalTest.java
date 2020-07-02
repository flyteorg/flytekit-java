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

import static java.util.Collections.emptyMap;
import static org.flyte.jflyte.ApiUtils.createVar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTypes;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class ExecuteLocalTest {

  @Test
  public void testParseInputs_string() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("string", createVar(SimpleType.STRING)),
            new String[] {"--string=string_value"});

    assertEquals(ImmutableMap.of("string", literalOf(Primitive.ofString("string_value"))), inputs);
  }

  @Test
  public void testParseInputs_integer() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("integer", createVar(SimpleType.INTEGER)),
            new String[] {"--integer=42"});

    assertEquals(ImmutableMap.of("integer", literalOf(Primitive.ofInteger(42))), inputs);
  }

  @Test
  public void testParseInputs_float() {
    Map<String, Literal> inputs =
        parseInputs(
            ImmutableMap.of("float", createVar(SimpleType.FLOAT)), new String[] {"--float=42.4"});

    double inputFloat = inputs.get("float").scalar().primitive().float_();

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
            "true_arg", literalOf(Primitive.ofBoolean(true)),
            "false_arg", literalOf(Primitive.ofBoolean(false))),
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

  @Test
  public void testParseInputs_defaultValues() {
    ExecuteLocal cmd = new ExecuteLocalWithDefaultValues(ImmutableMap.of("arg", "foo"));

    Map<String, Literal> inputs =
        parseInputs(ImmutableMap.of("arg", createVar(SimpleType.STRING)), new String[0], cmd);

    assertEquals(ImmutableMap.of("arg", literalOf(Primitive.ofString("foo"))), inputs);
  }

  @Test
  public void testParseInputs_missingArgument() {
    CommandLine.ParameterException exception =
        assertThrows(
            CommandLine.ParameterException.class,
            () -> parseInputs(ImmutableMap.of("arg", createVar(SimpleType.STRING)), new String[0]));

    assertEquals("Missing required option '--arg'", exception.getMessage());
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
  void testLoadAll_withoutDuplicates() {
    ClassLoader classLoader1 = new TestClassLoader();
    ClassLoader classLoader2 = new TestClassLoader();
    TestTask task1 = new TestTask();
    TestTask task2 = new TestTask();

    Map<String, ClassLoader> modules =
        ImmutableMap.of("source1", classLoader1, "source2", classLoader2);
    Map<ClassLoader, Map<String, SdkRunnableTask<?, ?>>> tasksPerClassLoader =
        ImmutableMap.of(
            classLoader1, ImmutableMap.of("task1", task1),
            classLoader2, ImmutableMap.of("task2", task2));

    Map<String, SdkRunnableTask<?, ?>> tasksByName =
        ExecuteLocal.loadAll(modules, (cl, __) -> tasksPerClassLoader.get(cl), emptyMap());

    assertEquals(ImmutableMap.of("task1", task1, "task2", task2), tasksByName);
  }

  @Test
  void testLoadAll_withDuplicates() {
    ClassLoader classLoader1 = new TestClassLoader();
    ClassLoader classLoader2 = new TestClassLoader();
    ClassLoader classLoader3 = new TestClassLoader();
    TestTask duplicateTask1 = new TestTask();
    TestTask duplicateTask2 = new TestTask();
    TestTask uniqueTask = new TestTask();

    Map<String, ClassLoader> modules =
        ImmutableMap.of("source1", classLoader1, "source2", classLoader2, "source3", classLoader3);
    Map<ClassLoader, Map<String, SdkRunnableTask<?, ?>>> tasksPerClassLoader =
        ImmutableMap.of(
            classLoader1, ImmutableMap.of("dupTask1", duplicateTask1, "unqTask", uniqueTask),
            classLoader2, ImmutableMap.of("dupTask1", duplicateTask1, "dupTask2", duplicateTask2),
            classLoader3, ImmutableMap.of("dupTask2", duplicateTask2));

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ExecuteLocal.loadAll(modules, (cl, __) -> tasksPerClassLoader.get(cl), emptyMap()));

    assertEquals(
        "Found duplicate items among the modules: "
            + "{dupTask1 -> [source1, source2]}; {dupTask2 -> [source2, source3]}",
        exception.getMessage());
  }

  private static Map<String, Literal> parseInputs(
      Map<String, Variable> variableMap, String[] args) {
    return parseInputs(variableMap, args, new ExecuteLocal());
  }

  private static Map<String, Literal> parseInputs(
      Map<String, Variable> variableMap, String[] args, ExecuteLocal cmd) {
    CommandLine.Model.CommandSpec spec = CommandLine.Model.CommandSpec.create();
    variableMap.forEach((name, variable) -> spec.addOption(cmd.getOption(name, variable)));

    return cmd.parseInputs(spec, variableMap, args);
  }

  private static Literal literalOf(Primitive primitive) {
    return Literal.of(Scalar.of(primitive));
  }

  private static class ExecuteLocalWithDefaultValues extends ExecuteLocal {

    private final ImmutableMap<String, String> defaultValues;

    public ExecuteLocalWithDefaultValues(ImmutableMap<String, String> defaultValues) {
      this.defaultValues = defaultValues;
    }

    @Nullable
    @Override
    protected String getDefaultValue(String name) {
      return defaultValues.get(name);
    }
  }

  private static class TestTask extends SdkRunnableTask<Void, Void> {
    private static final long serialVersionUID = -2949483398581210936L;

    private TestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public Void run(Void input) {
      return null;
    }
  }

  private static class TestClassLoader extends ClassLoader {}
}
