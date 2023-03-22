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
package org.flyte.flytekit.testing;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.jackson.JacksonSdkType;
import org.junit.jupiter.api.Test;

class TestingRunnableNodeTest {

  @Test
  void testRun_withStubValues() {
    Map<Input, Output> fixedOutputs = singletonMap(Input.create("7"), Output.create(7L));
    TestNode node = new TestNode(null, fixedOutputs);

    Map<String, Literal> output = node.run(singletonMap("in", Literals.ofString("7")));

    assertThat(output, hasEntry("out", Literals.ofInteger(7L)));
  }

  @Test
  void testRun_withFunction() {
    Function<Input, Output> fn = in -> Output.create(Long.parseLong(in.in().get()));
    TestNode node = new TestNode(fn, emptyMap());

    Map<String, Literal> output = node.run(singletonMap("in", Literals.ofString("7")));

    assertThat(output, hasEntry("out", Literals.ofInteger(7L)));
  }

  @Test
  void testRun_preferStubValuesOverFunction() {
    Map<Input, Output> fixedOutputs =
        singletonMap(Input.create("meaning of life"), Output.create(42L));
    Function<Input, Output> fn = in -> Output.create(Long.parseLong(in.in().get()));
    TestNode node = new TestNode(fn, fixedOutputs);

    Map<String, Literal> output =
        node.run(singletonMap("in", Literals.ofString("meaning of life")));

    assertThat(output, hasEntry("out", Literals.ofInteger(42L)));
  }

  @Test
  void testRun_notFound() {
    Map<Input, Output> fixedOutputs = singletonMap(Input.create("7"), Output.create(7L));
    TestNode node = new TestNode(null, fixedOutputs);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> node.run(singletonMap("in", Literals.ofString("not in fixed outputs"))));

    assertThat(
        ex.getMessage(),
        equalTo(
            "Can't find input Input{in=SdkBindingData{type=strings, value=not in fixed outputs}} for remote test [TestTask] "
                + "across known test inputs, use a magic wang to provide a test double"));
  }

  @Test
  void testWithMismatchingInput() {
    Function<Input, Output> fn = in -> {
      throw new AssertionError("should not happen");
    };
    TestNode node =
        new TestNode(null, emptyMap())
            .withRunFn(fn)
            .withFixedOutput(Input.create("8"), Output.create(7L));

    assertThrows(IllegalArgumentException.class,
        () -> node.run(singletonMap("in", Literals.ofString("7"))));
  }

  @Test
  void testWithFixedOutput() {
    TestNode node =
        new TestNode(null, emptyMap()).withFixedOutput(Input.create("7"), Output.create(7L));

    Map<String, Literal> output = node.run(singletonMap("in", Literals.ofString("7")));

    assertThat(output, hasEntry("out", Literals.ofInteger(7L)));
  }

  @Test
  void testWithRunFn() {
    Function<Input, Output> fn = in -> Output.create(Long.parseLong(in.in().get()));
    TestNode node = new TestNode(null, emptyMap()).withRunFn(fn);

    Map<String, Literal> output = node.run(singletonMap("in", Literals.ofString("7")));

    assertThat(output, hasEntry("out", Literals.ofInteger(7L)));
  }

  @Test
  void testGetNameShouldDeriveFromId() {
    TestNode node = new TestNode(null, emptyMap());

    assertThat(node.getName(), equalTo("TestTask"));
  }

  static class TestNode
      extends TestingRunnableNode<PartialTaskIdentifier, Input, Output, TestNode> {

    protected TestNode(Function<Input, Output> runFn, Map<Input, Output> fixedOutputs) {
      super(
          PartialTaskIdentifier.builder().name("TestTask").build(),
          JacksonSdkType.of(Input.class),
          JacksonSdkType.of(Output.class),
          runFn,
          fixedOutputs,
          (id, inType, outType, f, m) -> new TestNode(f, m),
          "test",
          "a magic wang");
    }
  }

  @AutoValue
  abstract static class Input {
    abstract SdkBindingData<String> in();

    public static Input create(String in) {
      return new AutoValue_TestingRunnableNodeTest_Input(SdkBindingDataFactory.of(in));
    }
  }

  @AutoValue
  abstract static class Output {
    abstract SdkBindingData<Long> out();

    public static Output create(Long out) {
      return new AutoValue_TestingRunnableNodeTest_Output(SdkBindingDataFactory.of(out));
    }
  }
}
