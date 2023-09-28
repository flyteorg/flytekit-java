/*
 * Copyright 2023 Flyte Authors.
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
package org.flyte.flytekit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.runtime.BoxedUnit;

@ExtendWith(MockitoExtension.class)
class SdkTransformTest {

  @Mock private SdkNode<Void> mockResponse;

  @Test
  void applyShouldPropagateCallToSubClasses() {
    var transform = Mockito.spy(new TransformWithInputs());
    var builder = new SdkWorkflowBuilder();
    var nodeId = "node";
    var upstreamNodeIds = List.of("upstream-node");
    var metadata = SdkNodeMetadata.builder().name("fancy-name").build();
    var in = SdkBindingDataFactory.of(1);
    var inputs = TestUnaryIntegerInput.create(in);
    var inputsBindings = Map.<String, SdkBindingData<?>>of("in", in);

    transform.apply(builder, nodeId, upstreamNodeIds, metadata, inputs);

    verify(transform).apply(builder, nodeId, upstreamNodeIds, metadata, inputsBindings);
  }

  @Test
  void applyShouldRejectCallsForNullInputsForTypesWithVariables() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new TransformWithInputs()
                    .apply(
                        new SdkWorkflowBuilder(),
                        "node",
                        List.of(),
                        null,
                        (TestUnaryIntegerInput) null));

    assertThat(
        exception.getMessage(),
        equalTo("Null supplied as input for a transform with variables: [in]"));
  }

  @ParameterizedTest
  @MethodSource("nullValues")
  void shouldAcceptNullValuesJavaAndScala(Object nullValue) {
    var spyTransform = Mockito.spy(new TransformWithoutInputs());
    var builder = new SdkWorkflowBuilder();
    var nodeId = "node";
    var upstreamNodeIds = List.of("upstream-node");
    var metadata = SdkNodeMetadata.builder().name("fancy-name").build();
    var inputsBindings = Map.<String, SdkBindingData<?>>of();

    spyTransform.apply(builder, nodeId, upstreamNodeIds, metadata, nullValue);

    verify(spyTransform).apply(builder, nodeId, upstreamNodeIds, metadata, inputsBindings);
  }

  public static Stream<Arguments> nullValues() {
    return Stream.of(null, Arguments.of(BoxedUnit.UNIT));
  }

  @Test
  void applyShouldRejectCallsForNonNullInputsForTypesWithoutVariables() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new TransformWithoutInputs()
                    .apply(new SdkWorkflowBuilder(), "node", List.of(), null, "not a null value"));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Null input expected for a transform with no variables, but was: not a null value"));
  }

  private class TransformWithInputs extends SdkTransform<TestUnaryIntegerInput, Void> {

    @Override
    public SdkType<TestUnaryIntegerInput> getInputType() {
      return new TestUnaryIntegerInput.SdkType();
    }

    @Override
    public SdkType<Void> getOutputType() {
      return SdkTypes.nulls();
    }

    @CanIgnoreReturnValue
    @Override
    SdkNode<Void> apply(
        SdkWorkflowBuilder builder,
        String nodeId,
        List<String> upstreamNodeIds,
        @Nullable SdkNodeMetadata metadata,
        Map<String, SdkBindingData<?>> inputs) {
      return mockResponse;
    }
  }

  private class TransformWithoutInputs extends SdkTransform<Object, Void> {

    @Override
    public SdkType<Object> getInputType() {
      return new CustomNoVariableType();
    }

    @Override
    public SdkType<Void> getOutputType() {
      return SdkTypes.nulls();
    }

    @CanIgnoreReturnValue
    @Override
    SdkNode<Void> apply(
        SdkWorkflowBuilder builder,
        String nodeId,
        List<String> upstreamNodeIds,
        @Nullable SdkNodeMetadata metadata,
        Map<String, SdkBindingData<?>> inputs) {
      return mockResponse;
    }
  }

  // No rational user would write a SdkType implementation like this, but it allows us to test
  // the corner case that types without variables should accept only null values
  private static class CustomNoVariableType extends SdkType<Object> {

    @Override
    public Map<String, Literal> toLiteralMap(Object value) {
      return Map.of();
    }

    @Override
    public Object fromLiteralMap(Map<String, Literal> value) {
      return null;
    }

    @Override
    public Object promiseFor(String nodeId) {
      return null;
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return Map.of();
    }

    @Override
    public Map<String, SdkLiteralType<?>> toLiteralTypes() {
      return Map.of();
    }

    @Override
    public Map<String, SdkBindingData<?>> toSdkBindingMap(Object value) {
      return Map.of();
    }
  }
}
