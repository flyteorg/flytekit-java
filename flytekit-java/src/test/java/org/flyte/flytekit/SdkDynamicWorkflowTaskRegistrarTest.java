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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

import com.google.errorprone.annotations.Var;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;
import org.junit.jupiter.api.Test;

class SdkDynamicWorkflowTaskRegistrarTest {

  private static final Power2 POWER_2 = new Power2();

  @Test
  void shouldLoad() {
    Map<TaskIdentifier, DynamicWorkflowTask> loadedDynWf =
        new SdkDynamicWorkflowTaskRegistrar()
            .load(
                Map.of(
                    SdkConfig.DOMAIN_ENV_VAR,
                    "dev",
                    SdkConfig.PROJECT_ENV_VAR,
                    "tests",
                    SdkConfig.VERSION_ENV_VAR,
                    "1"),
                List.of(POWER_2));

    assertThat(loadedDynWf.size(), equalTo(1));

    var entry = loadedDynWf.entrySet().stream().iterator().next();
    var identifier = entry.getKey();
    assertThat(identifier.name(), equalTo(POWER_2.getName()));
    assertThat(identifier.domain(), equalTo("dev"));
    assertThat(identifier.version(), equalTo("1"));

    var dynWf = entry.getValue();
    assertThat(dynWf.getName(), equalTo(POWER_2.getName()));
    assertThat(
        dynWf.getInterface(),
        equalTo(
            TypedInterface.builder()
                .inputs(SdkLiteralTypes.integers().asSdkType("n").getVariableMap())
                .outputs(SdkLiteralTypes.integers().asSdkType("2n").getVariableMap())
                .build()));
    var spec =
        dynWf.run(Map.of("n", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(3)))));
    assertThat(spec.nodes(), hasSize(3));
    assertThat(
        spec.outputs(),
        hasItems(
            Binding.builder()
                .var_("2n")
                .binding(
                    BindingData.ofOutputReference(
                        OutputReference.builder().var("fx").nodeId("mult-2").build()))
                .build()));
  }

  public static class Power2
      extends SdkDynamicWorkflowTask<SdkBindingData<Long>, SdkBindingData<Long>> {
    public Power2() {
      super(SdkLiteralTypes.integers().asSdkType("n"), SdkLiteralTypes.integers().asSdkType("2n"));
    }

    @Override
    public SdkBindingData<Long> run(SdkWorkflowBuilder builder, SdkBindingData<Long> input) {
      Long n = input.get();
      SdkBindingData<Long> unit = SdkBindingDataFactory.of(1L);
      if (n == 0) {
        return unit;
      }
      @Var var x = unit;
      for (int i = 0; i < n; i++) {
        x = builder.apply("mult-" + i, new Mult2(), x).getOutputs();
      }
      return x;
    }
  }

  static class Mult2 extends SdkRunnableTask<SdkBindingData<Long>, SdkBindingData<Long>> {
    private static final long serialVersionUID = -3913746037079733005L;

    public Mult2() {
      super(SdkLiteralTypes.integers().asSdkType("x"), SdkLiteralTypes.integers().asSdkType("fx"));
    }

    @Override
    public SdkBindingData<Long> run(SdkBindingData<Long> input) {
      return SdkBindingDataFactory.of(input.get() * 2);
    }
  }
}
