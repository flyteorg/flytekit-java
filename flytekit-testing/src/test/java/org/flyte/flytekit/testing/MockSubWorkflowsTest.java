/*
 * Copyright 2024 Flyte Authors.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;
import org.junit.jupiter.api.Test;

public class MockSubWorkflowsTest {
  @Test
  public void test() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new Workflow())
            .withFixedInputs(
                JacksonSdkType.of(SubWorkflowInputs.class),
                SubWorkflowInputs.create(SdkBindingDataFactory.of(1)))
            .withWorkflowOutput(
                new SubWorkflow(),
                JacksonSdkType.of(SubWorkflowInputs.class),
                SubWorkflowInputs.create(SdkBindingDataFactory.of(1)),
                JacksonSdkType.of(SubWorkflowOutputs.class),
                SubWorkflowOutputs.create(SdkBindingDataFactory.of(10)))
            .withWorkflowOutput(
                new SubWorkflow(),
                JacksonSdkType.of(SubWorkflowInputs.class),
                SubWorkflowInputs.create(SdkBindingDataFactory.of(2)),
                JacksonSdkType.of(SubWorkflowOutputs.class),
                SubWorkflowOutputs.create(SdkBindingDataFactory.of(20)))
            .execute();

    assertThat(result.getIntegerOutput("o"), equalTo(10L));
  }

  public static class Workflow extends SdkWorkflow<SubWorkflowInputs, SubWorkflowOutputs> {
    public Workflow() {
      super(
          JacksonSdkType.of(SubWorkflowInputs.class), JacksonSdkType.of(SubWorkflowOutputs.class));
    }

    @Override
    public SubWorkflowOutputs expand(SdkWorkflowBuilder builder, SubWorkflowInputs inputs) {

      var subOut1 =
          builder
              .apply(
                  "sub", new SubWorkflow(), SubWorkflowInputs.create(SdkBindingDataFactory.of(1)))
              .getOutputs();
      var subOut2 =
          builder
              .apply(
                  "sub1", new SubWorkflow(), SubWorkflowInputs.create(SdkBindingDataFactory.of(2)))
              .getOutputs();

      return SubWorkflowOutputs.create(subOut1.o());
    }
  }

  public static class SubWorkflow extends SdkWorkflow<SubWorkflowInputs, SubWorkflowOutputs> {
    public SubWorkflow() {
      super(
          JacksonSdkType.of(SubWorkflowInputs.class), JacksonSdkType.of(SubWorkflowOutputs.class));
    }

    @Override
    public SubWorkflowOutputs expand(SdkWorkflowBuilder builder, SubWorkflowInputs inputs) {

      return SubWorkflowOutputs.create(inputs.a());
    }
  }

  @AutoValue
  public abstract static class SubWorkflowInputs {
    public abstract SdkBindingData<Long> a();

    public static MockSubWorkflowsTest.SubWorkflowInputs create(SdkBindingData<Long> a) {
      return new AutoValue_MockSubWorkflowsTest_SubWorkflowInputs(a);
    }
  }

  @AutoValue
  public abstract static class SubWorkflowOutputs {
    public abstract SdkBindingData<Long> o();

    public static MockSubWorkflowsTest.SubWorkflowOutputs create(SdkBindingData<Long> o) {
      return new AutoValue_MockSubWorkflowsTest_SubWorkflowOutputs(o);
    }
  }
}
