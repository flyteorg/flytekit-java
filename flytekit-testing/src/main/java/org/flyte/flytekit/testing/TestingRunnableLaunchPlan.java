/*
 * Copyright 2022-2023 Flyte Authors
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

import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.flytekit.SdkType;
import org.flyte.localengine.RunnableLaunchPlan;

/** {@link RunnableLaunchPlan} that can fix output for specific input. */
public class TestingRunnableLaunchPlan<InputT, OutputT>
    extends TestingRunnableNode<
        PartialLaunchPlanIdentifier, InputT, OutputT, TestingRunnableLaunchPlan<InputT, OutputT>>
    implements RunnableLaunchPlan {

  TestingRunnableLaunchPlan(
      PartialLaunchPlanIdentifier launchPlanId,
      SdkType<InputT> inputType,
      SdkType<OutputT> outputType,
      Function<InputT, OutputT> runFn,
      boolean runFnProvided,
      Map<InputT, OutputT> fixedOutputs) {
    super(
        launchPlanId,
        inputType,
        outputType,
        runFn,
        runFnProvided,
        fixedOutputs,
        TestingRunnableLaunchPlan::new,
        "launch plan",
        "SdkTestingExecutor#withLaunchPlanOutput or SdkTestingExecutor#withLaunchPlan");
  }

  static <InputT, OutputT> TestingRunnableLaunchPlan<InputT, OutputT> create(
      String name, SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    PartialLaunchPlanIdentifier launchPlanId =
        PartialLaunchPlanIdentifier.builder().name(name).build();

    return new TestingRunnableLaunchPlan<>(
        launchPlanId, inputType, outputType, null, false, emptyMap());
  }
}
