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
package org.flyte.localengine.examples;

import static org.flyte.flytekit.SdkBindingData.ofBoolean;
import static org.flyte.flytekit.SdkBindingData.ofInteger;
import static org.flyte.flytekit.SdkConditions.eq;
import static org.flyte.flytekit.SdkConditions.when;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

// 3X+1
@AutoService(SdkWorkflow.class)
public class CollatzConjectureStepWorkflow extends SdkWorkflow {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData x = builder.inputOfInteger("x");
    SdkBindingData isOdd =
        builder.apply("is_odd", new IsEvenTask().withInput("x", x)).getOutput("res");

    SdkBindingData nextX =
        builder
            .apply(
                "decide",
                when(
                        "was_even",
                        eq(ofBoolean(true), isOdd),
                        new Divide().withInput("num", x).withInput("den", ofInteger(2L)))
                    .otherwise("was_odd", new ThreeXPlusOne().withInput("x", x)))
            .getOutput("res");

    builder.output("nextX", nextX);
  }
}
