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
package org.flyte.flytekit;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of {@link SdkLaunchPlanRegistry} with handy methods to register {@link
 * SdkLaunchPlan}s.
 */
public abstract class SimpleSdkLaunchPlanRegistry implements SdkLaunchPlanRegistry {

  private final Map<String, SdkLaunchPlan> launchPlans = new LinkedHashMap<>();

  /**
   * Register the given launch plan.
   *
   * @param launchPlan the given launch plan.
   */
  protected void registerLaunchPlan(SdkLaunchPlan launchPlan) {
    String name = launchPlan.name();
    if (launchPlans.containsKey(name)) {
      throw new IllegalArgumentException(
          String.format("Registered duplicated launch plan: [%s]", name));
    }
    launchPlans.put(name, launchPlan);
  }

  /**
   * Register default launch plans for discovered workflows. A default launch plan is the one with
   * the same name as the workflow and set no fixed or default inputs.
   */
  protected void registerDefaultLaunchPlans() {
    List<SdkWorkflow<?, ?>> workflows = SdkWorkflowRegistry.loadAll();

    registerDefaultLaunchPlans(workflows);
  }

  // Visible for testing
  void registerDefaultLaunchPlans(List<SdkWorkflow<?, ?>> workflows) {
    for (SdkWorkflow<?, ?> sdkWorkflow : workflows) {
      SdkLaunchPlan defaultLaunchPlan = SdkLaunchPlan.of(sdkWorkflow);
      registerLaunchPlan(defaultLaunchPlan);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<SdkLaunchPlan> getLaunchPlans() {
    return List.copyOf(launchPlans.values());
  }
}
