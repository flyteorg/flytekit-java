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
package org.flyte.flytekit;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.LaunchPlanRegistrar;
import org.flyte.api.v1.PartialWorkflowIdentifier;

/** A registrar that creates {@link LaunchPlan} instances. */
@AutoService(LaunchPlanRegistrar.class)
public class SdkLaunchPlanRegistrar extends LaunchPlanRegistrar {
  private static final Logger LOG = Logger.getLogger(SdkLaunchPlanRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  @Override
  public Map<LaunchPlanIdentifier, LaunchPlan> load(
      Map<String, String> env, ClassLoader classLoader) {
    ServiceLoader<SdkLaunchPlanRegistry> loader =
        ServiceLoader.load(SdkLaunchPlanRegistry.class, classLoader);

    LOG.fine("Discovering SdkLaunchPlans");

    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans = new HashMap<>();

    for (SdkLaunchPlanRegistry sdkLaunchPlanRegistry : loader) {
      load(launchPlans, env, sdkLaunchPlanRegistry);
    }

    return launchPlans;
  }

  private void load(
      Map<LaunchPlanIdentifier, LaunchPlan> currentPlans,
      Map<String, String> env,
      SdkLaunchPlanRegistry sdkLaunchPlanRegistry) {
    SdkConfig sdkConfig = SdkConfig.load(env);

    for (SdkLaunchPlan sdkLaunchPlan : sdkLaunchPlanRegistry.getLaunchPlans()) {
      String name = sdkLaunchPlan.getName();
      LaunchPlanIdentifier launchPlanId =
          LaunchPlanIdentifier.builder()
              .domain(sdkConfig.domain())
              .project(sdkConfig.project())
              .name(name)
              .version(sdkConfig.version())
              .build();
      LOG.fine(String.format("Discovered [%s]", name));

      LaunchPlan launchPlan =
          LaunchPlan.builder()
              .name(sdkLaunchPlan.getName())
              .workflowId(getWorkflowIdentifier(sdkConfig, sdkLaunchPlan))
              .fixedInputs(sdkLaunchPlan.getFixedInputs())
              .build();
      LaunchPlan previous = currentPlans.put(launchPlanId, launchPlan);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format("Discovered a duplicate launch plan [%s]", name));
      }
    }
  }

  private PartialWorkflowIdentifier getWorkflowIdentifier(
      SdkConfig sdkConfig, SdkLaunchPlan sdkLaunchPlan) {
    boolean isProjectSet = sdkLaunchPlan.getWorkflowProject() != null;

    return PartialWorkflowIdentifier.builder()
        .project((isProjectSet) ? sdkLaunchPlan.getWorkflowProject() : sdkConfig.project())
        .domain((isProjectSet) ? sdkLaunchPlan.getWorkflowDomain() : sdkConfig.domain())
        .name(sdkLaunchPlan.getWorkflowName())
        .version((isProjectSet) ? sdkLaunchPlan.getWorkflowVersion() : sdkConfig.version())
        .build();
  }
}
