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

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.flyte.api.v1.CronSchedule;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.LaunchPlanRegistrar;
import org.flyte.api.v1.PartialWorkflowIdentifier;

/**
 * Default implementation of a {@link LaunchPlanRegistrar} that discovers {@link
 * SdkLaunchPlanRegistry}s implementation via {@link ServiceLoader} mechanism and then proceed to
 * load {@link SdkLaunchPlan} from them. Launch plans registry implementations must use
 * {@code @AutoService(SdkLaunchPlanRegistry.class)} or manually add their fully qualifies name to
 * the corresponding file.
 *
 * @see ServiceLoader
 */
@AutoService(LaunchPlanRegistrar.class)
public class SdkLaunchPlanRegistrar extends LaunchPlanRegistrar {
  private static final Logger LOG = Logger.getLogger(SdkLaunchPlanRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  /**
   * Load {@link SdkLaunchPlanRegistry}s using {@link ServiceLoader} and load {@link SdkLaunchPlan}
   * from them.
   *
   * @param env env vars in a map that would be used to pick up the project, domain and version for
   *     the discovered tasks.
   * @param classLoader class loader to use when discovering the task using {@link
   *     ServiceLoader#load(Class, ClassLoader)}
   * @return a map of {@link LaunchPlan}s by its task identifier.
   */
  @Override
  public Map<LaunchPlanIdentifier, LaunchPlan> load(
      Map<String, String> env, ClassLoader classLoader) {
    ServiceLoader<SdkLaunchPlanRegistry> loader =
        ServiceLoader.load(SdkLaunchPlanRegistry.class, classLoader);

    LOG.fine("Discovering SdkLaunchPlans");

    List<SdkLaunchPlanRegistry> discoveredRegistries = new ArrayList<>();
    loader.iterator().forEachRemaining(discoveredRegistries::add);

    return load(env, discoveredRegistries);
  }

  // VisibleForTesting
  Map<LaunchPlanIdentifier, LaunchPlan> load(
      Map<String, String> env, List<SdkLaunchPlanRegistry> registries) {
    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans = new HashMap<>();

    for (SdkLaunchPlanRegistry sdkLaunchPlanRegistry : registries) {
      SdkConfig sdkConfig = SdkConfig.load(env);

      for (SdkLaunchPlan sdkLaunchPlan : sdkLaunchPlanRegistry.getLaunchPlans()) {
        String name = sdkLaunchPlan.name();
        LaunchPlanIdentifier launchPlanId =
            LaunchPlanIdentifier.builder()
                .domain(sdkConfig.domain())
                .project(sdkConfig.project())
                .name(name)
                .version(sdkConfig.version())
                .build();
        LOG.fine(String.format("Discovered [%s]", name));

        LaunchPlan.Builder builder =
            LaunchPlan.builder()
                .name(sdkLaunchPlan.name())
                .workflowId(getWorkflowIdentifier(sdkLaunchPlan))
                .fixedInputs(sdkLaunchPlan.fixedInputs())
                .defaultInputs(sdkLaunchPlan.defaultInputs());

        if (sdkLaunchPlan.cronSchedule() != null) {
          builder.cronSchedule(getCronSchedule(sdkLaunchPlan.cronSchedule()));
        }

        LaunchPlan launchPlan = builder.build();
        LaunchPlan previous = launchPlans.put(launchPlanId, launchPlan);

        if (previous != null) {
          throw new IllegalArgumentException(
              String.format("Discovered a duplicate launch plan [%s]", name));
        }
      }
    }

    return launchPlans;
  }

  private CronSchedule getCronSchedule(SdkCronSchedule sdkCronSchedule) {
    CronSchedule.Builder scheduleBuilder =
        CronSchedule.builder().schedule(sdkCronSchedule.schedule());

    if (sdkCronSchedule.offset() != null) {
      scheduleBuilder.offset(sdkCronSchedule.offset().toString());
    }

    return scheduleBuilder.build();
  }

  private PartialWorkflowIdentifier getWorkflowIdentifier(SdkLaunchPlan sdkLaunchPlan) {
    return PartialWorkflowIdentifier.builder()
        .project(sdkLaunchPlan.workflowProject())
        .domain(sdkLaunchPlan.workflowDomain())
        .name(sdkLaunchPlan.workflowName())
        .version(sdkLaunchPlan.workflowVersion())
        .build();
  }
}
