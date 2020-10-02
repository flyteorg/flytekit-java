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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.flyte.flytekit.SdkConfig.DOMAIN_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.PROJECT_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.VERSION_ENV_VAR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.junit.jupiter.api.Test;

class SdkLaunchPlanRegistrarTest {

  private static final Map<String, String> ENV;

  static {
    HashMap<String, String> env = new HashMap<>();
    env.put(PROJECT_ENV_VAR, "project");
    env.put(DOMAIN_ENV_VAR, "domain");
    env.put(VERSION_ENV_VAR, "version");
    ENV = Collections.unmodifiableMap(env);
  }

  private final SdkLaunchPlanRegistrar registrar = new SdkLaunchPlanRegistrar();

  @Test
  void shouldLoadLaunchPlansFromDiscoveredRegistry() {
    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans = registrar.load(ENV);

    LaunchPlanIdentifier expectedIdentifier =
        LaunchPlanIdentifier.builder()
            .project("project")
            .domain("domain")
            .name("TestPlan")
            .version("version")
            .build();
    LaunchPlan plan =
        LaunchPlan.builder()
            .name("TestPlan")
            .workflowId(
                PartialWorkflowIdentifier.builder()
                    .project("project")
                    .domain("domain")
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .version("version")
                    .build())
            .fixedInputs(
                singletonMap(
                    "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("bar")))))
            .build();
    assertThat(launchPlans, hasEntry(expectedIdentifier, plan));
  }

  @Test
  void shouldRejectLoadingLaunchPlanDuplicatesInSameRegistry() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> registrar.load(new HashMap<>(), ENV, new TestRegistryWithDuplicates()));

    assertThat(
        exception.getMessage(), equalTo("Discovered a duplicate launch plan [DuplicatedPlan]"));
  }

  @Test
  void shouldRejectLoadingLaunchPlanDuplicatesAcrossRegistries() {
    LaunchPlanIdentifier identifier =
        LaunchPlanIdentifier.builder()
            .project("project")
            .domain("domain")
            .name("TestPlan")
            .version("version")
            .build();
    LaunchPlan plan =
        LaunchPlan.builder()
            .name("TestPlan")
            .workflowId(
                PartialWorkflowIdentifier.builder()
                    .project("project")
                    .domain("domain")
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .version("version")
                    .build())
            .fixedInputs(Collections.emptyMap())
            .build();
    HashMap<LaunchPlanIdentifier, LaunchPlan> currentPlans = new HashMap<>();
    currentPlans.put(identifier, plan);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> registrar.load(currentPlans, ENV, new TestRegistry()));

    assertThat(exception.getMessage(), equalTo("Discovered a duplicate launch plan [TestPlan]"));
  }

  @AutoService(SdkLaunchPlanRegistry.class)
  public static class TestRegistry implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return singletonList(
          SdkLaunchPlan.of(new TestWorkflow()).withName("TestPlan").withFixedInput("foo", "bar"));
    }
  }

  public static class TestRegistryWithDuplicates implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return Arrays.asList(
          SdkLaunchPlan.of(new TestWorkflow()).withName("DuplicatedPlan"),
          SdkLaunchPlan.of(new TestWorkflow()).withName("DuplicatedPlan"));
    }
  }

  public static class TestWorkflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      // Do nothing
    }
  }
}
