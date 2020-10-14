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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SimpleSdkLaunchPlanTest {
  private static final SdkWorkflow WORKFLOW = new TestWorkflow();

  private static final SdkLaunchPlan LP = SdkLaunchPlan.of(WORKFLOW).withName("lp");
  private static final SdkLaunchPlan LP2 = SdkLaunchPlan.of(WORKFLOW).withName("lp2");

  @Test
  void shouldRegisterLaunchPlans() {
    TestSimpleSdkLaunchPlanRegistry registry = new TestSimpleSdkLaunchPlanRegistry();

    List<SdkLaunchPlan> launchPlans = registry.getLaunchPlans();

    assertThat(launchPlans, equalTo(Arrays.asList(LP, LP2)));
  }

  public static class TestSimpleSdkLaunchPlanRegistry extends SimpleSdkLaunchPlanRegistry {
    public TestSimpleSdkLaunchPlanRegistry() {
      registerLaunchPlan(LP);
      registerLaunchPlan(LP2);
    }
  }

  @Test
  void shouldRegisterDefaultLaunchPlans() {
    SdkLaunchPlan expectedDefaultLaunchPlan = SdkLaunchPlan.of(WORKFLOW);
    TestSimpleSdkLaunchPlanRegistryWithDefaults registry =
        new TestSimpleSdkLaunchPlanRegistryWithDefaults();

    List<SdkLaunchPlan> launchPlans = registry.getLaunchPlans();

    assertThat(launchPlans, contains(expectedDefaultLaunchPlan));
  }

  public static class TestSimpleSdkLaunchPlanRegistryWithDefaults
      extends SimpleSdkLaunchPlanRegistry {
    public TestSimpleSdkLaunchPlanRegistryWithDefaults() {
      // Real user's subclasses will use the overloaded version without params
      // using this version in test to skip the ServiceLoader discovery part
      registerDefaultLaunchPlans(singletonList(new TestWorkflow()));
    }
  }

  @Test
  void shouldRejectLaunchPlansWithDuplicateNames() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, TestSimpleSdkLaunchPlanRegistryWithDuplicateNames::new);

    assertThat(
        exception.getMessage(), equalTo("Registered duplicated launch plan: [duplicateName]"));
  }

  public static class TestSimpleSdkLaunchPlanRegistryWithDuplicateNames
      extends SimpleSdkLaunchPlanRegistry {
    public TestSimpleSdkLaunchPlanRegistryWithDuplicateNames() {
      registerLaunchPlan(SdkLaunchPlan.of(new TestWorkflow()).withName("duplicateName"));
      registerLaunchPlan(SdkLaunchPlan.of(new OtherTestWorkflow()).withName("duplicateName"));
    }
  }

  public static class TestWorkflow extends SdkWorkflow {
    @Override
    public void expand(SdkWorkflowBuilder builder) {
      // Do nothing
    }
  }

  private static class OtherTestWorkflow extends SdkWorkflow {
    @Override
    public void expand(SdkWorkflowBuilder builder) {
      // Do nothing
    }
  }
}
