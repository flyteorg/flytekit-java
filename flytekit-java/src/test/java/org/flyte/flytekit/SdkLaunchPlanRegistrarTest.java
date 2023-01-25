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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.flyte.flytekit.SdkConfig.DOMAIN_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.PROJECT_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.VERSION_ENV_VAR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auto.service.AutoService;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.CronSchedule;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Parameter;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;

class SdkLaunchPlanRegistrarTest {

  private static final Map<String, String> ENV =
      Map.of(PROJECT_ENV_VAR, "project", DOMAIN_ENV_VAR, "domain", VERSION_ENV_VAR, "version");

  private final SdkLaunchPlanRegistrar registrar = new SdkLaunchPlanRegistrar();

  @Test
  void shouldLoadLaunchPlansFromDiscoveredRegistries() {
    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans = registrar.load(ENV);
    Primitive defaultPrimitive = Primitive.ofStringValue("default-bar");
    LaunchPlanIdentifier expectedTestPlan =
        LaunchPlanIdentifier.builder()
            .project("project")
            .domain("domain")
            .name("TestPlan")
            .version("version")
            .build();
    LaunchPlan expectedPlan =
        LaunchPlan.builder()
            .name("TestPlan")
            .workflowId(
                PartialWorkflowIdentifier.builder()
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .build())
            .fixedInputs(
                singletonMap(
                    "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("bar")))))
            .defaultInputs(
                singletonMap(
                    "default-foo",
                    Parameter.create(
                        Variable.builder()
                            .description("")
                            .literalType(LiteralType.ofSimpleType(SimpleType.STRING))
                            .build(),
                        Literal.ofScalar(Scalar.ofPrimitive(defaultPrimitive)))))
            .build();
    LaunchPlanIdentifier expectedOtherTestPlan =
        LaunchPlanIdentifier.builder()
            .project("project")
            .domain("domain")
            .name("OtherTestPlan")
            .version("version")
            .build();
    LaunchPlan expectedOtherPlan =
        LaunchPlan.builder()
            .name("OtherTestPlan")
            .workflowId(
                PartialWorkflowIdentifier.builder()
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .build())
            .fixedInputs(
                singletonMap(
                    "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("baz")))))
            .defaultInputs(
                singletonMap(
                    "default-foo",
                    Parameter.create(
                        Variable.builder()
                            .description("")
                            .literalType(LiteralType.ofSimpleType(SimpleType.STRING))
                            .build(),
                        Literal.ofScalar(Scalar.ofPrimitive(defaultPrimitive)))))
            .build();

    assertAll(
        () -> assertThat(launchPlans, hasEntry(is(expectedTestPlan), is(expectedPlan))),
        () -> assertThat(launchPlans, hasEntry(is(expectedOtherTestPlan), is(expectedOtherPlan))));
  }

  @Test
  void shouldTestLaunchPlansWithCronSchedule() {
    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans =
        registrar.load(ENV, singletonList(new TestRegistryWithSchedules()));

    LaunchPlanIdentifier expectedIdentifierWithoutOffset =
        LaunchPlanIdentifier.builder()
            .project("project")
            .domain("domain")
            .name("TestPlanScheduleWithoutOffset")
            .version("version")
            .build();
    LaunchPlan planWithoutOffset =
        LaunchPlan.builder()
            .name("TestPlanScheduleWithoutOffset")
            .workflowId(
                PartialWorkflowIdentifier.builder()
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .build())
            .fixedInputs(Collections.emptyMap())
            .defaultInputs(Collections.emptyMap())
            .cronSchedule(CronSchedule.builder().schedule("daily").build())
            .build();

    LaunchPlanIdentifier expectedIdentifierWithOffset =
        LaunchPlanIdentifier.builder()
            .project("project")
            .domain("domain")
            .name("TestPlanScheduleWithOffset")
            .version("version")
            .build();
    LaunchPlan planWithOffset =
        LaunchPlan.builder()
            .name("TestPlanScheduleWithOffset")
            .workflowId(
                PartialWorkflowIdentifier.builder()
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .build())
            .fixedInputs(Collections.emptyMap())
            .defaultInputs(Collections.emptyMap())
            .cronSchedule(CronSchedule.builder().schedule("daily").offset("PT1H").build())
            .build();

    assertThat(
        launchPlans,
        allOf(
            hasEntry(expectedIdentifierWithoutOffset, planWithoutOffset),
            hasEntry(expectedIdentifierWithOffset, planWithOffset)));
  }

  @Test
  void shouldRejectLoadingLaunchPlanDuplicatesInSameRegistry() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> registrar.load(ENV, singletonList(new TestRegistryWithDuplicates())));

    assertThat(
        exception.getMessage(), equalTo("Discovered a duplicate launch plan [DuplicatedPlan]"));
  }

  @Test
  void shouldRejectLoadingLaunchPlanDuplicatesAcrossRegistries() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> registrar.load(ENV, asList(new TestRegistry(), new DuplicationOfTestRegistry())));

    assertThat(exception.getMessage(), equalTo("Discovered a duplicate launch plan [TestPlan]"));
  }

  @AutoService(SdkLaunchPlanRegistry.class)
  public static class TestRegistry implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return singletonList(
          SdkLaunchPlan.of(new TestWorkflow())
              .withName("TestPlan")
              .withFixedInput("foo", "bar")
              .withDefaultInput("default-foo", "default-bar"));
    }
  }

  @AutoService(SdkLaunchPlanRegistry.class)
  public static class OtherTestRegistry implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return singletonList(
          SdkLaunchPlan.of(new TestWorkflow())
              .withName("OtherTestPlan")
              .withFixedInput("foo", "baz")
              .withDefaultInput("default-foo", "default-bar"));
    }
  }

  public static class TestRegistryWithDuplicates implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return asList(
          SdkLaunchPlan.of(new TestWorkflow()).withName("DuplicatedPlan"),
          SdkLaunchPlan.of(new OtherTestWorkflow()).withName("DuplicatedPlan"));
    }
  }

  public static class DuplicationOfTestRegistry implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return singletonList(SdkLaunchPlan.of(new OtherTestWorkflow()).withName("TestPlan"));
    }
  }

  public static class TestRegistryWithSchedules implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return Arrays.asList(
          SdkLaunchPlan.of(new TestWorkflow())
              .withName("TestPlanScheduleWithoutOffset")
              .withCronSchedule(SdkCronSchedule.of("daily")),
          SdkLaunchPlan.of(new TestWorkflow())
              .withName("TestPlanScheduleWithOffset")
              .withCronSchedule(SdkCronSchedule.of("daily", Duration.ofHours(1))));
    }
  }

  public static class TestWorkflow extends SdkWorkflow<Void, Void> {

    public TestWorkflow() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      builder.inputOfString("foo");
      builder.inputOfString("default-foo");
    }
  }

  public static class OtherTestWorkflow extends SdkWorkflow<Void, Void> {

    public OtherTestWorkflow() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      // Do nothing
    }
  }
}
