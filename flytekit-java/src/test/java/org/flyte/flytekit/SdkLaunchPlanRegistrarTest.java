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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SdkLaunchPlanRegistrarTest {

  private static final Map<String, String> ENV;
  private DynamicURLClassLoader classLoader;

  static {
    HashMap<String, String> env = new HashMap<>();
    env.put(PROJECT_ENV_VAR, "project");
    env.put(DOMAIN_ENV_VAR, "domain");
    env.put(VERSION_ENV_VAR, "version");
    ENV = Collections.unmodifiableMap(env);
  }

  private final SdkLaunchPlanRegistrar registrar = new SdkLaunchPlanRegistrar();

  @BeforeEach
  void setUp() {
    this.classLoader =
        new DynamicURLClassLoader(
            (URLClassLoader) SdkLaunchPlanRegistrarTest.class.getClassLoader());
  }

  @Test
  void shouldLoadLaunchPlansFromDiscoveredRegistries(@TempDir Path tempDir) throws Exception {
    writeRegistryToMetaInfServices(tempDir, TestRegistry.class, OtherTestRegistry.class);
    Map<LaunchPlanIdentifier, LaunchPlan> launchPlans = registrar.load(ENV, classLoader);

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
                    .project("project")
                    .domain("domain")
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .version("version")
                    .build())
            .fixedInputs(
                singletonMap(
                    "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("bar")))))
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
                    .project("project")
                    .domain("domain")
                    .name("org.flyte.flytekit.SdkLaunchPlanRegistrarTest$TestWorkflow")
                    .version("version")
                    .build())
            .fixedInputs(
                singletonMap(
                    "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("baz")))))
            .build();

    assertAll(
        () -> assertThat(launchPlans, hasEntry(is(expectedTestPlan), is(expectedPlan))),
        () -> assertThat(launchPlans, hasEntry(is(expectedOtherTestPlan), is(expectedOtherPlan))));
  }

  @Test
  void shouldRejectLoadingLaunchPlanDuplicatesInSameRegistry(@TempDir Path tempDir)
      throws Exception {
    writeRegistryToMetaInfServices(tempDir, TestRegistryWithDuplicates.class);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> registrar.load(ENV, classLoader));

    assertThat(
        exception.getMessage(), equalTo("Discovered a duplicate launch plan [DuplicatedPlan]"));
  }

  @Test
  void shouldRejectLoadingLaunchPlanDuplicatesAcrossRegistries(@TempDir Path tempDir)
      throws Exception {
    writeRegistryToMetaInfServices(tempDir, TestRegistry.class, DuplicationOfTestRegistry.class);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> registrar.load(ENV, classLoader));

    assertThat(exception.getMessage(), equalTo("Discovered a duplicate launch plan [TestPlan]"));
  }

  @SafeVarargs
  private final void writeRegistryToMetaInfServices(
      Path tempDir, Class<? extends SdkLaunchPlanRegistry>... registryClasses) throws IOException {
    Path servicesDir = tempDir.resolve("META-INF/services");
    Files.createDirectories(servicesDir);
    Path serviceLoaderPath = servicesDir.resolve(SdkLaunchPlanRegistry.class.getName());
    try (BufferedWriter writer = Files.newBufferedWriter(serviceLoaderPath);
        PrintWriter printWriter = new PrintWriter(writer)) {
      for (Class<?> c : registryClasses) {
        printWriter.println(c.getName());
      }
    }

    classLoader.addURL(tempDir.toUri().toURL());
  }

  public static class TestRegistry implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return singletonList(
          SdkLaunchPlan.of(new TestWorkflow()).withName("TestPlan").withFixedInput("foo", "bar"));
    }
  }

  public static class OtherTestRegistry implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return singletonList(
          SdkLaunchPlan.of(new TestWorkflow())
              .withName("OtherTestPlan")
              .withFixedInput("foo", "baz"));
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

  public static class DuplicationOfTestRegistry implements SdkLaunchPlanRegistry {

    @Override
    public List<SdkLaunchPlan> getLaunchPlans() {
      return singletonList(
          SdkLaunchPlan.of(new TestWorkflow()).withName("TestPlan").withFixedInput("foo", "bar"));
    }
  }

  public static class TestWorkflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      // Do nothing
    }
  }

  public static class DynamicURLClassLoader extends URLClassLoader {

    public DynamicURLClassLoader(URLClassLoader classLoader) {
      super(classLoader.getURLs());
    }

    @Override
    public void addURL(URL url) {
      super.addURL(url);
    }
  }
}
