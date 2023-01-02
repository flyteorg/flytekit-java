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

import static java.util.Collections.singletonMap;
import static org.flyte.flytekit.SdkConfig.DOMAIN_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.PROJECT_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.VERSION_ENV_VAR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.ContainerTask;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;
import org.junit.jupiter.api.Test;

public class SdkContainerTaskRegistrarTest {
  private static final Map<String, String> ENV;

  static {
    HashMap<String, String> env = new HashMap<>();
    env.put(PROJECT_ENV_VAR, "project");
    env.put(DOMAIN_ENV_VAR, "domain");
    env.put(VERSION_ENV_VAR, "version");
    ENV = Collections.unmodifiableMap(env);
  }

  private static final String TEST_TASK_NAME =
      "org.flyte.flytekit.SdkContainerTaskRegistrarTest$TestTask";
  private static final String OTHER_TEST_TASK_NAME =
      "org.flyte.flytekit.SdkContainerTaskRegistrarTest$OtherTestTask";
  private final SdkContainerTaskRegistrar registrar = new SdkContainerTaskRegistrar();

  @Test
  void shouldLoadMultipleTasksFromDiscoveredRegistries() {
    // given
    TaskIdentifier expectedTestTaskId = taskIdentifier(TEST_TASK_NAME);
    TaskIdentifier expectedOtherTestTaskId = taskIdentifier(OTHER_TEST_TASK_NAME);

    // when
    Map<TaskIdentifier, ContainerTask> tasks = registrar.load(ENV);

    // then
    assertAll(
        () -> assertThat(tasks, hasKey(is(expectedTestTaskId))),
        () -> assertThat(tasks, hasKey(is(expectedOtherTestTaskId))));
  }

  @Test
  void shouldLoadTaskParameters() {
    // given
    TaskIdentifier expectedTestTaskId = taskIdentifier(TEST_TASK_NAME);

    // when
    Map<TaskIdentifier, ContainerTask> tasks = registrar.load(ENV);

    // then
    ContainerTask task = tasks.get(expectedTestTaskId);

    assertThat(task.getName(), equalTo(TEST_TASK_NAME));
    assertThat(task.getType(), equalTo("raw-container"));
    assertThat(
        task.getInterface(),
        equalTo(
            TypedInterface.builder()
                .inputs(SdkTypes.nulls().getVariableMap())
                .outputs(SdkTypes.nulls().getVariableMap())
                .build()));
    assertThat(
        task.getResources(),
        equalTo(
            Resources.builder()
                .requests(resources("0.5", "2Gi"))
                .limits(resources("2", "5Gi"))
                .build()));
    assertThat(task.getImage(), equalTo("image"));
    assertThat(task.getArgs(), contains("-c", "echo hello"));
    assertThat(task.getCommand(), contains("bash"));
    assertThat(task.getEnv(), contains(KeyValuePair.of("KEY", "VALUE")));
  }

  @AutoService(SdkContainerTask.class)
  public static class TestTask extends SdkContainerTask<Void, Void> {

    private static final long serialVersionUID = 2751205856616541247L;

    public TestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public SdkResources getResources() {
      return SdkResources.builder()
          .requests(sdkResources("0.5", "2Gi"))
          .limits(sdkResources("2", "5Gi"))
          .build();
    }

    @Override
    public String getImage() {
      return "image";
    }

    @Override
    public List<String> getArgs() {
      return Arrays.asList("-c", "echo hello");
    }

    @Override
    public List<String> getCommand() {
      return Arrays.asList("bash");
    }

    @Override
    public Map<String, String> getEnv() {
      return singletonMap("KEY", "VALUE");
    }
  }

  @AutoService(SdkContainerTask.class)
  public static class OtherTestTask extends SdkContainerTask<Void, Void> {

    private static final long serialVersionUID = -7757282344498000982L;

    public OtherTestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public String getImage() {
      return "other-image";
    }
  }

  private static TaskIdentifier taskIdentifier(String taskName) {
    return TaskIdentifier.builder()
        .project("project")
        .domain("domain")
        .name(taskName)
        .version("version")
        .build();
  }

  private static Map<Resources.ResourceName, String> resources(String cpu, String memory) {
    Map<Resources.ResourceName, String> limits = new HashMap<>();
    limits.put(Resources.ResourceName.CPU, cpu);
    limits.put(Resources.ResourceName.MEMORY, memory);
    return limits;
  }

  private static Map<SdkResources.ResourceName, String> sdkResources(String cpu, String memory) {
    Map<SdkResources.ResourceName, String> limits = new HashMap<>();
    limits.put(SdkResources.ResourceName.CPU, cpu);
    limits.put(SdkResources.ResourceName.MEMORY, memory);
    return limits;
  }
}
