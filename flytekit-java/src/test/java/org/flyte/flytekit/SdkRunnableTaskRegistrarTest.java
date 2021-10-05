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

import static org.flyte.flytekit.SdkConfig.DOMAIN_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.PROJECT_ENV_VAR;
import static org.flyte.flytekit.SdkConfig.VERSION_ENV_VAR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;
import org.junit.jupiter.api.Test;

public class SdkRunnableTaskRegistrarTest {
  private static final Map<String, String> ENV;

  static {
    HashMap<String, String> env = new HashMap<>();
    env.put(PROJECT_ENV_VAR, "project");
    env.put(DOMAIN_ENV_VAR, "domain");
    env.put(VERSION_ENV_VAR, "version");
    ENV = Collections.unmodifiableMap(env);
  }

  private final SdkRunnableTaskRegistrar registrar = new SdkRunnableTaskRegistrar();

  @Test
  void shouldLoadRunnableTasksFromDiscoveredRegistries() {
    // given
    String testTaskName = "org.flyte.flytekit.SdkRunnableTaskRegistrarTest$TestTask";
    String otherTestTaskName = "org.flyte.flytekit.SdkRunnableTaskRegistrarTest$OtherTestTask";
    TaskIdentifier expectedTestTaskId =
        TaskIdentifier.builder()
            .project("project")
            .domain("domain")
            .name(testTaskName)
            .version("version")
            .build();

    TypedInterface typedInterface =
        TypedInterface.builder()
            .inputs(SdkTypes.nulls().getVariableMap())
            .outputs(SdkTypes.nulls().getVariableMap())
            .build();

    RetryStrategy retries = RetryStrategy.builder().retries(0).build();
    RetryStrategy otherRetries = RetryStrategy.builder().retries(1).build();

    Map<Resources.ResourceName, String> limits = new HashMap<>();
    limits.put(Resources.ResourceName.CPU, "0.5");
    limits.put(Resources.ResourceName.MEMORY, "2Gi");
    Map<Resources.ResourceName, String> requests = new HashMap<>();
    requests.put(Resources.ResourceName.CPU, "2");
    requests.put(Resources.ResourceName.MEMORY, "5Gi");
    Resources resources = Resources.builder().limits(limits).requests(requests).build();

    RunnableTask expectedTask = createRunnableTask(testTaskName, typedInterface, retries, null);

    TaskIdentifier expectedOtherTestTaskId =
        TaskIdentifier.builder()
            .project("project")
            .domain("domain")
            .name(otherTestTaskName)
            .version("version")
            .build();
    RunnableTask expectedOtherTask =
        createRunnableTask(otherTestTaskName, typedInterface, otherRetries, resources);

    // when
    Map<TaskIdentifier, RunnableTask> tasks = registrar.load(ENV);

    // then
    assertAll(
        () -> assertThat(tasks, hasKey(is(expectedTestTaskId))),
        () -> assertThat(tasks, hasKey(is(expectedOtherTestTaskId))));
    assertTaskEquals(tasks.get(expectedTestTaskId), expectedTask);
    assertTaskEquals(tasks.get(expectedOtherTestTaskId), expectedOtherTask);
  }

  private RunnableTask createRunnableTask(
      String taskName, TypedInterface typedInterface, RetryStrategy retries, Resources resources) {
    return new RunnableTask() {
      @Override
      public String getName() {
        return taskName;
      }

      @Override
      public TypedInterface getInterface() {
        return typedInterface;
      }

      @Override
      public Map<String, Literal> run(Map<String, Literal> inputs) {
        System.out.println("Hello world");
        return null;
      }

      @Override
      public RetryStrategy getRetries() {
        return retries;
      }

      @Override
      public Resources getResources() {
        if (resources == null) {
          return RunnableTask.super.getResources();
        } else {
          return resources;
        }
      }
    };
  }

  private void assertTaskEquals(RunnableTask actualTask, RunnableTask expectedTask) {
    assertThat(actualTask.getName(), equalTo(expectedTask.getName()));
    assertThat(actualTask.getType(), equalTo(expectedTask.getType()));
    assertThat(actualTask.getCustom(), equalTo(expectedTask.getCustom()));
    assertThat(actualTask.getInterface(), equalTo(expectedTask.getInterface()));
    assertThat(actualTask.getResources(), equalTo(expectedTask.getResources()));
  }

  @AutoService(SdkRunnableTask.class)
  public static class TestTask extends SdkRunnableTask<Void, Void> {

    private static final long serialVersionUID = 2751205856616541247L;

    public TestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public Void run(Void input) {
      System.out.println("Hello World, Task");
      return null;
    }
  }

  @AutoService(SdkRunnableTask.class)
  public static class OtherTestTask extends SdkRunnableTask<Void, Void> {

    private static final long serialVersionUID = -7757282344498000982L;

    public OtherTestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public Void run(Void input) {
      System.out.println("Hello World, Other Task");
      return null;
    }

    @Override
    public SdkResources getResources() {
      Map<SdkResources.ResourceName, String> limits = new HashMap<>();
      limits.put(SdkResources.ResourceName.CPU, "0.5");
      limits.put(SdkResources.ResourceName.MEMORY, "2Gi");
      Map<SdkResources.ResourceName, String> requests = new HashMap<>();
      requests.put(SdkResources.ResourceName.CPU, "2");
      requests.put(SdkResources.ResourceName.MEMORY, "5Gi");
      return SdkResources.builder().limits(limits).requests(requests).build();
    }
  }
}
