/*
 * Copyright 2023 Flyte Authors.
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
import java.util.Map;
import org.flyte.api.v1.PluginTask;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkPluginTaskRegistrarTest {
  private static final String TASK_TYPE = "test-task";
  private static final Map<String, String> ENV =
      Map.of(PROJECT_ENV_VAR, "project", DOMAIN_ENV_VAR, "domain", VERSION_ENV_VAR, "version");

  private SdkPluginTaskRegistrar registrar;

  @BeforeEach
  void setUp() {
    registrar = new SdkPluginTaskRegistrar();
  }

  @Test
  void shouldLoadPluginTasksFromDiscoveredRegistries() {
    // given
    String testTaskName = "org.flyte.flytekit.SdkPluginTaskRegistrarTest$TestTask";
    String otherTestTaskName = "org.flyte.flytekit.SdkPluginTaskRegistrarTest$OtherTestTask";
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

    PluginTask expectedTask = createPluginTask(testTaskName, typedInterface, retries, false);

    TaskIdentifier expectedOtherTestTaskId =
        TaskIdentifier.builder()
            .project("project")
            .domain("domain")
            .name(otherTestTaskName)
            .version("version")
            .build();
    PluginTask expectedOtherTask =
        createPluginTask(otherTestTaskName, typedInterface, otherRetries, true);

    // when
    Map<TaskIdentifier, PluginTask> tasks = registrar.load(ENV);

    // then
    assertAll(
        () -> assertThat(tasks, hasKey(is(expectedTestTaskId))),
        () -> assertThat(tasks, hasKey(is(expectedOtherTestTaskId))));
    assertTaskEquals(tasks.get(expectedTestTaskId), expectedTask);
    assertTaskEquals(tasks.get(expectedOtherTestTaskId), expectedOtherTask);
  }

  private PluginTask createPluginTask(
      String taskName, TypedInterface typedInterface, RetryStrategy retries, boolean isSyncPlugin) {
    return new PluginTask() {
      @Override
      public boolean isSyncPlugin() {
        return isSyncPlugin;
      }

      @Override
      public String getName() {
        return taskName;
      }

      @Override
      public String getType() {
        return TASK_TYPE;
      }

      @Override
      public TypedInterface getInterface() {
        return typedInterface;
      }

      @Override
      public RetryStrategy getRetries() {
        return retries;
      }
    };
  }

  private void assertTaskEquals(PluginTask actualTask, PluginTask expectedTask) {
    assertThat(actualTask.getName(), equalTo(expectedTask.getName()));
    assertThat(actualTask.getType(), equalTo(expectedTask.getType()));
    assertThat(actualTask.getCustom(), equalTo(expectedTask.getCustom()));
    assertThat(actualTask.getInterface(), equalTo(expectedTask.getInterface()));
    assertThat(actualTask.getRetries(), equalTo(expectedTask.getRetries()));
  }

  @AutoService(SdkPluginTask.class)
  public static class TestTask extends SdkPluginTask<Void, Void> {

    private static final long serialVersionUID = 2751205856616541247L;

    public TestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public String getType() {
      return TASK_TYPE;
    }
  }

  @AutoService(SdkPluginTask.class)
  public static class OtherTestTask extends SdkPluginTask<Void, Void> {

    private static final long serialVersionUID = -7757282344498000982L;

    public OtherTestTask() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public String getType() {
      return TASK_TYPE;
    }

    @Override
    public int getRetries() {
      return 1;
    }

    @Override
    public boolean isSyncPlugin() {
      return true;
    }
  }
}
