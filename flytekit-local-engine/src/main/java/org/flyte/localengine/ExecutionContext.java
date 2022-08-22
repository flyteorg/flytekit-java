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
package org.flyte.localengine;

import static java.util.Collections.emptyMap;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.WorkflowTemplate;

@AutoValue
public abstract class ExecutionContext {
  abstract Map<String, RunnableTask> runnableTasks();

  abstract Map<String, DynamicWorkflowTask> dynamicWorkflowTasks();

  abstract Map<String, WorkflowTemplate> workflowTemplates();

  abstract Map<String, RunnableLaunchPlan> runnableLaunchPlans();

  abstract ExecutionListener executionListener();

  public static Builder builder() {
    return new AutoValue_ExecutionContext.Builder()
        .dynamicWorkflowTasks(emptyMap())
        .executionListener(NoopExecutionListener.create())
        .runnableTasks(emptyMap())
        .runnableLaunchPlans(emptyMap())
        .workflowTemplates(emptyMap());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder runnableTasks(Map<String, RunnableTask> runnableTasks);

    public abstract Builder dynamicWorkflowTasks(
        Map<String, DynamicWorkflowTask> dynamicWorkflowTasks);

    public abstract Builder workflowTemplates(Map<String, WorkflowTemplate> workflowTemplates);

    public abstract Builder runnableLaunchPlans(
        Map<String, RunnableLaunchPlan> runnableLaunchPlans);

    public abstract Builder executionListener(ExecutionListener executionListener);

    public abstract ExecutionContext build();
  }
}
