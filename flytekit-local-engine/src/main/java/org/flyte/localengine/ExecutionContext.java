/*
 * Copyright 2022-2023 Flyte Authors
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

/**
 * Contains context data used by the {@link ExecutionNodeCompiler} and the {@link LocalEngine} in
 * order to compile and execute workflows, subworkflos, tasks, etc.
 */
@AutoValue
public abstract class ExecutionContext {
  /** Mapping from task name to {@link RunnableTask}. */
  abstract Map<String, RunnableTask> runnableTasks();

  /** Mapping from task name to {@link DynamicWorkflowTask}. */
  abstract Map<String, DynamicWorkflowTask> dynamicWorkflowTasks();

  /** Mapping from workflow name to {@link WorkflowTemplate}. */
  abstract Map<String, WorkflowTemplate> workflowTemplates();

  /** Mapping from launch plan name to {@link RunnableLaunchPlan}. */
  abstract Map<String, RunnableLaunchPlan> runnableLaunchPlans();

  /**
   * Instance of {@link ExecutionListener} to be able to register nodes depending on their state.
   */
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

    /** Stores the mapping from task name to {@link RunnableTask}. */
    public abstract Builder runnableTasks(Map<String, RunnableTask> runnableTasks);

    /** Stores the mapping from task name to {@link DynamicWorkflowTask}. */
    public abstract Builder dynamicWorkflowTasks(
        Map<String, DynamicWorkflowTask> dynamicWorkflowTasks);

    /** Stores the mapping from workflow name to {@link WorkflowTemplate}. */
    public abstract Builder workflowTemplates(Map<String, WorkflowTemplate> workflowTemplates);

    /** Stores the mapping from launch plan name to {@link RunnableLaunchPlan}. */
    public abstract Builder runnableLaunchPlans(
        Map<String, RunnableLaunchPlan> runnableLaunchPlans);

    /** Stores the {@link ExecutionListener} used to register nodes depending on their state. */
    public abstract Builder executionListener(ExecutionListener executionListener);

    public abstract ExecutionContext build();
  }
}
