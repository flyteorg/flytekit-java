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
package org.flyte.jflyte;

import java.util.Map;
import java.util.stream.Collectors;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;

public class Modules {

  public static Map<String, WorkflowTemplate> loadWorkflows(Map<String, String> env) {
    Map<WorkflowIdentifier, WorkflowTemplate> registrarWorkflows =
        Registrars.loadAll(WorkflowTemplateRegistrar.class, env);

    return registrarWorkflows.entrySet().stream()
        .collect(Collectors.toMap(x -> x.getKey().name(), Map.Entry::getValue));
  }

  public static Map<String, RunnableTask> loadTasks(Map<String, String> env) {
    Map<TaskIdentifier, RunnableTask> registrarRunnableTasks =
        Registrars.loadAll(RunnableTaskRegistrar.class, env);

    return registrarRunnableTasks.entrySet().stream()
        .collect(Collectors.toMap(x -> x.getKey().name(), Map.Entry::getValue));
  }
}
