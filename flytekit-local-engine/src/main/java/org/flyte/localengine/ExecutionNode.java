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

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.WorkflowTemplate;

@AutoValue
public abstract class ExecutionNode {

  public abstract String nodeId();

  public abstract List<String> upstreamNodeIds();

  @Nullable
  public abstract List<Binding> bindings();

  @Nullable
  public abstract RunnableTask runnableTask();

  @Nullable
  public abstract WorkflowTemplate subWorkflow();

  public abstract int attempts();

  static Builder builder() {
    return new AutoValue_ExecutionNode.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder nodeId(String nodeId);

    abstract Builder upstreamNodeIds(List<String> upstreamNodeIds);

    abstract Builder bindings(List<Binding> bindings);

    abstract Builder runnableTask(RunnableTask runnableTask);

    abstract Builder subWorkflow(WorkflowTemplate subWorkflow);

    abstract Builder attempts(int attempts);

    abstract ExecutionNode build();
  }
}
