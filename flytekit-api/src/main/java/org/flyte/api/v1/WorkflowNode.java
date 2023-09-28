/*
 * Copyright 2020-2021 Flyte Authors
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
package org.flyte.api.v1;

import com.google.auto.value.AutoOneOf;
import com.google.auto.value.AutoValue;

/** Refers to a the workflow the node is to execute. */
@AutoValue
public abstract class WorkflowNode {
  public abstract Reference reference();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_WorkflowNode.Builder();
  }

  @AutoOneOf(Reference.Kind.class)
  public abstract static class Reference {
    public enum Kind {
      /** A globally unique identifier for the launch plan. */
      LAUNCH_PLAN_REF,

      /** Reference to a subworkflow, that should be defined with the compiler context. */
      SUB_WORKFLOW_REF
    }

    public abstract Kind kind();

    public abstract PartialLaunchPlanIdentifier launchPlanRef();

    public abstract PartialWorkflowIdentifier subWorkflowRef();

    public static Reference ofLaunchPlanRef(PartialLaunchPlanIdentifier launchPlanRef) {
      return AutoOneOf_WorkflowNode_Reference.launchPlanRef(launchPlanRef);
    }

    public static Reference ofSubWorkflowRef(PartialWorkflowIdentifier subWorkflowRef) {
      return AutoOneOf_WorkflowNode_Reference.subWorkflowRef(subWorkflowRef);
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder reference(Reference reference);

    public abstract WorkflowNode build();
  }
}
