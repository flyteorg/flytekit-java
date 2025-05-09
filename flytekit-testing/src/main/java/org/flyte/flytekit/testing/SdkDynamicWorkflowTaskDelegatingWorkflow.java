/*
 * Copyright 2025 Flyte Authors.
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
package org.flyte.flytekit.testing;

import java.util.Objects;
import org.flyte.flytekit.SdkDynamicWorkflowTask;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

public class SdkDynamicWorkflowTaskDelegatingWorkflow<InputT, OutputT>
    extends SdkWorkflow<Void, OutputT> {
  private final SdkDynamicWorkflowTask<InputT, OutputT> delegate;
  private final InputT input;

  public SdkDynamicWorkflowTaskDelegatingWorkflow(
      SdkDynamicWorkflowTask<InputT, OutputT> delegate,
      InputT input,
      SdkType<OutputT> outputSdkType) {
    super(SdkTypes.nulls(), outputSdkType);
    this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
    this.input = input;
  }

  @Override
  protected OutputT expand(SdkWorkflowBuilder builder, Void ignored) {
    return delegate.run(builder, this.input);
  }
}
