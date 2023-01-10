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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.time.Duration;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class NodeMetadataExampleWorkflow extends SdkWorkflow<NodeMetadataExampleWorkflow.Output> {

  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<String> c();

    /**
     * Wraps the constructor of the generated output value class.
     *
     * @param c the String literal output of {@link NodeMetadataExampleWorkflow}
     * @return output of NodeMetadataExampleWorkflow
     */
    public static NodeMetadataExampleWorkflow.Output create(String c) {
      return new AutoValue_NodeMetadataExampleWorkflow_Output(SdkBindingData.ofString(c));
    }
  }

  public NodeMetadataExampleWorkflow() {
    super(JacksonSdkType.of(NodeMetadataExampleWorkflow.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> a = SdkBindingData.ofInteger(0);
    SdkBindingData<Long> b = SdkBindingData.ofInteger(1);

    SdkBindingData<Long> c =
        builder
            .apply(
                "sum-a-b",
                SumTask.of(a, b)
                    .withNameOverride("sum a+b")
                    .withTimeoutOverride(Duration.ofMinutes(15)))
            .getOutputs()
            .c();

    builder.output("c", c, "Value of the sum");
  }
}
