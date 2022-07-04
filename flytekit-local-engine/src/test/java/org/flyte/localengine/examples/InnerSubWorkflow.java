package org.flyte.localengine.examples;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class InnerSubWorkflow extends SdkWorkflow {
    @Override
    public void expand(SdkWorkflowBuilder builder) {
        SdkBindingData a = builder.inputOfInteger("a");
        SdkBindingData b = builder.inputOfInteger("b");
        SdkBindingData c = builder.apply("sum-task", new SumTask().withInput("a", a).withInput("b", b)).getOutput("c");

        builder.output("result", c);
    }
}
