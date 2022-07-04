package org.flyte.localengine.examples;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class SubWorkflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
        SdkBindingData a = builder.inputOfInteger("a");
        SdkBindingData b = builder.inputOfInteger("b");
        SdkBindingData c = builder.inputOfInteger("c");
        SdkBindingData ab = builder.apply("sum-a-b", new SumTask().withInput("a", a).withInput("b", b)).getOutput("c");
        SdkBindingData res = builder.apply("sum-ab-c", new InnerSubWorkflow().withInput("a", ab).withInput("b", c)).getOutput("result");
        builder.output("result", res);
    }
}