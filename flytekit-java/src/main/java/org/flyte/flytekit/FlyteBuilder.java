package org.flyte.flytekit;

public abstract class FlyteBuilder<T> {
    abstract SdkTransform build();

    abstract T getOutputs(SdkNode node);
}

