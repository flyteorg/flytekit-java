package org.flyte.flytekit;

public abstract class FlyteBuilder<T> {
    abstract FlyteTransform<T> build();

    abstract T getOutputs(SdkNode node);
}