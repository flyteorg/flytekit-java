package org.flyte.flytekit;

public abstract class FlyteBuilder2<T> {
    abstract FlyteTransform<T> build();

    abstract T getOutputs(SdkNode node);
}
