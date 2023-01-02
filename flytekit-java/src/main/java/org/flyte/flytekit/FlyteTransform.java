package org.flyte.flytekit;

public class FlyteTransform<T> {

    private final SdkTransform transform;
    private final FlyteBuilder<T> builder;

    public FlyteTransform(SdkTransform transform, FlyteBuilder<T> builder) {
        this.transform = transform;
        this.builder = builder;
    }

    public SdkTransform getTransform() {
        return transform;
    }

    public FlyteBuilder<T> getBuilder() {
        return builder;
    }
}