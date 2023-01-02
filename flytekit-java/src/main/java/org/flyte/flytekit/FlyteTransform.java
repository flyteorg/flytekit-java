package org.flyte.flytekit;

import java.time.Duration;

public class FlyteTransform<T> {

    private final SdkTransform transform;
    private final FlyteBuilder<T> builder;

    public FlyteTransform(SdkTransform transform, FlyteBuilder<T> builder) {
        this.transform = transform;
        this.builder = builder;
    }

    public FlyteTransform<T> withUpstreamNode(SdkNode node) {
        transform.withUpstreamNode(node);
        return this;
    }

    public FlyteTransform<T> withTimeoutOverride(Duration timeout) {
        transform.withTimeoutOverride(timeout);
        return this;
    }

    public FlyteTransform<T> withNameOverride(String name) {
        transform.withNameOverride(name);
        return this;
    }

    public FlyteBuilder<T> getBuilder() {
        return builder;
    }
}