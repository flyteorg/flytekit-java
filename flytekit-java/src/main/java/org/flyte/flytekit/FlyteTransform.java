package org.flyte.flytekit;

public class FlyteTransform<T> {

    private SdkTransform transform;
    private FlyteBuilder2<T> builder2;

    public FlyteTransform(SdkTransform transform, FlyteBuilder2<T> builder2) {
        this.transform = transform;
        this.builder2 = builder2;
    }

    public SdkTransform getTransform() {
        return transform;
    }

    public FlyteBuilder2<T> getBuilder2() {
        return builder2;
    }
}
