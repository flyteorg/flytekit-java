package org.flyte.flytekit;

public class FlyteNode<T> {
    private SdkNode node;
    private T outputs;

    public FlyteNode(SdkNode node, T out) {
      this.node = node;
      this.outputs = out;
    }

    public T getOutputs() {
      return outputs;
    }

    public SdkNode get() {
      return node;
    }
}