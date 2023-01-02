package org.flyte.flytekit;

public class FlyteNode<T> {
    private SdkNode node;
    private T out;

    public FlyteNode(SdkNode node, T out) {
      this.node = node;
      this.out = out;
    }

    public T getOutputs() {
      return out;
    }

    public SdkNode getNode() {
      return node;
    }
  }