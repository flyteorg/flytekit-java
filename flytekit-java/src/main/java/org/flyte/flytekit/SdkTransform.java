/*
 * Copyright 2021 Flyte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.flytekit;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/** Implementations of {@code SdkTransform} transform a set of inputs into a {@link SdkNode}. */
public abstract class SdkTransform<InputT, OutputT> {

  /** Specifies the transform input type. */
  public abstract SdkType<InputT> getInputType();

  /** Specifies the transform output type. */
  public abstract SdkType<OutputT> getOutputType();

  /**
   * Applies this transformation over {@code inputs}.
   *
   * @param builder workflow builder to keep tracks of the nodes that have been created.
   * @param nodeId node id of the new node
   * @param upstreamNodeIds node id lists for explicits upstream dependencies. The
   * @param metadata node's metadata
   * @param inputs inputs to transform
   * @return the new {@link SdkNode}
   */
  public final SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      @Nullable InputT inputs) {
    checkNullOnlyVoid(inputs);
    var inputsBindings = getInputType().toSdkBindingMap(inputs);
    return apply(builder, nodeId, upstreamNodeIds, metadata, inputsBindings);
  }

  /**
   * Applies this transformation over {@code inputs}.
   *
   * @param builder workflow builder to keep tracks of the nodes that have been created.
   * @param nodeId node id of the new node
   * @param upstreamNodeIds node id lists for explicits upstream dependencies. The
   * @param metadata node's metadata
   * @param inputs inputs to transform
   * @return the new {@link SdkNode}
   */
  abstract SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs);

  /**
   * Returns a new transformation derived from this one, with an explicit upstream node dependency.
   *
   * @param node the explicit upstream node dependency
   * @return the new transformation
   */
  public SdkTransform<InputT, OutputT> withUpstreamNode(SdkNode<?> node) {
    return SdkMetadataDecoratorTransform.of(this, List.of(node.getNodeId()));
  }

  /**
   * Returns a new transformation derived from this one, with the name overriden by a new one.
   *
   * @param name new name
   * @return the new transformation
   */
  public SdkTransform<InputT, OutputT> withNameOverride(String name) {
    requireNonNull(name, "Name override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().name(name).build();
    return SdkMetadataDecoratorTransform.of(this, metadata);
  }

  SdkTransform<InputT, OutputT> withNameOverrideIfNotSet(String name) {
    return withNameOverride(name);
  }

  /**
   * Returns a new transformation derived from this one, with the timeout overriden by a new one.
   *
   * @param timeout new timeout
   * @return the new transformation
   */
  public SdkTransform<InputT, OutputT> withTimeoutOverride(Duration timeout) {
    requireNonNull(timeout, "Timeout override cannot be null");

    SdkNodeMetadata metadata = SdkNodeMetadata.builder().timeout(timeout).build();
    return SdkMetadataDecoratorTransform.of(this, metadata);
  }

  /** Returns the name of the transformation. */
  public String getName() {
    return getClass().getName();
  }

  void checkNullOnlyVoid(@Nullable InputT inputs) {
    Set<String> variableNames = getInputType().variableNames();
    boolean hasProperties = !variableNames.isEmpty();
    if (inputs == null && hasProperties) {
      throw new IllegalArgumentException(
          "Null supplied as input for a transform with variables: " + variableNames);
    } else if (inputs != null && !hasProperties) {
      throw new IllegalArgumentException("Null input expected for a transform with no variables");
    }
  }
}
