/*
 * Copyright 2020 Spotify AB.
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
package org.flyte.jflyte;

import static org.flyte.jflyte.TestingListener.ofCompleted;
import static org.flyte.jflyte.TestingListener.ofError;
import static org.flyte.jflyte.TestingListener.ofPending;
import static org.flyte.jflyte.TestingListener.ofRetrying;
import static org.flyte.jflyte.TestingListener.ofStarting;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.TypedInterface;
import org.junit.jupiter.api.Test;

public class ChainedExecutionListenerTest {

  @Test
  public void testChaining() {
    TestingListener listener1 = new TestingListener();
    TestingListener listener2 = new TestingListener();

    ChainedExecutionListener chained =
        new ChainedExecutionListener(ImmutableList.of(listener1, listener2));
    ExecutionNode node =
        ExecutionNode.builder()
            .nodeId("node-1")
            .upstreamNodeIds(ImmutableList.of())
            .bindings(ImmutableList.of())
            .runnableTask(new EmptyRunnableTask())
            .attempts(1)
            .build();

    Literal a = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(42L)));
    Literal b = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(1337L)));

    chained.pending(node);
    chained.starting(node, ImmutableMap.of("a", a));
    chained.retrying(node, ImmutableMap.of("a", a), new RuntimeException("oops"), /* attempt= */ 0);
    chained.completed(node, ImmutableMap.of("a", a), ImmutableMap.of("b", b));
    chained.error(node, ImmutableMap.of("a", a), new RuntimeException("oops"));

    List<List<Object>> expected =
        ImmutableList.of(
            ofPending("node-1"),
            ofStarting("node-1", ImmutableMap.of("a", a)),
            ofRetrying("node-1", ImmutableMap.of("a", a), "oops", /* attempt= */ 0),
            ofCompleted("node-1", ImmutableMap.of("a", a), ImmutableMap.of("b", b)),
            ofError("node-1", ImmutableMap.of("a", a), "oops"));

    assertEquals(expected, listener1.actions);
    assertEquals(expected, listener2.actions);
  }

  private static class EmptyRunnableTask implements RunnableTask {

    @Override
    public String getName() {
      return "empty_runnable_task";
    }

    @Override
    public TypedInterface getInterface() {
      return TypedInterface.builder().inputs(ImmutableMap.of()).outputs(ImmutableMap.of()).build();
    }

    @Override
    public Map<String, Literal> run(Map<String, Literal> inputs) {
      return ImmutableMap.of();
    }

    @Override
    public RetryStrategy getRetries() {
      return RetryStrategy.builder().retries(0).build();
    }
  }
}
