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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.WorkflowNode;
import org.junit.jupiter.api.Test;

public class SdkRemoteLaunchPlanTest {
  @Test
  void applyShouldReturnASdkWorkflowNode() {
    var inputs =
        TestPairIntegerInput.create(SdkBindingDataFactory.of(1), SdkBindingDataFactory.of(2));
    SdkRemoteLaunchPlan<TestPairIntegerInput, TestUnaryBooleanOutput> remoteLaunchPlan =
        new TestSdkRemoteLaunchPlan();

    SdkNode<TestUnaryBooleanOutput> node =
        remoteLaunchPlan.apply(
            mock(SdkWorkflowBuilder.class),
            "some-node-id",
            singletonList("upstream-1"),
            /*metadata=*/ null,
            inputs);

    assertAll(
        () -> assertThat(node.getNodeId(), is("some-node-id")),
        () ->
            assertThat(
                node.toIdl(),
                is(
                    Node.builder()
                        .id("some-node-id")
                        .workflowNode(
                            WorkflowNode.builder()
                                .reference(
                                    WorkflowNode.Reference.ofLaunchPlanRef(
                                        PartialLaunchPlanIdentifier.builder()
                                            .domain("dev")
                                            .project("project-a")
                                            .name("SomeWorkflow")
                                            .version("version")
                                            .build()))
                                .build())
                        .upstreamNodeIds(singletonList("upstream-1"))
                        .inputs(
                            Arrays.asList(
                                Binding.builder()
                                    .var_("a")
                                    .binding(
                                        BindingData.ofScalar(
                                            Scalar.ofPrimitive(Primitive.ofIntegerValue(1))))
                                    .build(),
                                Binding.builder()
                                    .var_("b")
                                    .binding(
                                        BindingData.ofScalar(
                                            Scalar.ofPrimitive(Primitive.ofIntegerValue(2))))
                                    .build()))
                        .build())),
        () ->
            assertThat(
                node.getOutputBindings(),
                is(
                    singletonMap(
                        "o",
                        SdkBindingData.promise(SdkLiteralTypes.booleans(), "some-node-id", "o")))));
  }

  @SuppressWarnings("ExtendsAutoValue")
  static class TestSdkRemoteLaunchPlan
      extends SdkRemoteLaunchPlan<TestPairIntegerInput, TestUnaryBooleanOutput> {

    @Override
    public String domain() {
      return "dev";
    }

    @Override
    public String project() {
      return "project-a";
    }

    @Override
    public String name() {
      return "SomeWorkflow";
    }

    @Override
    public String version() {
      return "version";
    }

    @Override
    public SdkType<TestPairIntegerInput> inputs() {
      return new TestPairIntegerInput.SdkType();
    }

    @Override
    public SdkType<TestUnaryBooleanOutput> outputs() {
      return new TestUnaryBooleanOutput.SdkType();
    }
  }
}
