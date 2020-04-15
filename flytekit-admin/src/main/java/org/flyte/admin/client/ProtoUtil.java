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
package org.flyte.admin.client;

import static java.util.Objects.requireNonNull;

import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.IdentifierOuterClass.ResourceType;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import flyteidl.core.Types;
import flyteidl.core.Workflow;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.Identifier;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;

/** Utility to serialize flytekit-api into flyteidl proto. */
class ProtoUtil {

  public static IdentifierOuterClass.Identifier serialize(Identifier id) {
    ResourceType type = getResourceType(id);

    return IdentifierOuterClass.Identifier.newBuilder()
        .setResourceType(type)
        .setDomain(id.domain())
        .setProject(id.project())
        .setName(id.name())
        .setVersion(id.version())
        .build();
  }

  private static ResourceType getResourceType(Identifier id) {
    if (id instanceof LaunchPlanIdentifier) { // if only Java 14 :(
      return ResourceType.LAUNCH_PLAN;
    } else if (id instanceof TaskIdentifier) {
      return ResourceType.TASK;
    } else if (id instanceof WorkflowIdentifier) {
      return ResourceType.WORKFLOW;
    }
    throw new IllegalArgumentException("Unknown Identifier type: " + id.getClass());
  }

  public static Tasks.TaskTemplate serialize(TaskTemplate taskTemplate) {
    Tasks.RuntimeMetadata runtime =
        Tasks.RuntimeMetadata.newBuilder()
            .setType(Tasks.RuntimeMetadata.RuntimeType.FLYTE_SDK)
            .setFlavor("java")
            .setVersion("0.0.1")
            .build();

    Tasks.TaskMetadata metadata = Tasks.TaskMetadata.newBuilder().setRuntime(runtime).build();

    Container container =
        requireNonNull(
            taskTemplate.container(), "Only container based task templates are supported");
    return Tasks.TaskTemplate.newBuilder()
        .setContainer(serialize(container))
        .setMetadata(metadata)
        .setInterface(serialize(taskTemplate.interface_()))
        .setType("java-task")
        .build();
  }

  private static Interface.TypedInterface serialize(TypedInterface interface_) {
    return Interface.TypedInterface.newBuilder().setInputs(serialize(interface_.inputs())).build();
  }

  public static Interface.VariableMap serialize(Map<String, Variable> inputs) {
    Interface.VariableMap.Builder builder = Interface.VariableMap.newBuilder();

    inputs.forEach((key, value) -> builder.putVariables(key, serialize(value)));

    return builder.build();
  }

  public static Interface.Variable serialize(Variable value) {
    return Interface.Variable.newBuilder()
        .setDescription(value.description())
        .setType(serialize(value.literalType()))
        .build();
  }

  public static Types.LiteralType serialize(LiteralType literalType) {
    return Types.LiteralType.newBuilder().setSimple(serialize(literalType.simpleType())).build();
  }

  @Nullable
  public static Types.SimpleType serialize(@Nullable SimpleType simpleType) {
    if (simpleType == null) {
      return null;
    }

    switch (simpleType) {
      case STRING:
        return Types.SimpleType.STRING;
      default:
        // FIXME
        return Types.SimpleType.UNRECOGNIZED;
    }
  }

  public static Tasks.Container serialize(Container container) {
    return Tasks.Container.newBuilder()
        .setImage(container.image())
        .addAllCommand(container.command())
        .addAllArgs(container.args())
        .build();
  }

  public static Workflow.WorkflowTemplate serialize(WorkflowTemplate template) {
    Workflow.WorkflowTemplate.Builder builder =
        Workflow.WorkflowTemplate.newBuilder().setMetadata(serialize(template.metadata()));

    template.nodes().forEach(node -> builder.addNodes(serialize(node)));

    return builder.build();
  }

  private static Workflow.WorkflowMetadata serialize(
      @SuppressWarnings("UnusedVariable") WorkflowMetadata metadata) {
    return Workflow.WorkflowMetadata.newBuilder().build();
  }

  private static Workflow.Node serialize(Node node) {
    Workflow.TaskNode taskNode =
        Workflow.TaskNode.newBuilder()
            .setReferenceId(serialize(node.taskNode().referenceId()))
            .build();

    Workflow.Node.Builder builder =
        Workflow.Node.newBuilder().setId(node.id()).setTaskNode(taskNode);

    node.inputs().forEach(input -> builder.addInputs(serialize(input)));

    return builder.build();
  }

  public static Literals.Binding serialize(Binding input) {
    return Literals.Binding.newBuilder()
        .setVar(input.var_())
        .setBinding(serialize(input.binding()))
        .build();
  }

  public static Literals.BindingData serialize(BindingData binding) {
    return Literals.BindingData.newBuilder().setScalar(serialize(binding.scalar())).build();
  }

  public static Literals.Scalar serialize(@Nullable Scalar scalar) {
    if (scalar == null) {
      return null;
    }

    Primitive primitive = requireNonNull(scalar.primitive(), "Only primitive scalar are supported");
    return Literals.Scalar.newBuilder().setPrimitive(serialize(primitive)).build();
  }

  private static Literals.Primitive serialize(Primitive primitive) {
    return Literals.Primitive.newBuilder().setStringValue(primitive.string()).build();
  }
}
