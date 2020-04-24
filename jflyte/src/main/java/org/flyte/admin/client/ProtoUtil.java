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

import com.google.common.annotations.VisibleForTesting;
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
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.Identifier;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.Timestamp;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;

/** Utility to serialize flytekit-api into flyteidl proto. */
class ProtoUtil {

  static final String TASK_TYPE = "java-task";
  static final String RUNTIME_FLAVOR = "java";
  static final String RUNTIME_VERSION = "0.0.1";

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
            .setFlavor(RUNTIME_FLAVOR)
            .setVersion(RUNTIME_VERSION)
            .build();

    Tasks.TaskMetadata metadata = Tasks.TaskMetadata.newBuilder().setRuntime(runtime).build();

    Container container =
        requireNonNull(
            taskTemplate.container(), "Only container based task templates are supported");
    return Tasks.TaskTemplate.newBuilder()
        .setContainer(serialize(container))
        .setMetadata(metadata)
        .setInterface(serialize(taskTemplate.interface_()))
        .setType(TASK_TYPE)
        .build();
  }

  private static Interface.TypedInterface serialize(TypedInterface interface_) {
    return Interface.TypedInterface.newBuilder().setInputs(serialize(interface_.inputs())).build();
  }

  private static Interface.VariableMap serialize(Map<String, Variable> inputs) {
    Interface.VariableMap.Builder builder = Interface.VariableMap.newBuilder();

    inputs.forEach((key, value) -> builder.putVariables(key, serialize(value)));

    return builder.build();
  }

  private static Interface.Variable serialize(Variable value) {
    Interface.Variable.Builder builder =
        Interface.Variable.newBuilder().setType(serialize(value.literalType()));

    String description = value.description();
    if (description != null) {
      builder.setDescription(description);
    }

    return builder.build();
  }

  private static Types.LiteralType serialize(LiteralType literalType) {
    return Types.LiteralType.newBuilder().setSimple(serialize(literalType.simpleType())).build();
  }

  @Nullable
  private static Types.SimpleType serialize(@Nullable SimpleType simpleType) {
    if (simpleType == null) {
      return null;
    }

    switch (simpleType) {
      case INTEGER:
        return Types.SimpleType.INTEGER;
      case FLOAT:
        return Types.SimpleType.FLOAT;
      case STRING:
        return Types.SimpleType.STRING;
      case BOOLEAN:
        return Types.SimpleType.BOOLEAN;
      case DATETIME:
        return Types.SimpleType.DATETIME;
      case DURATION:
        return Types.SimpleType.DURATION;
    }

    return Types.SimpleType.UNRECOGNIZED;
  }

  private static Tasks.Container serialize(Container container) {
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

  private static Literals.Binding serialize(Binding input) {
    return Literals.Binding.newBuilder()
        .setVar(input.var_())
        .setBinding(serialize(input.binding()))
        .build();
  }

  private static Literals.BindingData serialize(BindingData binding) {
    return Literals.BindingData.newBuilder().setScalar(serialize(binding.scalar())).build();
  }

  private static Literals.Scalar serialize(@Nullable Scalar scalar) {
    if (scalar == null) {
      return null;
    }

    Primitive primitive = requireNonNull(scalar.primitive(), "Only primitive scalar are supported");
    return Literals.Scalar.newBuilder().setPrimitive(serialize(primitive)).build();
  }

  @VisibleForTesting
  static Literals.Primitive serialize(Primitive primitive) {
    Literals.Primitive.Builder builder = Literals.Primitive.newBuilder();

    switch (primitive.type()) {
      case INTEGER:
        builder.setInteger(primitive.integer());
        break;
      case FLOAT:
        builder.setFloatValue(primitive.float_());
        break;
      case STRING:
        builder.setStringValue(primitive.string());
        break;
      case BOOLEAN:
        builder.setBoolean(primitive.boolean_());
        break;
      case DATETIME:
        Timestamp datetime = primitive.datetime();
        builder.setDatetime(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(datetime.seconds())
                .setNanos(datetime.nanos())
                .build());
        break;
      case DURATION:
        Duration duration = primitive.duration();
        builder.setDuration(
            com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.seconds())
                .setNanos(duration.nanos())
                .build());
        break;
    }
    return builder.build();
  }
}
