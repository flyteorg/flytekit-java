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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import flyteidl.admin.Common;
import flyteidl.core.Errors;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import flyteidl.core.Types;
import flyteidl.core.Workflow;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.Identifier;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;

/** Utility to serialize between flytekit-api and flyteidl proto. */
@SuppressWarnings("PreferJavaTimeOverload")
class ProtoUtil {
  static final String TASK_TYPE = "java-task";
  static final String RUNTIME_FLAVOR = "java";
  static final String RUNTIME_VERSION = "0.0.1";

  private ProtoUtil() {
    throw new UnsupportedOperationException();
  }

  static Map<String, Literal> deserialize(Literals.LiteralMap literalMap) {
    Map<String, Literal> inputs = new HashMap<>();

    for (Map.Entry<String, Literals.Literal> entry : literalMap.getLiteralsMap().entrySet()) {
      inputs.put(entry.getKey(), deserialize(entry.getValue()));
    }

    return inputs;
  }

  static Literal deserialize(Literals.Literal literal) {
    if (literal.getScalar() != null) {
      return Literal.of(deserialize(literal.getScalar()));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Literal [%s]", literal));
  }

  static Scalar deserialize(Literals.Scalar scalar) {
    if (scalar.getPrimitive() != null) {
      return Scalar.of(deserialize(scalar.getPrimitive()));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Scalar [%s]", scalar));
  }

  @SuppressWarnings("fallthrough")
  static Primitive deserialize(Literals.Primitive primitive) {
    switch (primitive.getValueCase()) {
      case INTEGER:
        return Primitive.ofInteger(primitive.getInteger());
      case FLOAT_VALUE:
        return Primitive.ofFloat(primitive.getFloatValue());
      case STRING_VALUE:
        return Primitive.ofString(primitive.getStringValue());
      case BOOLEAN:
        return Primitive.ofBoolean(primitive.getBoolean());
      case DATETIME:
        com.google.protobuf.Timestamp datetime = primitive.getDatetime();
        return Primitive.ofDatetime(
            Instant.ofEpochSecond(datetime.getSeconds(), datetime.getNanos()));
      case DURATION:
        com.google.protobuf.Duration duration = primitive.getDuration();
        return Primitive.ofDuration(Duration.ofSeconds(duration.getSeconds(), duration.getNanos()));
      case VALUE_NOT_SET:
        // fallthrough
    }

    throw new UnsupportedOperationException(String.format("Unsupported Primitive [%s]", primitive));
  }

  static IdentifierOuterClass.Identifier serialize(Identifier id) {
    IdentifierOuterClass.ResourceType type = getResourceType(id);

    return IdentifierOuterClass.Identifier.newBuilder()
        .setResourceType(type)
        .setDomain(id.domain())
        .setProject(id.project())
        .setName(id.name())
        .setVersion(id.version())
        .build();
  }

  static IdentifierOuterClass.ResourceType getResourceType(Identifier id) {
    if (id instanceof LaunchPlanIdentifier) { // if only Java 14 :(
      return IdentifierOuterClass.ResourceType.LAUNCH_PLAN;
    } else if (id instanceof TaskIdentifier) {
      return IdentifierOuterClass.ResourceType.TASK;
    } else if (id instanceof WorkflowIdentifier) {
      return IdentifierOuterClass.ResourceType.WORKFLOW;
    }

    throw new IllegalArgumentException("Unknown Identifier type: " + id.getClass());
  }

  static Tasks.TaskTemplate serialize(TaskTemplate taskTemplate) {
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
    return Interface.TypedInterface.newBuilder()
        .setInputs(serializeVariableMap(interface_.inputs()))
        .setOutputs(serializeVariableMap(interface_.outputs()))
        .build();
  }

  private static Interface.VariableMap serializeVariableMap(Map<String, Variable> inputs) {
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
    Tasks.Container.Builder builder =
        Tasks.Container.newBuilder()
            .setImage(container.image())
            .addAllCommand(container.command())
            .addAllArgs(container.args());

    container.env().forEach(pair -> builder.addEnv(serialize(pair)));

    return builder.build();
  }

  private static Literals.KeyValuePair serialize(KeyValuePair pair) {
    Literals.KeyValuePair.Builder builder = Literals.KeyValuePair.newBuilder();

    builder.setKey(pair.key());

    if (pair.value() != null) {
      builder.setValue(pair.value());
    }

    return builder.build();
  }

  public static Workflow.WorkflowTemplate serialize(WorkflowTemplate template) {
    Workflow.WorkflowTemplate.Builder builder =
        Workflow.WorkflowTemplate.newBuilder()
            .setMetadata(serialize(template.metadata()))
            .setInterface(serialize(template.interface_()));

    template.outputs().forEach(output -> builder.addOutputs(serialize(output)));
    template.nodes().forEach(node -> builder.addNodes(serialize(node)));

    return builder.build();
  }

  private static Workflow.WorkflowMetadata serialize(
      @SuppressWarnings("UnusedVariable") WorkflowMetadata metadata) {
    return Workflow.WorkflowMetadata.newBuilder().build();
  }

  private static Workflow.Node serialize(Node node) {

    Workflow.Node.Builder builder =
        Workflow.Node.newBuilder().setId(node.id()).addAllUpstreamNodeIds(node.upstreamNodeIds());

    Workflow.TaskNode taskNode = serialize(node.taskNode());
    if (taskNode != null) {
      builder.setTaskNode(taskNode);
    }

    node.inputs().forEach(input -> builder.addInputs(serialize(input)));

    return builder.build();
  }

  private static Workflow.TaskNode serialize(@Nullable TaskNode apiTaskNode) {
    if (apiTaskNode == null) {
      return null;
    }

    TaskIdentifier taskIdentifier =
        TaskIdentifier.builder()
            .domain(apiTaskNode.referenceId().domain())
            .project(apiTaskNode.referenceId().project())
            .name(apiTaskNode.referenceId().name())
            .version(apiTaskNode.referenceId().version())
            .build();

    return Workflow.TaskNode.newBuilder().setReferenceId(serialize(taskIdentifier)).build();
  }

  private static Literals.Binding serialize(Binding binding) {
    return Literals.Binding.newBuilder()
        .setVar(binding.var_())
        .setBinding(serialize(binding.binding()))
        .build();
  }

  static Literals.BindingData serialize(BindingData binding) {
    Literals.BindingData.Builder builder = Literals.BindingData.newBuilder();

    switch (binding.kind()) {
      case SCALAR:
        return builder.setScalar(serialize(binding.scalar())).build();
      case PROMISE:
        return builder.setPromise(serialize(binding.promise())).build();
    }

    throw new AssertionError("unexpected BindingData.Kind: " + binding.kind());
  }

  static Types.OutputReference serialize(@Nullable OutputReference promise) {
    if (promise == null) {
      return null;
    }

    return Types.OutputReference.newBuilder()
        .setNodeId(promise.nodeId())
        .setVar(promise.var())
        .build();
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
        Instant datetime = primitive.datetime();
        builder.setDatetime(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(datetime.getEpochSecond())
                .setNanos(datetime.getNano())
                .build());
        break;
      case DURATION:
        Duration duration = primitive.duration();
        builder.setDuration(
            com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build());
        break;
    }

    return builder.build();
  }

  static Literals.LiteralMap serializeLiteralMap(Map<String, Literal> outputs) {
    Literals.LiteralMap.Builder builder = Literals.LiteralMap.newBuilder();

    outputs.forEach((key, value) -> builder.putLiterals(key, serialize(value)));

    return builder.build();
  }

  private static Literals.Literal serialize(Literal value) {
    Literals.Literal.Builder builder = Literals.Literal.newBuilder();

    switch (value.kind()) {
      case SCALAR:
        builder.setScalar(serialize(value.scalar()));

        return builder.build();
    }

    throw new AssertionError("unexpected Literal.Kind: " + value.kind());
  }

  static Common.NamedEntityIdentifier serialize(NamedEntityIdentifier taskId) {
    return Common.NamedEntityIdentifier.newBuilder()
        .setDomain(taskId.domain())
        .setProject(taskId.project())
        .setName(taskId.name())
        .build();
  }

  static TaskIdentifier deserializeTaskId(IdentifierOuterClass.Identifier id) {
    Preconditions.checkArgument(
        id.getResourceType() == IdentifierOuterClass.ResourceType.TASK,
        "isn't ResourceType.TASK, got [%s]",
        id.getResourceType());

    /* domain= */
    /* project= */
    /* name= */
    /* version= */ return TaskIdentifier.builder()
        .domain(id.getDomain())
        .project(id.getProject())
        .name(id.getName())
        .version(id.getVersion())
        .build();
  }

  static Errors.ErrorDocument serialize(Throwable e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));

    return Errors.ErrorDocument.newBuilder()
        .setError(
            Errors.ContainerError.newBuilder()
                .setCode("SYSTEM:Unknown")
                .setKind(Errors.ContainerError.Kind.NON_RECOVERABLE)
                .setMessage(sw.toString())
                .build())
        .build();
  }
}
