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
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import flyteidl.admin.Common;
import flyteidl.admin.ScheduleOuterClass;
import flyteidl.core.Errors;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import flyteidl.core.Types;
import flyteidl.core.Types.SchemaType.SchemaColumn.SchemaColumnType;
import flyteidl.core.Workflow;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.CronSchedule;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.PartialIdentifier;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SchemaType;
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
    if (literal.hasScalar()) {
      return Literal.ofScalar(deserialize(literal.getScalar()));
    }
    if (literal.hasCollection()) {
      return Literal.ofCollection(deserialize(literal.getCollection()));
    }
    if (literal.hasMap()) {
      return Literal.ofMap(deserialize(literal.getMap()));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Literal [%s]", literal));
  }

  static Scalar deserialize(Literals.Scalar scalar) {
    if (scalar.getPrimitive() != null) {
      return Scalar.ofPrimitive(deserialize(scalar.getPrimitive()));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Scalar [%s]", scalar));
  }

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

  static List<Literal> deserialize(Literals.LiteralCollection literalCollection) {
    return literalCollection.getLiteralsList().stream()
        .map(ProtoUtil::deserialize)
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));
  }

  static IdentifierOuterClass.Identifier serialize(PartialIdentifier id) {
    IdentifierOuterClass.ResourceType type = getResourceType(id);

    return IdentifierOuterClass.Identifier.newBuilder()
        .setResourceType(type)
        .setDomain(id.domain())
        .setProject(id.project())
        .setName(id.name())
        .setVersion(id.version())
        .build();
  }

  static IdentifierOuterClass.ResourceType getResourceType(PartialIdentifier id) {
    if (id instanceof LaunchPlanIdentifier) { // if only Java 14 :(
      return IdentifierOuterClass.ResourceType.LAUNCH_PLAN;
    } else if (id instanceof TaskIdentifier || id instanceof PartialTaskIdentifier) {
      return IdentifierOuterClass.ResourceType.TASK;
    } else if (id instanceof WorkflowIdentifier || id instanceof PartialWorkflowIdentifier) {
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

    Tasks.TaskMetadata metadata =
        Tasks.TaskMetadata.newBuilder()
            .setRuntime(runtime)
            .setRetries(serialize(taskTemplate.retries()))
            .build();

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

  private static Literals.RetryStrategy serialize(RetryStrategy retryStrategy) {
    return Literals.RetryStrategy.newBuilder().setRetries(retryStrategy.retries()).build();
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

  @VisibleForTesting
  static Types.LiteralType serialize(LiteralType literalType) {
    Types.LiteralType.Builder builder = Types.LiteralType.newBuilder();
    switch (literalType.getKind()) {
      case SIMPLE_TYPE:
        builder.setSimple(serialize(literalType.simpleType()));
        break;
      case SCHEMA_TYPE:
        builder.setSchema(serialize(literalType.schemaType()));
        break;
      case COLLECTION_TYPE:
        builder.setCollectionType(serialize(literalType.collectionType()));
        break;
      case MAP_VALUE_TYPE:
        builder.setMapValueType(serialize(literalType.mapValueType()));
        break;
      case BLOB_TYPE:
        builder.setBlob(serialize(literalType.blobType()));
        break;
    }
    return builder.build();
  }

  private static Types.SimpleType serialize(SimpleType simpleType) {
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

  private static Types.SchemaType serialize(SchemaType schemaType) {
    Types.SchemaType.Builder builder = Types.SchemaType.newBuilder();
    schemaType.columns().forEach(column -> builder.addColumns(serialize(column)));
    return builder.build();
  }

  private static Types.SchemaType.SchemaColumn serialize(SchemaType.Column schemaColumn) {
    return Types.SchemaType.SchemaColumn.newBuilder()
        .setName(schemaColumn.name())
        .setType(serialize(schemaColumn.type()))
        .build();
  }

  private static Types.SchemaType.SchemaColumn.SchemaColumnType serialize(
      SchemaType.ColumnType columnType) {
    switch (columnType) {
      case INTEGER:
        return SchemaColumnType.INTEGER;
      case FLOAT:
        return SchemaColumnType.FLOAT;
      case STRING:
        return SchemaColumnType.STRING;
      case BOOLEAN:
        return SchemaColumnType.BOOLEAN;
      case DATETIME:
        return SchemaColumnType.DATETIME;
      case DURATION:
        return SchemaColumnType.DURATION;
    }
    return SchemaColumnType.UNRECOGNIZED;
  }

  private static Types.BlobType serialize(BlobType blobType) {
    return Types.BlobType.newBuilder()
        .setFormat(blobType.format())
        .setDimensionality(serialize(blobType.dimensionality()))
        .build();
  }

  private static Types.BlobType.BlobDimensionality serialize(
      BlobType.BlobDimensionality dimensionality) {
    switch (dimensionality) {
      case SINGLE:
        return Types.BlobType.BlobDimensionality.SINGLE;
      case MULTIPART:
        return Types.BlobType.BlobDimensionality.MULTIPART;
    }
    return Types.BlobType.BlobDimensionality.UNRECOGNIZED;
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
        Workflow.Node.newBuilder()
            .setId(node.id())
            .addAllUpstreamNodeIds(node.upstreamNodeIds())
            .setTaskNode(serialize(node.taskNode()));

    node.inputs().forEach(input -> builder.addInputs(serialize(input)));

    return builder.build();
  }

  private static Workflow.TaskNode serialize(TaskNode apiTaskNode) {
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
      case COLLECTION:
        return builder.setCollection(serializeBindingCollection(binding.collection())).build();
      case PROMISE:
        return builder.setPromise(serialize(binding.promise())).build();
      case MAP:
        return builder.setMap(serializeBindingMap(binding.map())).build();
    }

    throw new AssertionError("unexpected BindingData.Kind: " + binding.kind());
  }

  static Types.OutputReference serialize(OutputReference promise) {
    return Types.OutputReference.newBuilder()
        .setNodeId(promise.nodeId())
        .setVar(promise.var())
        .build();
  }

  private static Literals.Scalar serialize(Scalar scalar) {
    switch (scalar.kind()) {
      case PRIMITIVE:
        Primitive primitive = scalar.primitive();
        return Literals.Scalar.newBuilder().setPrimitive(serialize(primitive)).build();
    }

    throw new AssertionError("Unexpected Scalar.Kind: " + scalar.kind());
  }

  private static Literals.BindingDataCollection serializeBindingCollection(
      List<BindingData> collection) {
    Literals.BindingDataCollection.Builder builder = Literals.BindingDataCollection.newBuilder();
    collection.forEach(binding -> builder.addBindings(serialize(binding)));
    return builder.build();
  }

  private static Literals.BindingDataMap serializeBindingMap(Map<String, BindingData> map) {
    Literals.BindingDataMap.Builder builder = Literals.BindingDataMap.newBuilder();
    map.forEach((key, value) -> builder.putBindings(key, serialize(value)));
    return builder.build();
  }

  private static Literals.LiteralCollection serialize(List<Literal> literals) {
    Literals.LiteralCollection.Builder builder = Literals.LiteralCollection.newBuilder();
    literals.forEach(literal -> builder.addLiterals(serialize(literal)));
    return builder.build();
  }

  static Literals.LiteralMap serialize(Map<String, Literal> literals) {
    Literals.LiteralMap.Builder builder = Literals.LiteralMap.newBuilder();
    literals.forEach((name, literal) -> builder.putLiterals(name, serialize(literal)));
    return builder.build();
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

  static Literals.Literal serialize(Literal value) {
    Literals.Literal.Builder builder = Literals.Literal.newBuilder();

    switch (value.kind()) {
      case SCALAR:
        builder.setScalar(serialize(value.scalar()));
        return builder.build();
      case COLLECTION:
        builder.setCollection(serialize(value.collection()));
        return builder.build();
      case MAP:
        builder.setMap(serialize(value.map()));
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

    return TaskIdentifier.builder()
        .domain(id.getDomain())
        .project(id.getProject())
        .name(id.getName())
        .version(id.getVersion())
        .build();
  }

  static WorkflowIdentifier deserializeWorkflowId(IdentifierOuterClass.Identifier id) {
    Preconditions.checkArgument(
        id.getResourceType() == IdentifierOuterClass.ResourceType.WORKFLOW,
        "isn't ResourceType.WORKFLOW, got [%s]",
        id.getResourceType());

    return WorkflowIdentifier.builder()
        .project(id.getProject())
        .domain(id.getDomain())
        .name(id.getName())
        .version(id.getVersion())
        .build();
  }

  static Errors.ContainerError serializeThrowable(Throwable e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));

    return Errors.ContainerError.newBuilder()
        .setCode("SYSTEM:Unknown")
        .setKind(Errors.ContainerError.Kind.NON_RECOVERABLE)
        .setMessage(sw.toString())
        .build();
  }

  static Errors.ContainerError.Kind serialize(ContainerError.Kind kind) {
    switch (kind) {
      case RECOVERABLE:
        return Errors.ContainerError.Kind.RECOVERABLE;
      case NON_RECOVERABLE:
        return Errors.ContainerError.Kind.NON_RECOVERABLE;
    }

    throw new AssertionError("unexpected ContainerError.Kind: " + kind);
  }

  static Errors.ContainerError serializeContainerError(ContainerError error) {
    return Errors.ContainerError.newBuilder()
        .setCode(error.getCode())
        .setKind(serialize(error.getKind()))
        .setMessage(error.getMessage())
        .build();
  }

  static ScheduleOuterClass.Schedule serialize(CronSchedule cronSchedule) {
    String schedule = cronSchedule.schedule();
    String offset = cronSchedule.offset();
    ScheduleOuterClass.CronSchedule.Builder builder =
        ScheduleOuterClass.CronSchedule.newBuilder().setSchedule(schedule);

    if (offset != null) {
      builder.setOffset(offset);
    }

    return ScheduleOuterClass.Schedule.newBuilder().setCronSchedule(builder.build()).build();
  }
}
