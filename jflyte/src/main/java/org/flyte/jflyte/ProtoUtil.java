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
package org.flyte.jflyte;

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static org.flyte.jflyte.MoreCollectors.mapValues;
import static org.flyte.jflyte.MoreCollectors.toUnmodifiableList;
import static org.flyte.jflyte.MoreCollectors.toUnmodifiableMap;
import static org.flyte.jflyte.QuantityUtil.isValidQuantity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import flyteidl.admin.Common;
import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.admin.ScheduleOuterClass;
import flyteidl.admin.TaskOuterClass;
import flyteidl.admin.WorkflowOuterClass;
import flyteidl.core.Condition;
import flyteidl.core.DynamicJob;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.BlobMetadata;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.ConjunctionExpression;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.CronSchedule;
import org.flyte.api.v1.DynamicJobSpec;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.NodeError;
import org.flyte.api.v1.NodeMetadata;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Parameter;
import org.flyte.api.v1.PartialIdentifier;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SchemaType;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;

/** Utility to serialize between flytekit-api and flyteidl proto. */
@SuppressWarnings("PreferJavaTimeOverload")
class ProtoUtil {
  // Datetime is a proto Timestamp and therefore must conform to the Timestamp range. See:
  // https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp
  private static final Instant DATETIME_MIN =
      ISO_DATE_TIME.parse("0001-01-01T00:00:00Z", Instant::from);
  private static final Instant DATETIME_MAX =
      ISO_DATE_TIME.parse("9999-12-31T23:59:59.999999999Z", Instant::from);
  private static final Pattern DNS_1123_REGEXP = Pattern.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$");

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
    switch (literal.getValueCase()) {
      case SCALAR:
        return Literal.ofScalar(deserialize(literal.getScalar()));
      case COLLECTION:
        return Literal.ofCollection(deserialize(literal.getCollection()));
      case MAP:
        return Literal.ofMap(deserialize(literal.getMap()));
      case VALUE_NOT_SET:
        // fallthrough
    }

    throw new UnsupportedOperationException(String.format("Unsupported Literal [%s]", literal));
  }

  static Scalar deserialize(Literals.Scalar scalar) {
    switch (scalar.getValueCase()) {
      case PRIMITIVE:
        return Scalar.ofPrimitive(deserialize(scalar.getPrimitive()));

      case GENERIC:
        return Scalar.ofGeneric(deserialize(scalar.getGeneric()));

      case BLOB:
        return Scalar.ofBlob(deserialize(scalar.getBlob()));

      case BINARY:
      case ERROR:
      case NONE_TYPE:
      case SCHEMA:
      case STRUCTURED_DATASET:
      case UNION:
        // TODO unsupported

      case VALUE_NOT_SET:
        // fallthrough
    }

    throw new UnsupportedOperationException(String.format("Unsupported Scalar [%s]", scalar));
  }

  static Primitive deserialize(Literals.Primitive primitive) {
    switch (primitive.getValueCase()) {
      case INTEGER:
        return Primitive.ofIntegerValue(primitive.getInteger());
      case FLOAT_VALUE:
        return Primitive.ofFloatValue(primitive.getFloatValue());
      case STRING_VALUE:
        return Primitive.ofStringValue(primitive.getStringValue());
      case BOOLEAN:
        return Primitive.ofBooleanValue(primitive.getBoolean());
      case DATETIME:
        com.google.protobuf.Timestamp datetime = primitive.getDatetime();
        return Primitive.ofDatetime(
            Instant.ofEpochSecond(datetime.getSeconds(), datetime.getNanos()));
      case DURATION:
        com.google.protobuf.Duration duration = primitive.getDuration();
        return Primitive.ofDuration(Duration.ofSeconds(duration.getSeconds(), duration.getNanos()));
      case VALUE_NOT_SET:
        throw new UnsupportedOperationException(
            String.format("Unsupported Primitive [%s]", primitive));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Primitive [%s]", primitive));
  }

  static Blob deserialize(Literals.Blob blob) {
    BlobType type =
        BlobType.builder()
            .format(blob.getMetadata().getType().getFormat())
            .dimensionality(deserialize(blob.getMetadata().getType().getDimensionality()))
            .build();

    BlobMetadata metadata = BlobMetadata.builder().type(type).build();

    return Blob.builder().uri(blob.getUri()).metadata(metadata).build();
  }

  static BlobType.BlobDimensionality deserialize(Types.BlobType.BlobDimensionality dimensionality) {
    switch (dimensionality) {
      case SINGLE:
        return BlobType.BlobDimensionality.SINGLE;
      case MULTIPART:
        return BlobType.BlobDimensionality.MULTIPART;
      case UNRECOGNIZED:
        // fallthrough
    }

    throw new UnsupportedOperationException(
        String.format("Unsupported BlobDimensionality [%s]", dimensionality));
  }

  static List<Literal> deserialize(Literals.LiteralCollection literalCollection) {
    return literalCollection.getLiteralsList().stream()
        .map(ProtoUtil::deserialize)
        .collect(toUnmodifiableList());
  }

  private static Struct deserialize(com.google.protobuf.Struct struct) {
    Map<String, Struct.Value> fields = mapValues(struct.getFieldsMap(), ProtoUtil::deserialize);

    return Struct.of(fields);
  }

  private static Struct.Value deserialize(com.google.protobuf.Value value) {
    switch (value.getKindCase()) {
      case BOOL_VALUE:
        return Struct.Value.ofBoolValue(value.getBoolValue());

      case STRING_VALUE:
        return Struct.Value.ofStringValue(value.getStringValue());

      case NUMBER_VALUE:
        return Struct.Value.ofNumberValue(value.getNumberValue());

      case NULL_VALUE:
        return Struct.Value.ofNullValue();

      case LIST_VALUE:
        List<Struct.Value> valuesList =
            value.getListValue().getValuesList().stream()
                .map(ProtoUtil::deserialize)
                .collect(toUnmodifiableList());

        return Struct.Value.ofListValue(valuesList);

      case STRUCT_VALUE:
        Struct struct = deserialize(value.getStructValue());

        return Struct.Value.ofStructValue(struct);

      case KIND_NOT_SET:
        throw new UnsupportedOperationException(String.format("Unsupported Value [%s]", value));
    }

    throw new UnsupportedOperationException(String.format("Unsupported Value [%s]", value));
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
    if (id instanceof LaunchPlanIdentifier
        || id instanceof PartialLaunchPlanIdentifier) { // if only Java 14 :(
      return IdentifierOuterClass.ResourceType.LAUNCH_PLAN;
    } else if (id instanceof TaskIdentifier || id instanceof PartialTaskIdentifier) {
      return IdentifierOuterClass.ResourceType.TASK;
    } else if (id instanceof WorkflowIdentifier || id instanceof PartialWorkflowIdentifier) {
      return IdentifierOuterClass.ResourceType.WORKFLOW;
    }

    throw new IllegalArgumentException("Unknown Identifier type: " + id.getClass());
  }

  static Tasks.TaskTemplate serialize(TaskIdentifier id, TaskTemplate taskTemplate) {
    return serialize(taskTemplate).toBuilder().setId(serialize(id)).build();
  }

  static TaskOuterClass.TaskSpec serialize(TaskSpec spec) {
    return TaskOuterClass.TaskSpec.newBuilder().setTemplate(serialize(spec.taskTemplate())).build();
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
            .setDiscoverable(taskTemplate.discoverable())
            .setDiscoveryVersion(taskTemplate.discoveryVersion())
            .setCacheSerializable(taskTemplate.cacheSerializable())
            .build();

    Container container =
        requireNonNull(
            taskTemplate.container(), "Only container based task templates are supported");

    return Tasks.TaskTemplate.newBuilder()
        .setContainer(serialize(container))
        .setMetadata(metadata)
        .setInterface(serialize(taskTemplate.interface_()))
        .setType(taskTemplate.type())
        .setCustom(serializeStruct(taskTemplate.custom()))
        .build();
  }

  static TaskTemplate deserialize(Tasks.TaskTemplate proto) {
    return TaskTemplate.builder()
        .container(proto.hasContainer() ? deserialize(proto.getContainer()) : null)
        .custom(deserialize(proto.getCustom()))
        .interface_(deserialize(proto.getInterface()))
        .retries(deserialize(proto.getMetadata().getRetries()))
        .type(proto.getType())
        .discoverable(proto.getMetadata().getDiscoverable())
        .discoveryVersion(proto.getMetadata().getDiscoveryVersion())
        .cacheSerializable(proto.getMetadata().getCacheSerializable())
        .build();
  }

  private static Literals.RetryStrategy serialize(RetryStrategy retryStrategy) {
    return Literals.RetryStrategy.newBuilder().setRetries(retryStrategy.retries()).build();
  }

  private static RetryStrategy deserialize(Literals.RetryStrategy proto) {
    return RetryStrategy.builder().retries(proto.getRetries()).build();
  }

  private static Interface.TypedInterface serialize(TypedInterface interface_) {
    return Interface.TypedInterface.newBuilder()
        .setInputs(serializeVariableMap(interface_.inputs()))
        .setOutputs(serializeVariableMap(interface_.outputs()))
        .build();
  }

  private static TypedInterface deserialize(Interface.TypedInterface proto) {
    return TypedInterface.builder()
        .inputs(deserialize(proto.getInputs()))
        .outputs(deserialize(proto.getOutputs()))
        .build();
  }

  private static Interface.VariableMap serializeVariableMap(Map<String, Variable> inputs) {
    Interface.VariableMap.Builder builder = Interface.VariableMap.newBuilder();

    inputs.forEach((key, value) -> builder.putVariables(key, serialize(value)));

    return builder.build();
  }

  private static Map<String, Variable> deserialize(Interface.VariableMap proto) {
    ImmutableMap.Builder<String, Variable> builder = ImmutableMap.builder();

    proto.getVariablesMap().forEach((key, value) -> builder.put(key, deserialize(value)));

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

  private static Variable deserialize(Interface.Variable proto) {
    return Variable.builder()
        .literalType(deserialize(proto.getType()))
        .description(proto.getDescription())
        .build();
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

  static LiteralType deserialize(Types.LiteralType proto) {
    switch (proto.getTypeCase()) {
      case SIMPLE:
        return LiteralType.ofSimpleType(deserialize(proto.getSimple()));
      case BLOB:
        return LiteralType.ofBlobType(deserialize(proto.getBlob()));
      case COLLECTION_TYPE:
        return LiteralType.ofCollectionType(deserialize(proto.getCollectionType()));
      case SCHEMA:
        return LiteralType.ofSchemaType(deserialize(proto.getSchema()));
      case MAP_VALUE_TYPE:
        return LiteralType.ofMapValueType(deserialize(proto.getMapValueType()));
      case ENUM_TYPE:
        throw new IllegalArgumentException("Type ENUM not supported"); // TODO
      case STRUCTURED_DATASET_TYPE:
        throw new IllegalArgumentException("Type STRUCTURED_DATASET not supported"); // TODO
      case UNION_TYPE:
        throw new IllegalArgumentException("Type UNION not supported"); // TODO
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("Can't deserialize LiteralType because TYPE_NOT_SET");
    }

    throw new AssertionError("Unexpected LiteralType: " + proto);
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
      case STRUCT:
        return Types.SimpleType.STRUCT;
    }

    return Types.SimpleType.UNRECOGNIZED;
  }

  private static SimpleType deserialize(Types.SimpleType proto) {
    switch (proto) {
      case INTEGER:
        return SimpleType.INTEGER;
      case FLOAT:
        return SimpleType.FLOAT;
      case STRING:
        return SimpleType.STRING;
      case BOOLEAN:
        return SimpleType.BOOLEAN;
      case DATETIME:
        return SimpleType.DATETIME;
      case DURATION:
        return SimpleType.DURATION;
      case STRUCT:
        return SimpleType.STRUCT;
      case ERROR:
      case BINARY:
      case NONE:
        throw new IllegalArgumentException("Unsupported SimpleType: " + proto);

      case UNRECOGNIZED:
        throw new IllegalArgumentException("Can't deserialize LiteralType because UNRECOGNIZED");
    }

    throw new IllegalArgumentException("Unexpected SimpleType: " + proto);
  }

  private static Types.SchemaType serialize(SchemaType schemaType) {
    Types.SchemaType.Builder builder = Types.SchemaType.newBuilder();
    schemaType.columns().forEach(column -> builder.addColumns(serialize(column)));
    return builder.build();
  }

  private static SchemaType deserialize(Types.SchemaType proto) {
    ImmutableList.Builder<SchemaType.Column> columns = ImmutableList.builder();

    proto.getColumnsList().forEach(column -> columns.add(deserialize(column)));

    return SchemaType.builder().columns(columns.build()).build();
  }

  private static Types.SchemaType.SchemaColumn serialize(SchemaType.Column schemaColumn) {
    return Types.SchemaType.SchemaColumn.newBuilder()
        .setName(schemaColumn.name())
        .setType(serialize(schemaColumn.type()))
        .build();
  }

  private static SchemaType.Column deserialize(Types.SchemaType.SchemaColumn proto) {
    return SchemaType.Column.builder()
        .name(proto.getName())
        .type(deserialize(proto.getType()))
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

  private static SchemaType.ColumnType deserialize(
      Types.SchemaType.SchemaColumn.SchemaColumnType proto) {
    switch (proto) {
      case INTEGER:
        return SchemaType.ColumnType.INTEGER;
      case FLOAT:
        return SchemaType.ColumnType.FLOAT;
      case STRING:
        return SchemaType.ColumnType.STRING;
      case BOOLEAN:
        return SchemaType.ColumnType.BOOLEAN;
      case DATETIME:
        return SchemaType.ColumnType.DATETIME;
      case DURATION:
        return SchemaType.ColumnType.DURATION;
      case UNRECOGNIZED:
        throw new IllegalArgumentException(
            "Can't deserialize SchemaColumnType because UNRECOGNIZED");
    }

    throw new IllegalArgumentException("Unexpected SchemaColumnType: " + proto);
  }

  private static Types.BlobType serialize(BlobType blobType) {
    return Types.BlobType.newBuilder()
        .setFormat(blobType.format())
        .setDimensionality(serialize(blobType.dimensionality()))
        .build();
  }

  private static BlobType deserialize(Types.BlobType blobType) {
    return BlobType.builder()
        .format(blobType.getFormat())
        .dimensionality(deserialize(blobType.getDimensionality()))
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

  @VisibleForTesting
  static Tasks.Container serialize(Container container) {
    Tasks.Container.Builder builder =
        Tasks.Container.newBuilder()
            .setImage(container.image())
            .addAllCommand(container.command())
            .addAllArgs(container.args());

    container.env().forEach(pair -> builder.addEnv(serialize(pair)));
    Resources resources = container.resources();
    if (resources != null) {
      builder.setResources(serialize(resources));
    }
    return builder.build();
  }

  private static Tasks.Resources serialize(Resources resources) {
    Tasks.Resources.Builder builder = Tasks.Resources.newBuilder();
    if (resources.requests() != null) {
      populateBuilder("requests", resources.requests(), builder::addRequests);
    }
    if (resources.limits() != null) {
      populateBuilder("limits", resources.limits(), builder::addLimits);
    }
    return builder.build();
  }

  private static void populateBuilder(
      String type,
      Map<Resources.ResourceName, String> resourceMap,
      Consumer<Tasks.Resources.ResourceEntry> builder) {
    resourceMap.forEach(
        (name, value) -> {
          if (!isValidQuantity(value)) {
            throw new IllegalArgumentException(
                String.format("Resource %s [%s] has invalid quantity: %s", type, name, value));
          }
          builder.accept(
              Tasks.Resources.ResourceEntry.newBuilder()
                  .setName(serialize(name))
                  .setValue(value)
                  .build());
        });
  }

  private static Tasks.Resources.ResourceName serialize(Resources.ResourceName name) {
    switch (name) {
      case UNKNOWN:
        return Tasks.Resources.ResourceName.UNKNOWN;
      case CPU:
        return Tasks.Resources.ResourceName.CPU;
      case GPU:
        return Tasks.Resources.ResourceName.GPU;
      case MEMORY:
        return Tasks.Resources.ResourceName.MEMORY;
      case STORAGE:
        return Tasks.Resources.ResourceName.STORAGE;
      case EPHEMERAL_STORAGE:
        return Tasks.Resources.ResourceName.EPHEMERAL_STORAGE;
    }
    throw new AssertionError("Unexpected Resources.ResourceName: " + name);
  }

  private static Container deserialize(Tasks.Container container) {
    ImmutableList.Builder<KeyValuePair> env = ImmutableList.builder();

    container.getEnvList().forEach(keyValuePair -> env.add(deserialize(keyValuePair)));

    return Container.builder()
        .args(ImmutableList.copyOf(container.getArgsList()))
        .command(ImmutableList.copyOf(container.getCommandList()))
        .env(env.build())
        .image(container.getImage())
        .build();
  }

  private static Literals.KeyValuePair serialize(KeyValuePair pair) {
    Literals.KeyValuePair.Builder builder = Literals.KeyValuePair.newBuilder();

    builder.setKey(pair.key());

    if (pair.value() != null) {
      builder.setValue(pair.value());
    }

    return builder.build();
  }

  private static KeyValuePair deserialize(Literals.KeyValuePair pair) {
    return KeyValuePair.of(pair.getKey(), pair.getValue());
  }

  static LaunchPlanOuterClass.LaunchPlanSpec serialize(LaunchPlan launchPlan) {
    LaunchPlanOuterClass.LaunchPlanSpec.Builder specBuilder =
        LaunchPlanOuterClass.LaunchPlanSpec.newBuilder()
            .setWorkflowId(ProtoUtil.serialize(launchPlan.workflowId()))
            .setFixedInputs(ProtoUtil.serialize(launchPlan.fixedInputs()))
            .setDefaultInputs(ProtoUtil.serializeParameters(launchPlan.defaultInputs()));

    if (launchPlan.cronSchedule() != null) {
      ScheduleOuterClass.Schedule schedule = ProtoUtil.serialize(launchPlan.cronSchedule());
      specBuilder.setEntityMetadata(
          LaunchPlanOuterClass.LaunchPlanMetadata.newBuilder().setSchedule(schedule).build());
    }

    return specBuilder.build();
  }

  static WorkflowOuterClass.WorkflowSpec serialize(WorkflowIdentifier id, WorkflowSpec spec) {
    WorkflowOuterClass.WorkflowSpec.Builder builder =
        WorkflowOuterClass.WorkflowSpec.newBuilder()
            .setTemplate(serialize(id, spec.workflowTemplate()));

    spec.subWorkflows()
        .forEach(
            (subWorkflowId, subWorkflow) ->
                builder.addSubWorkflows(serialize(subWorkflowId, subWorkflow)));

    return builder.build();
  }

  static Workflow.WorkflowTemplate serialize(WorkflowIdentifier id, WorkflowTemplate template) {
    return serialize(template).toBuilder().setId(serialize(id)).build();
  }

  static Workflow.WorkflowTemplate serialize(WorkflowTemplate template) {
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

  @VisibleForTesting
  static Workflow.Node serialize(Node node) {
    if (!isDNS1123Label(node.id())) {
      throw new IllegalArgumentException(
          String.format("Node id [%s] must conform to DNS 1123 naming format", node.id()));
    }

    Workflow.Node.Builder builder =
        Workflow.Node.newBuilder().setId(node.id()).addAllUpstreamNodeIds(node.upstreamNodeIds());

    if (node.metadata() != null) {
      builder.setMetadata(serialize(node.metadata()));
    }

    if (node.taskNode() != null) {
      builder.setTaskNode(serialize(node.taskNode()));
    }

    if (node.branchNode() != null) {
      builder.setBranchNode(serialize(node.branchNode()));
    }

    if (node.workflowNode() != null) {
      builder.setWorkflowNode(serialize(node.workflowNode()));
    }

    node.inputs().forEach(input -> builder.addInputs(serialize(input)));

    return builder.build();
  }

  private static boolean isDNS1123Label(String label) {
    return DNS_1123_REGEXP.matcher(label).matches();
  }

  private static Workflow.NodeMetadata serialize(NodeMetadata metadata) {
    Workflow.NodeMetadata.Builder builder = Workflow.NodeMetadata.newBuilder();

    if (metadata.name() != null) {
      builder.setName(metadata.name());
    }
    if (metadata.timeout() != null) {
      builder.setTimeout(serialize(metadata.timeout()));
    }
    if (metadata.retries() != null) {
      builder.setRetries(serialize(metadata.retries()));
    }

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

  @VisibleForTesting
  static Workflow.WorkflowNode serialize(WorkflowNode workflowNode) {
    Workflow.WorkflowNode.Builder builder = Workflow.WorkflowNode.newBuilder();

    switch (workflowNode.reference().kind()) {
      case LAUNCH_PLAN_REF:
        return builder
            .setLaunchplanRef(serialize(workflowNode.reference().launchPlanRef()))
            .build();
      case SUB_WORKFLOW_REF:
        return builder
            .setSubWorkflowRef(serialize(workflowNode.reference().subWorkflowRef()))
            .build();
    }

    throw new AssertionError(
        "Unexpected WorkflowNode.Reference.Kind: " + workflowNode.reference().kind());
  }

  @VisibleForTesting
  static Workflow.BranchNode serialize(BranchNode branchNode) {
    return Workflow.BranchNode.newBuilder().setIfElse(serialize(branchNode.ifElse())).build();
  }

  static Workflow.IfElseBlock serialize(IfElseBlock ifElse) {
    Workflow.IfElseBlock.Builder builder = Workflow.IfElseBlock.newBuilder();

    builder.setCase(serialize(ifElse.case_()));

    if (ifElse.elseNode() != null) {
      builder.setElseNode(serialize(ifElse.elseNode()));
    }

    if (ifElse.error() != null) {
      builder.setError(serialize(ifElse.error()));
    }

    ifElse.other().forEach(other -> builder.addOther(serialize(other)));

    return builder.build();
  }

  private static Workflow.IfBlock serialize(IfBlock ifBlock) {
    return Workflow.IfBlock.newBuilder()
        .setCondition(serialize(ifBlock.condition()))
        .setThenNode(serialize(ifBlock.thenNode()))
        .build();
  }

  @VisibleForTesting
  static Condition.BooleanExpression serialize(BooleanExpression condition) {
    switch (condition.kind()) {
      case COMPARISON:
        return Condition.BooleanExpression.newBuilder()
            .setComparison(serialize(condition.comparison()))
            .build();
      case CONJUNCTION:
        return Condition.BooleanExpression.newBuilder()
            .setConjunction(serialize(condition.conjunction()))
            .build();
    }

    throw new AssertionError("Unexpected BooleanExpression.Kind: " + condition.kind());
  }

  private static Condition.ConjunctionExpression serialize(ConjunctionExpression conjunction) {
    return Condition.ConjunctionExpression.newBuilder()
        .setLeftExpression(serialize(conjunction.leftExpression()))
        .setRightExpression(serialize(conjunction.rightExpression()))
        .setOperator(serialize(conjunction.operator()))
        .build();
  }

  private static Condition.ComparisonExpression serialize(ComparisonExpression comparison) {
    return Condition.ComparisonExpression.newBuilder()
        .setLeftValue(serialize(comparison.leftValue()))
        .setRightValue(serialize(comparison.rightValue()))
        .setOperator(serialize(comparison.operator()))
        .build();
  }

  private static Condition.ComparisonExpression.Operator serialize(
      ComparisonExpression.Operator operator) {
    switch (operator) {
      case EQ:
        return Condition.ComparisonExpression.Operator.EQ;
      case GT:
        return Condition.ComparisonExpression.Operator.GT;
      case LT:
        return Condition.ComparisonExpression.Operator.LT;
      case LTE:
        return Condition.ComparisonExpression.Operator.LTE;
      case GTE:
        return Condition.ComparisonExpression.Operator.GTE;
      case NEQ:
        return Condition.ComparisonExpression.Operator.NEQ;
    }

    throw new AssertionError("Unexpected ComparisonExpression.Operator: " + operator);
  }

  private static Condition.Operand serialize(Operand operand) {
    switch (operand.kind()) {
      case VAR:
        return Condition.Operand.newBuilder().setVar(operand.var()).build();
      case PRIMITIVE:
        return Condition.Operand.newBuilder().setPrimitive(serialize(operand.primitive())).build();
    }

    throw new AssertionError("Unexpected Operand.Kind: " + operand.kind());
  }

  private static Condition.ConjunctionExpression.LogicalOperator serialize(
      ConjunctionExpression.LogicalOperator operator) {
    switch (operator) {
      case AND:
        return Condition.ConjunctionExpression.LogicalOperator.AND;
      case OR:
        return Condition.ConjunctionExpression.LogicalOperator.OR;
    }

    throw new AssertionError("Unexpected ConjunctionExpression.LogicalOperator: " + operator);
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

      case GENERIC:
        Struct generic = scalar.generic();

        return Literals.Scalar.newBuilder().setGeneric(serializeStruct(generic)).build();

      case BLOB:
        Blob blob = scalar.blob();

        return Literals.Scalar.newBuilder().setBlob(serialize(blob)).build();
    }

    throw new AssertionError("Unexpected Scalar.Kind: " + scalar.kind());
  }

  private static com.google.protobuf.Struct serializeStruct(Struct struct) {
    com.google.protobuf.Struct.Builder builder = com.google.protobuf.Struct.newBuilder();

    struct.fields().forEach((key, value) -> builder.putFields(key, serializeValue(value)));

    return builder.build();
  }

  private static com.google.protobuf.Value serializeValue(Struct.Value value) {
    switch (value.kind()) {
      case STRING_VALUE:
        return Value.newBuilder().setStringValue(value.stringValue()).build();

      case BOOL_VALUE:
        return Value.newBuilder().setBoolValue(value.boolValue()).build();

      case LIST_VALUE:
        ListValue.Builder listValueBuilder = ListValue.newBuilder();
        value.listValue().forEach(elem -> listValueBuilder.addValues(serializeValue(elem)));

        return Value.newBuilder().setListValue(listValueBuilder.build()).build();

      case NULL_VALUE:
        return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

      case NUMBER_VALUE:
        return Value.newBuilder().setNumberValue(value.numberValue()).build();

      case STRUCT_VALUE:
        return Value.newBuilder().setStructValue(serializeStruct(value.structValue())).build();
    }

    throw new AssertionError("Unexpected Value.Kind: " + value.kind());
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

    switch (primitive.kind()) {
      case INTEGER_VALUE:
        builder.setInteger(primitive.integerValue());
        break;
      case FLOAT_VALUE:
        builder.setFloatValue(primitive.floatValue());
        break;
      case STRING_VALUE:
        builder.setStringValue(primitive.stringValue());
        break;
      case BOOLEAN_VALUE:
        builder.setBoolean(primitive.booleanValue());
        break;
      case DATETIME:
        Instant datetime = primitive.datetime();
        builder.setDatetime(serialize(datetime));
        break;
      case DURATION:
        Duration duration = primitive.duration();
        builder.setDuration(serialize(duration));
        break;
    }

    return builder.build();
  }

  private static Timestamp serialize(Instant datetime) {
    dateTimeRangeCheck(datetime);
    return Timestamp.newBuilder()
        .setSeconds(datetime.getEpochSecond())
        .setNanos(datetime.getNano())
        .build();
  }

  private static com.google.protobuf.Duration serialize(Duration duration) {
    return com.google.protobuf.Duration.newBuilder()
        .setSeconds(duration.getSeconds())
        .setNanos(duration.getNano())
        .build();
  }

  private static void dateTimeRangeCheck(Instant datetime) {
    if (datetime.isBefore(DATETIME_MIN)) {
      throw new IllegalArgumentException(
          String.format(
              "Datetime out of range, minimum allowed value [%s] but was [%s]",
              DATETIME_MIN, datetime));
    }
    if (datetime.isAfter(DATETIME_MAX)) {
      throw new IllegalArgumentException(
          String.format(
              "Datetime out of range, maximum allowed value [%s] but was [%s]",
              DATETIME_MAX, datetime));
    }
  }

  static Literals.Blob serialize(Blob blob) {
    Literals.BlobMetadata metadata =
        Literals.BlobMetadata.newBuilder().setType(serialize(blob.metadata().type())).build();

    return Literals.Blob.newBuilder().setUri(blob.uri()).setMetadata(metadata).build();
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

  static LaunchPlanIdentifier deserializeLaunchPlanId(IdentifierOuterClass.Identifier id) {
    Preconditions.checkArgument(
        id.getResourceType() == IdentifierOuterClass.ResourceType.LAUNCH_PLAN,
        "isn't ResourceType.LAUNCH_PLAN, got [%s]",
        id.getResourceType());

    return LaunchPlanIdentifier.builder()
        .project(id.getProject())
        .domain(id.getDomain())
        .name(id.getName())
        .version(id.getVersion())
        .build();
  }

  static Types.Error serialize(NodeError e) {
    return Types.Error.newBuilder()
        .setMessage(e.message())
        .setFailedNodeId(e.failedNodeId())
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

  static Interface.ParameterMap serializeParameters(Map<String, Parameter> defaultInputs) {
    return Interface.ParameterMap.newBuilder()
        .putAllParameters(
            defaultInputs.entrySet().stream()
                .collect(
                    toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> {
                          Interface.Variable variable = serialize(e.getValue().var());
                          Interface.Parameter.Builder parameterBuilder =
                              Interface.Parameter.newBuilder();

                          if (e.getValue().defaultValue() != null) {
                            parameterBuilder.setDefault(serialize(e.getValue().defaultValue()));
                          } else {
                            parameterBuilder.setRequired(false);
                          }

                          parameterBuilder.setVar(variable);
                          return parameterBuilder.build();
                        })))
        .build();
  }

  static DynamicJob.DynamicJobSpec serialize(DynamicJobSpec dynamicJobSpec) {
    DynamicJob.DynamicJobSpec.Builder builder = DynamicJob.DynamicJobSpec.newBuilder();

    dynamicJobSpec.nodes().forEach(node -> builder.addNodes(serialize(node)));
    dynamicJobSpec.outputs().forEach(binding -> builder.addOutputs(serialize(binding)));
    dynamicJobSpec
        .subWorkflows()
        .forEach(
            (id, workflowTemplate) -> builder.addSubworkflows(serialize(id, workflowTemplate)));
    dynamicJobSpec
        .tasks()
        .forEach((id, taskTemplate) -> builder.addTasks(serialize(id, taskTemplate)));

    return builder.build();
  }
}
