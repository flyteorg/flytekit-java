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
package org.flyte.flytekit.testing;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;
import static org.flyte.flytekit.testing.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.Var;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.flytekit.SdkRemoteTask;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.localengine.LocalEngine;

@AutoValue
public abstract class SdkTestingExecutor {

  abstract Map<String, Literal> fixedInputMap();

  abstract Map<String, LiteralType> fixedInputTypeMap();

  abstract Map<String, TestingRunnableTask<?, ?>> fixedTaskMap();

  abstract SdkWorkflow workflow();

  public static SdkTestingExecutor of(SdkWorkflow workflow) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    ServiceLoader<SdkRunnableTask<?, ?>> loader =
        (ServiceLoader) ServiceLoader.load(SdkRunnableTask.class);

    List<SdkRunnableTask<?, ?>> tasks = new ArrayList<>();
    loader.iterator().forEachRemaining(tasks::add);

    return SdkTestingExecutor.of(workflow, tasks);
  }

  public static SdkTestingExecutor of(SdkWorkflow workflow, List<SdkRunnableTask<?, ?>> tasks) {
    Map<String, TestingRunnableTask<?, ?>> fixedTasks = new HashMap<>();

    for (SdkRunnableTask<?, ?> task : tasks) {
      fixedTasks.put(task.getName(), TestingRunnableTask.create(task));
    }

    return SdkTestingExecutor.builder()
        .workflow(workflow)
        .fixedInputMap(emptyMap())
        .fixedInputTypeMap(emptyMap())
        .fixedTaskMap(fixedTasks)
        .build();
  }

  @AutoValue
  public abstract static class Result {
    abstract Map<String, Literal> literalMap();

    abstract Map<String, LiteralType> literalTypeMap();

    static Result create(Map<String, Literal> literalMap, Map<String, LiteralType> literalTypeMap) {
      return new AutoValue_SdkTestingExecutor_Result(literalMap, literalTypeMap);
    }

    public boolean getBooleanOutput(String name) {
      return getOutput(name, LiteralTypes.BOOLEAN).scalar().primitive().boolean_();
    }

    public double getFloatOutput(String name) {
      return getOutput(name, LiteralTypes.FLOAT).scalar().primitive().float_();
    }

    public Duration getDurationOutput(String name) {
      return getOutput(name, LiteralTypes.DURATION).scalar().primitive().duration();
    }

    public Instant getDatetimeOutput(String name) {
      return getOutput(name, LiteralTypes.DATETIME).scalar().primitive().datetime();
    }

    public long getIntegerOutput(String name) {
      return getOutput(name, LiteralTypes.INTEGER).scalar().primitive().integer();
    }

    public String getStringOutput(String name) {
      return getOutput(name, LiteralTypes.STRING).scalar().primitive().string();
    }

    public <T> T getOutputAs(SdkType<T> sdkType) {
      return sdkType.fromLiteralMap(literalMap());
    }

    private Literal getOutput(String name, LiteralType expectedLiteralType) {
      checkArgument(
          literalTypeMap().containsKey(name),
          "Output [%s] doesn't exist in %s",
          name,
          literalTypeMap().keySet());

      checkArgument(
          literalTypeMap().get(name).equals(expectedLiteralType),
          "Output [%s] (type %s) doesn't match expected type %s",
          name,
          LiteralTypes.toPrettyString(literalTypeMap().get(name)),
          LiteralTypes.toPrettyString(expectedLiteralType));

      return literalMap().get(name);
    }
  }

  public Result execute() {
    TestingSdkWorkflowBuilder builder =
        new TestingSdkWorkflowBuilder(fixedInputMap(), fixedInputTypeMap());
    workflow().expand(builder);

    WorkflowTemplate workflowTemplate = builder.toIdlTemplate();

    for (Node node : workflowTemplate.nodes()) {
      String taskName = node.taskNode().referenceId().name();

      checkArgument(
          fixedTaskMap().containsKey(taskName),
          "Can't execute remote task [%s], "
              + "use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask",
          taskName);
    }

    Map<String, Literal> outputLiteralMap =
        LocalEngine.compileAndExecute(
            workflowTemplate, unmodifiableMap(fixedTaskMap()), fixedInputMap());

    Map<String, LiteralType> outputLiteralTypeMap =
        workflowTemplate.interface_().outputs().entrySet().stream()
            .collect(toMap(Map.Entry::getKey, x -> x.getValue().literalType()));

    return Result.create(outputLiteralMap, outputLiteralTypeMap);
  }

  public SdkTestingExecutor withFixedInput(String inputName, boolean value) {
    return toBuilder()
        .putFixedInput(inputName, Literals.ofBoolean(value), LiteralTypes.BOOLEAN)
        .build();
  }

  public SdkTestingExecutor withFixedInput(String inputName, Instant value) {
    return toBuilder()
        .putFixedInput(inputName, Literals.ofDatetime(value), LiteralTypes.DATETIME)
        .build();
  }

  public SdkTestingExecutor withFixedInput(String inputName, Duration value) {
    return toBuilder()
        .putFixedInput(inputName, Literals.ofDuration(value), LiteralTypes.DURATION)
        .build();
  }

  public SdkTestingExecutor withFixedInput(String inputName, double value) {
    return toBuilder()
        .putFixedInput(inputName, Literals.ofFloat(value), LiteralTypes.FLOAT)
        .build();
  }

  public SdkTestingExecutor withFixedInput(String inputName, long value) {
    return toBuilder()
        .putFixedInput(inputName, Literals.ofInteger(value), LiteralTypes.INTEGER)
        .build();
  }

  public SdkTestingExecutor withFixedInput(String inputName, String value) {
    return toBuilder()
        .putFixedInput(inputName, Literals.ofString(value), LiteralTypes.STRING)
        .build();
  }

  public <T> SdkTestingExecutor withFixedInputs(SdkType<T> type, T value) {
    Map<String, Variable> variableMap = type.getVariableMap();
    @Var Builder builder = toBuilder();

    for (Map.Entry<String, Literal> entry : type.toLiteralMap(value).entrySet()) {
      LiteralType literalType = variableMap.get(entry.getKey()).literalType();

      builder = builder.putFixedInput(entry.getKey(), entry.getValue(), literalType);
    }

    return builder.build();
  }

  public <InputT, OutputT> SdkTestingExecutor withTaskOutput(
      SdkRunnableTask<InputT, OutputT> task, InputT input, OutputT output) {
    TestingRunnableTask<InputT, OutputT> fixedTask =
        getFixedTaskOrDefault(task.getName(), task.getInputType(), task.getOutputType());

    return toBuilder()
        .putFixedTask(task.getName(), fixedTask.withFixedOutput(input, output))
        .build();
  }

  public <InputT, OutputT> SdkTestingExecutor withTaskOutput(
      SdkRemoteTask<InputT, OutputT> task, InputT input, OutputT output) {
    TestingRunnableTask<InputT, OutputT> fixedTask =
        getFixedTaskOrDefault(task.name(), task.inputs(), task.outputs());

    // FIXME Careful reader would have noticed that here we ignore project, domain and version
    // it's because LocalEngine doesn't support it yet. We only correctly function
    // when task names are unique.

    return toBuilder().putFixedTask(task.name(), fixedTask.withFixedOutput(input, output)).build();
  }

  public <InputT, OutputT> SdkTestingExecutor withTask(
      SdkRunnableTask<InputT, OutputT> task, Function<InputT, OutputT> runFn) {
    TestingRunnableTask<InputT, OutputT> fixedTask =
        getFixedTaskOrDefault(task.getName(), task.getInputType(), task.getOutputType());

    return toBuilder().putFixedTask(task.getName(), fixedTask.withRunFn(runFn)).build();
  }

  private <InputT, OutputT> TestingRunnableTask<InputT, OutputT> getFixedTaskOrDefault(
      String name, SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    @SuppressWarnings({"unchecked"})
    TestingRunnableTask<InputT, OutputT> fixedTask =
        (TestingRunnableTask<InputT, OutputT>) fixedTaskMap().get(name);

    if (fixedTask == null) {
      return TestingRunnableTask.create(name, inputType, outputType);
    } else {
      return fixedTask;
    }
  }

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_SdkTestingExecutor.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder fixedInputMap(Map<String, Literal> fixedInputMap);

    abstract Builder fixedInputTypeMap(Map<String, LiteralType> fixedInputTypeMap);

    abstract Builder fixedTaskMap(Map<String, TestingRunnableTask<?, ?>> fixedTaskMap);

    abstract Map<String, Literal> fixedInputMap();

    abstract Map<String, LiteralType> fixedInputTypeMap();

    abstract Map<String, TestingRunnableTask<?, ?>> fixedTaskMap();

    abstract Builder workflow(SdkWorkflow workflow);

    Builder putFixedInput(String key, Literal value, LiteralType type) {
      Map<String, Literal> newFixedInputMap = new HashMap<>(fixedInputMap());
      newFixedInputMap.put(key, value);

      Map<String, LiteralType> newFixedInputTypeMap = new HashMap<>(fixedInputTypeMap());
      newFixedInputTypeMap.put(key, type);

      return fixedInputMap(unmodifiableMap(newFixedInputMap))
          .fixedInputTypeMap(unmodifiableMap(newFixedInputTypeMap));
    }

    Builder putFixedTask(String name, TestingRunnableTask<?, ?> fn) {
      Map<String, TestingRunnableTask<?, ?>> newFixedTaskMap = new HashMap<>(fixedTaskMap());
      newFixedTaskMap.put(name, fn);

      return fixedTaskMap(newFixedTaskMap);
    }

    abstract SdkTestingExecutor build();
  }
}
