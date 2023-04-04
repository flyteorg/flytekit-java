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
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowNode.Reference;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.flytekit.SdkRemoteLaunchPlan;
import org.flyte.flytekit.SdkRemoteTask;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.localengine.ExecutionContext;
import org.flyte.localengine.LocalEngine;

/** Flyte executor for testing purposes. */
@AutoValue
public abstract class SdkTestingExecutor {

  abstract Map<String, Literal> fixedInputs();

  abstract Map<String, LiteralType> fixedInputTypes();

  abstract Map<String, TestingRunnableTask<?, ?>> taskTestDoubles();

  abstract Map<String, TestingRunnableLaunchPlan<?, ?>> launchPlanTestDoubles();

  abstract SdkWorkflow<?, ?> workflow();

  abstract Map<String, WorkflowTemplate> workflowTemplates();

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SdkTestingExecutor of(SdkWorkflow<?, ?> workflow) {
    ServiceLoader<SdkRunnableTask<?, ?>> taskLoader =
        (ServiceLoader) ServiceLoader.load(SdkRunnableTask.class);

    List<SdkRunnableTask<?, ?>> tasks = new ArrayList<>();
    taskLoader.iterator().forEachRemaining(tasks::add);

    ServiceLoader<SdkWorkflow<?, ?>> workflowLoader =
        (ServiceLoader) ServiceLoader.load(SdkWorkflow.class);
    List<SdkWorkflow<?, ?>> workflows = new ArrayList<>();
    workflowLoader.iterator().forEachRemaining(workflows::add);

    return SdkTestingExecutor.of(workflow, tasks, workflows);
  }

  @Deprecated
  public static SdkTestingExecutor of(
      SdkWorkflow<?, ?> workflow, List<SdkRunnableTask<?, ?>> tasks) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    ServiceLoader<SdkWorkflow<?, ?>> workflowLoader =
        (ServiceLoader) ServiceLoader.load(SdkWorkflow.class);
    List<SdkWorkflow<?, ?>> workflows = new ArrayList<>();
    workflowLoader.iterator().forEachRemaining(workflows::add);

    return SdkTestingExecutor.of(workflow, tasks, workflows);
  }

  public static SdkTestingExecutor of(
      SdkWorkflow<?, ?> workflow,
      List<SdkRunnableTask<?, ?>> tasks,
      List<SdkWorkflow<?, ?>> workflows) {
    Map<String, TestingRunnableTask<?, ?>> fixedTasks =
        tasks.stream().collect(toMap(SdkRunnableTask::getName, TestingRunnableTask::create));

    Map<String, WorkflowTemplate> workflowTemplateMap =
        workflows.stream().collect(toMap(SdkWorkflow::getName, SdkWorkflow::toIdlTemplate));

    return SdkTestingExecutor.builder()
        .workflow(workflow)
        .workflowTemplates(workflowTemplateMap)
        .fixedInputs(emptyMap())
        .fixedInputTypes(emptyMap())
        .taskTestDoubles(fixedTasks)
        .launchPlanTestDoubles(emptyMap())
        .build();
  }

  public static class UnusedMockException extends IllegalArgumentException {
    UnusedMockException(String message) {
      super(message);
    }
  }

  @AutoValue
  public abstract static class Result {
    abstract Map<String, Literal> literalMap();

    abstract Map<String, LiteralType> literalTypeMap();

    static Result create(Map<String, Literal> literalMap, Map<String, LiteralType> literalTypeMap) {
      return new AutoValue_SdkTestingExecutor_Result(literalMap, literalTypeMap);
    }

    public boolean getBooleanOutput(String name) {
      return getOutput(name, LiteralTypes.BOOLEAN).scalar().primitive().booleanValue();
    }

    public double getFloatOutput(String name) {
      return getOutput(name, LiteralTypes.FLOAT).scalar().primitive().floatValue();
    }

    public Duration getDurationOutput(String name) {
      return getOutput(name, LiteralTypes.DURATION).scalar().primitive().duration();
    }

    public Instant getDatetimeOutput(String name) {
      return getOutput(name, LiteralTypes.DATETIME).scalar().primitive().datetime();
    }

    public long getIntegerOutput(String name) {
      return getOutput(name, LiteralTypes.INTEGER).scalar().primitive().integerValue();
    }

    public String getStringOutput(String name) {
      return getOutput(name, LiteralTypes.STRING).scalar().primitive().stringValue();
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
    WorkflowTemplate workflowTemplate = workflow().toIdlTemplate();
    checkInputsInFixedInputs(workflowTemplate);
    checkTestDoublesForNodes(workflowTemplate);

    Map<String, Literal> outputLiteralMap =
        new LocalEngine(
                ExecutionContext.builder()
                    .runnableTasks(unmodifiableMap(taskTestDoubles()))
                    .workflowTemplates(unmodifiableMap(workflowTemplates()))
                    .runnableLaunchPlans(unmodifiableMap(launchPlanTestDoubles()))
                    .build())
            .compileAndExecute(workflowTemplate, unmodifiableMap(fixedInputs()));

    Map<String, LiteralType> outputLiteralTypeMap =
        workflowTemplate.interface_().outputs().entrySet().stream()
            .collect(toMap(Map.Entry::getKey, x -> x.getValue().literalType()));

    launchPlanTestDoubles()
        .forEach(
            (key, value) ->
                value.fixedOutputs.forEach(
                    (in, mockedOutput) -> {
                      if (mockedOutput.unused()) {
                        throw new UnusedMockException(
                            String.format(
                                "unused launchplan mock name: [%s], input: [%s], output: [%s]",
                                key, in, mockedOutput));
                      }
                    }));
    taskTestDoubles()
        .forEach(
            (key, value) ->
                value.fixedOutputs.forEach(
                    (in, mockedOutput) -> {
                      if (mockedOutput.unused()) {
                        throw new UnusedMockException(
                            String.format(
                                "unused task mock name: [%s], input: [%s], output: [%s]",
                                key, in, mockedOutput));
                      }
                    }));

    return Result.create(outputLiteralMap, outputLiteralTypeMap);
  }

  private void checkInputsInFixedInputs(WorkflowTemplate template) {
    template
        .interface_()
        .inputs()
        .forEach(
            (inputName, inputVar) -> {
              LiteralType inputType = inputVar.literalType();

              LiteralType fixedInputType = fixedInputTypes().get(inputName);

              checkArgument(
                  fixedInputType != null,
                  "Fixed input [%s] (of type %s) isn't defined, use SdkTestingExecutor#withFixedInput",
                  inputName,
                  LiteralTypes.toPrettyString(inputType));

              checkArgument(
                  fixedInputType.equals(inputType),
                  "Fixed input [%s] (of type %s) doesn't match expected type %s",
                  inputName,
                  LiteralTypes.toPrettyString(fixedInputType),
                  LiteralTypes.toPrettyString(inputType));
            });
  }

  private void checkTestDoublesForNodes(WorkflowTemplate template) {
    for (Node node : template.nodes()) {
      if (node.taskNode() != null) {
        checkTestDoubleForTaskNode(node.taskNode());
      } else if (node.workflowNode() != null) {
        checkTestDoubleForWorkflowNode(node.workflowNode());
      }
    }
  }

  private void checkTestDoubleForTaskNode(TaskNode taskNode) {
    String taskName = taskNode.referenceId().name();

    checkArgument(
        taskTestDoubles().containsKey(taskName),
        "Can't execute remote task [%s], "
            + "use SdkTestingExecutor#withTaskOutput or SdkTestingExecutor#withTask "
            + "to provide a test double",
        taskName);
  }

  private void checkTestDoubleForWorkflowNode(WorkflowNode workflowNode) {
    Reference reference = workflowNode.reference();
    switch (reference.kind()) {
      case LAUNCH_PLAN_REF:
        String launchPlanName = reference.launchPlanRef().name();
        TestingRunnableLaunchPlan<?, ?> launchPlan = launchPlanTestDoubles().get(launchPlanName);

        checkArgument(
            launchPlan != null,
            "Can't execute remote launch plan "
                + "[%s], use SdkTestingExecutor#withLaunchPlanOutput or "
                + "SdkTestingExecutor#withLaunchPlan to provide a test double",
            launchPlanName);
        return;

      case SUB_WORKFLOW_REF:
        String subWorkflowName = reference.subWorkflowRef().name();
        WorkflowTemplate subWorkflowTemplate = workflowTemplates().get(subWorkflowName);

        checkArgument(
            subWorkflowTemplate != null, "Can't expand sub workflow [%s]", subWorkflowName);

        checkTestDoublesForNodes(subWorkflowTemplate);
    }
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

    return toBuilder().putFixedTask(task.name(), fixedTask.withFixedOutput(input, output)).build();
  }

  public <InputT, OutputT> SdkTestingExecutor withLaunchPlanOutput(
      SdkRemoteLaunchPlan<InputT, OutputT> launchPlan, InputT input, OutputT output) {
    TestingRunnableLaunchPlan<InputT, OutputT> runnableLaunchPlan =
        getRunnableLaunchPlanOrDefault(
            launchPlan.name(), launchPlan.inputs(), launchPlan.outputs());

    return toBuilder()
        .putLaunchPlan(launchPlan.name(), runnableLaunchPlan.withFixedOutput(input, output))
        .build();
  }

  public <InputT, OutputT> SdkTestingExecutor withLaunchPlan(
      SdkRemoteLaunchPlan<InputT, OutputT> launchPlan, Function<InputT, OutputT> runFn) {
    TestingRunnableLaunchPlan<InputT, OutputT> launchPlanTestDouble =
        getRunnableLaunchPlanOrDefault(
            launchPlan.name(), launchPlan.inputs(), launchPlan.outputs());

    return toBuilder()
        .putLaunchPlan(launchPlanTestDouble.getName(), launchPlanTestDouble.withRunFn(runFn))
        .build();
  }

  public <InputT, OutputT> SdkTestingExecutor withTask(
      SdkRunnableTask<InputT, OutputT> task, Function<InputT, OutputT> runFn) {
    TestingRunnableTask<InputT, OutputT> fixedTask =
        getFixedTaskOrDefault(task.getName(), task.getInputType(), task.getOutputType());

    return toBuilder().putFixedTask(task.getName(), fixedTask.withRunFn(runFn)).build();
  }

  public <InputT, OutputT> SdkTestingExecutor withWorkflowOutput(
      SdkWorkflow<InputT, OutputT> workflow,
      SdkType<InputT> inputType,
      InputT input,
      SdkType<OutputT> outputType,
      OutputT output) {
    verifyInputOutputMatchesWorkflowInterface(workflow, inputType, outputType);

    // replace workflow
    SdkWorkflow<InputT, OutputT> mockWorkflow =
        new TestingWorkflow<>(inputType, outputType, output);

    return toBuilder()
        .putWorkflowTemplate(workflow.getName(), mockWorkflow.toIdlTemplate())
        .build();
  }

  private static <InputT, OutputT> void verifyInputOutputMatchesWorkflowInterface(
      SdkWorkflow<InputT, OutputT> workflow,
      SdkType<InputT> inputType,
      SdkType<OutputT> outputType) {
    TypedInterface intf = workflow.toIdlTemplate().interface_();

    verifyVariablesMatches("Input", intf.inputs(), inputType.getVariableMap());
    verifyVariablesMatches("Output", intf.outputs(), outputType.getVariableMap());
  }

  private static void verifyVariablesMatches(
      String type, Map<String, Variable> actualVariables, Map<String, Variable> variables) {
    if (!actualVariables.equals(variables)) {
      throw new IllegalArgumentException(
          String.format(
              "%s type %s doesn't match expected type %s",
              type, toPrettyString(variables), toPrettyString(actualVariables)));
    }
  }

  static String toPrettyString(Map<String, Variable> variableMap) {
    return variableMap.entrySet().stream()
        .map(
            e ->
                String.format(
                    "%s=%s with description=%s",
                    e.getKey(),
                    LiteralTypes.toPrettyString(e.getValue().literalType()),
                    e.getValue().description()))
        .collect(Collectors.joining(", ", "{ ", " }"));
  }

  private <InputT, OutputT> TestingRunnableTask<InputT, OutputT> getFixedTaskOrDefault(
      String name, SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    @SuppressWarnings({"unchecked"})
    TestingRunnableTask<InputT, OutputT> fixedTask =
        (TestingRunnableTask<InputT, OutputT>) taskTestDoubles().get(name);

    return Objects.requireNonNullElseGet(
        fixedTask, () -> TestingRunnableTask.create(name, inputType, outputType));
  }

  private <InputT, OutputT>
      TestingRunnableLaunchPlan<InputT, OutputT> getRunnableLaunchPlanOrDefault(
          String name, SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    @SuppressWarnings({"unchecked"})
    TestingRunnableLaunchPlan<InputT, OutputT> launchPlantTestDouble =
        (TestingRunnableLaunchPlan<InputT, OutputT>) launchPlanTestDoubles().get(name);

    return Objects.requireNonNullElseGet(
        launchPlantTestDouble, () -> TestingRunnableLaunchPlan.create(name, inputType, outputType));
  }

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_SdkTestingExecutor.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder fixedInputs(Map<String, Literal> fixedInputs);

    abstract Builder fixedInputTypes(Map<String, LiteralType> fixedInputTypes);

    abstract Builder taskTestDoubles(Map<String, TestingRunnableTask<?, ?>> taskTestDoubles);

    abstract Builder workflow(SdkWorkflow<?, ?> workflow);

    abstract Builder launchPlanTestDoubles(
        Map<String, TestingRunnableLaunchPlan<?, ?>> launchPlanTestDoubles);

    abstract Builder workflowTemplates(Map<String, WorkflowTemplate> workflowTemplates);

    abstract Map<String, Literal> fixedInputs();

    abstract Map<String, LiteralType> fixedInputTypes();

    abstract Map<String, TestingRunnableTask<?, ?>> taskTestDoubles();

    abstract Map<String, WorkflowTemplate> workflowTemplates();

    abstract Map<String, TestingRunnableLaunchPlan<?, ?>> launchPlanTestDoubles();

    Builder putFixedInput(String key, Literal value, LiteralType type) {
      Map<String, Literal> newFixedInputs = new HashMap<>(fixedInputs());
      newFixedInputs.put(key, value);

      Map<String, LiteralType> newFixedInputTypeMap = new HashMap<>(fixedInputTypes());
      newFixedInputTypeMap.put(key, type);

      return fixedInputs(unmodifiableMap(newFixedInputs))
          .fixedInputTypes(unmodifiableMap(newFixedInputTypeMap));
    }

    Builder putFixedTask(String name, TestingRunnableTask<?, ?> fn) {
      Map<String, TestingRunnableTask<?, ?>> newTaskTestDoubles = new HashMap<>(taskTestDoubles());
      newTaskTestDoubles.put(name, fn);

      return taskTestDoubles(newTaskTestDoubles);
    }

    Builder putWorkflowTemplate(String name, WorkflowTemplate template) {
      Map<String, WorkflowTemplate> newWorkflowTemplates = new HashMap<>(workflowTemplates());
      newWorkflowTemplates.put(name, template);

      return workflowTemplates(newWorkflowTemplates);
    }

    Builder putLaunchPlan(String name, TestingRunnableLaunchPlan<?, ?> fn) {
      Map<String, TestingRunnableLaunchPlan<?, ?>> newLaunchPlanTestDoubles =
          new HashMap<>(launchPlanTestDoubles());
      newLaunchPlanTestDoubles.put(name, fn);

      return launchPlanTestDoubles(newLaunchPlanTestDoubles);
    }

    abstract SdkTestingExecutor build();
  }
}
