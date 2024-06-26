/*
 * Copyright 2020-2023 Flyte Authors.
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

import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Parameter;
import org.flyte.api.v1.Variable;

/** Flyte launch plan, identifiers, inputs and schedule. */
@SuppressWarnings("PreferJavaTimeOverload")
@AutoValue
public abstract class SdkLaunchPlan {

  /** Returns launch plan name. */
  public abstract String name();

  /** Returns referenced workflow's project. */
  @Nullable
  public abstract String workflowProject();

  /** Returns referenced workflow's domain. */
  @Nullable
  public abstract String workflowDomain();

  /** Returns referenced workflow's name. */
  public abstract String workflowName();

  /** Returns referenced workflow's version. */
  @Nullable
  public abstract String workflowVersion();

  /**
   * Expected Input interface of the referenced workflow. This map gets populated when call to
   * {@code #withFixedInput} or {@code #withDefaultInput} are made.This map is used to validate the
   * type of any fixed or default inputs.
   *
   * @return Workflow's expected input interface (names and types)
   */
  public abstract Map<String, LiteralType> workflowInputTypeMap();

  /**
   * Returns the workflow inputs that were fixed in the launch plan. Fixed inputs cannot be changed
   * at execution time
   */
  public abstract Map<String, Literal> fixedInputs();

  /**
   * Returns the workflow inputs that with default inputs in the launch plan. Default inputs can be
   * changed at execution time
   */
  public abstract Map<String, Parameter> defaultInputs();

  /** Returns the cron schedule of the launch plan. */
  @Nullable
  public abstract SdkCronSchedule cronSchedule();

  /** Returns the max parallelism of the launch plan. */
  @Nullable
  public abstract Optional<Integer> maxParallelism();

  /**
   * Creates a launch plan for specified {@link SdkLaunchPlan} with default naming, no inputs and no
   * schedule. The default launch plan name is {@link SdkWorkflow#getName()}. New name, inputs and
   * schedule could be added by calling {@link #withName(String)}, {@link #withFixedInput(String,
   * long)} (and friends respectively) and {@link #withCronSchedule(SdkCronSchedule)}.
   *
   * @param workflow Workflow to be reference by new {@link SdkLaunchPlan}.
   * @return the created {@link SdkLaunchPlan}.
   */
  public static SdkLaunchPlan of(SdkWorkflow<?, ?> workflow) {
    SdkWorkflowBuilder wfBuilder = new SdkWorkflowBuilder();
    workflow.expand(wfBuilder);
    return builder()
        .name(workflow.getName())
        .workflowName(workflow.getName())
        .workflowInputTypeMap(
            toWorkflowInputTypeMap(wfBuilder.getInputs(), in -> in.type().getLiteralType()))
        .build();
  }

  private static <T> Map<String, LiteralType> toWorkflowInputTypeMap(
      Map<String, T> inputMap, Function<T, LiteralType> toLiteralTypeFn) {
    return inputMap.keySet().stream()
        .collect(toUnmodifiableMap(identity(), name -> toLiteralTypeFn.apply(inputMap.get(name))));
  }

  /**
   * Returns this launch plan with a new name.
   *
   * @param newName new name for the launch plan
   * @return the new launch plan
   */
  public SdkLaunchPlan withName(String newName) {
    return toBuilder().name(newName).build();
  }

  /**
   * Returns this launch plan with a new fixed input.
   *
   * @param inputName name for the input
   * @param value value to set
   * @return the new launch plan
   */
  public SdkLaunchPlan withFixedInput(String inputName, long value) {
    return withFixedInputs0(
        Map.of(inputName, Literals.ofInteger(value)), Map.of(inputName, LiteralTypes.INTEGER));
  }

  /**
   * Returns this launch plan with a new fixed input.
   *
   * @param inputName name for the input
   * @param value value to set
   * @return the new launch plan
   */
  public SdkLaunchPlan withFixedInput(String inputName, double value) {
    return withFixedInputs0(
        Map.of(inputName, Literals.ofFloat(value)), Map.of(inputName, LiteralTypes.FLOAT));
  }

  /**
   * Returns this launch plan with a new fixed input.
   *
   * @param inputName name for the input
   * @param value value to set
   * @return the new launch plan
   */
  public SdkLaunchPlan withFixedInput(String inputName, String value) {
    return withFixedInputs0(
        Map.of(inputName, Literals.ofString(value)), Map.of(inputName, LiteralTypes.STRING));
  }

  /**
   * Returns this launch plan with a new fixed input.
   *
   * @param inputName name for the input
   * @param value value to set
   * @return the new launch plan
   */
  public SdkLaunchPlan withFixedInput(String inputName, boolean value) {
    return withFixedInputs0(
        Map.of(inputName, Literals.ofBoolean(value)), Map.of(inputName, LiteralTypes.BOOLEAN));
  }

  /**
   * Returns this launch plan with a new fixed input.
   *
   * @param inputName name for the input
   * @param value value to set
   * @return the new launch plan
   */
  public SdkLaunchPlan withFixedInput(String inputName, Instant value) {
    return withFixedInputs0(
        Map.of(inputName, Literals.ofDatetime(value)), Map.of(inputName, LiteralTypes.DATETIME));
  }

  /**
   * Returns this launch plan with a new fixed input.
   *
   * @param inputName name for the input
   * @param value value to set
   * @return the new launch plan
   */
  public SdkLaunchPlan withFixedInput(String inputName, Duration value) {
    return withFixedInputs0(
        Map.of(inputName, Literals.ofDuration(value)), Map.of(inputName, LiteralTypes.DURATION));
  }

  /**
   * Returns this launch plan with fixed inputs.
   *
   * @param type for the inputs
   * @param value inputs to fix
   * @return the new launch plan
   */
  public <T> SdkLaunchPlan withFixedInputs(SdkType<T> type, T value) {
    return withFixedInputs0(
        type.toLiteralMap(value),
        toWorkflowInputTypeMap(type.getVariableMap(), Variable::literalType));
  }

  /**
   * Returns this launch plan with cron schedule.
   *
   * @param cronSchedule cron schedule to set
   * @return the new launch plan
   */
  public SdkLaunchPlan withCronSchedule(SdkCronSchedule cronSchedule) {
    return toBuilder().cronSchedule(cronSchedule).build();
  }

  private SdkLaunchPlan withFixedInputs0(
      Map<String, Literal> newInputs, Map<String, LiteralType> newInputTypes) {

    verifyNonEmptyWorkflowInput(newInputTypes, "fixed");
    verifyMatchedInput(newInputTypes, "fixed");

    Map<String, Literal> newCompleteInputs = mergeInputs(fixedInputs(), newInputs, "fixed");

    return toBuilder().fixedInputs(newCompleteInputs).build();
  }

  /**
   * Returns this launch plan with default input.
   *
   * @param inputName name for the input
   * @param value inputs to set default
   * @return the new launch plan
   */
  public SdkLaunchPlan withDefaultInput(String inputName, long value) {
    return withDefaultInputs0(
        Map.of(inputName, createParameter(LiteralTypes.INTEGER, Literals.ofInteger(value))));
  }

  /**
   * Returns this launch plan with default input.
   *
   * @param inputName name for the input
   * @param value inputs to set default
   * @return the new launch plan
   */
  public SdkLaunchPlan withDefaultInput(String inputName, double value) {
    return withDefaultInputs0(
        Map.of(inputName, createParameter(LiteralTypes.FLOAT, Literals.ofFloat(value))));
  }

  /**
   * Returns this launch plan with default input.
   *
   * @param inputName name for the input
   * @param value inputs to set default
   * @return the new launch plan
   */
  public SdkLaunchPlan withDefaultInput(String inputName, String value) {
    return withDefaultInputs0(
        Map.of(inputName, createParameter(LiteralTypes.STRING, Literals.ofString(value))));
  }

  /**
   * Returns this launch plan with default input.
   *
   * @param inputName name for the input
   * @param value inputs to set default
   * @return the new launch plan
   */
  public SdkLaunchPlan withDefaultInput(String inputName, boolean value) {
    return withDefaultInputs0(
        Map.of(inputName, createParameter(LiteralTypes.BOOLEAN, Literals.ofBoolean(value))));
  }

  /**
   * Returns this launch plan with default input.
   *
   * @param inputName name for the input
   * @param value inputs to set default
   * @return the new launch plan
   */
  public SdkLaunchPlan withDefaultInput(String inputName, Instant value) {
    return withDefaultInputs0(
        Map.of(inputName, createParameter(LiteralTypes.DATETIME, Literals.ofDatetime(value))));
  }

  /**
   * Returns this launch plan with default input.
   *
   * @param inputName name for the input
   * @param value inputs to set default
   * @return the new launch plan
   */
  public SdkLaunchPlan withDefaultInput(String inputName, Duration value) {
    return withDefaultInputs0(
        Map.of(inputName, createParameter(LiteralTypes.DURATION, Literals.ofDuration(value))));
  }

  private Parameter createParameter(LiteralType literalType, Literal literal) {
    return Parameter.create(
        Variable.builder().description("").literalType(literalType).build(), literal);
  }

  /**
   * Returns this launch plan with default inputs.
   *
   * @param type for the inputs
   * @param value inputs to set default
   * @return the new launch plan
   */
  public <T> SdkLaunchPlan withDefaultInput(SdkType<T> type, T value) {
    Map<String, LiteralType> literalTypeMap =
        type.getVariableMap().entrySet().stream()
            .collect(toMap(Map.Entry::getKey, x -> x.getValue().literalType()));

    Map<String, Literal> literalMap = type.toLiteralMap(value);

    return withDefaultInputs0(
        literalTypeMap.entrySet().stream()
            .collect(
                toMap(
                    Map.Entry::getKey,
                    v -> createParameter(v.getValue(), literalMap.get(v.getKey())))));
  }

  /**
   *
   * @param maxParallelism Optional Integer for the max parallelism (cannot be negative).
   *                       Default Value: Empty, it will default to what's set in the Flyte Platform.
   *                       0: It will try to use as much as allowed.
   * @return the new launch plan
   */
  public SdkLaunchPlan withMaxParallelism(Optional<Integer> maxParallelism) {
    return withMaxParallelism0(maxParallelism);
  }



  private SdkLaunchPlan withDefaultInputs0(Map<String, Parameter> newDefaultInputs) {

    verifyNonEmptyWorkflowInput(newDefaultInputs, "default");

    verifyMatchedInput(
        newDefaultInputs.entrySet().stream()
            .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().var().literalType())),
        "default");
    Map<String, Parameter> newCompleteDefaultInputs =
        mergeInputs(defaultInputs(), newDefaultInputs, "default");

    return toBuilder().defaultInputs(newCompleteDefaultInputs).build();
  }

  private SdkLaunchPlan withMaxParallelism0(Optional<Integer> maxParallelism) {
    if (maxParallelism.isPresent() && maxParallelism.get() < 0) {
      String message =
          String.format(
              "invalid max parallelism %s, expected a positive integer", maxParallelism.get());
      throw new IllegalArgumentException(message);
    }

    return toBuilder().maxParallelism(maxParallelism).build();
  }

  private <T> Map<String, T> mergeInputs(
      Map<String, T> oldInputs, Map<String, T> newInputs, String inputType) {
    Map<String, T> newCompleteInputs = new LinkedHashMap<>(oldInputs);

    newInputs.forEach(
        (inputName, value) -> {
          T previous = newCompleteInputs.put(inputName, value);
          if (previous != null) {
            throw new IllegalArgumentException(
                String.format(
                    "Duplicate %s input [%s] on launch plan [%s], values: [%s] [%s]",
                    inputType, inputName, name(), value, previous));
          }
        });
    return newCompleteInputs;
  }

  private <T> void verifyNonEmptyWorkflowInput(Map<String, T> newInputTypes, String inputType) {
    if (workflowInputTypeMap().isEmpty() && !newInputTypes.isEmpty()) {
      String message =
          String.format(
              "invalid launch plan %s inputs, expected none but found %s",
              inputType, newInputTypes.size());
      throw new IllegalArgumentException(message);
    }
  }

  private void verifyMatchedInput(Map<String, LiteralType> newInputTypes, String inputType) {
    for (Map.Entry<String, LiteralType> lpInputType : newInputTypes.entrySet()) {
      String inputName = lpInputType.getKey();
      LiteralType lpType = lpInputType.getValue();

      LiteralType wfType = workflowInputTypeMap().get(inputName);

      if (wfType == null) {
        String message = String.format("unexpected %s input %s", inputType, inputName);
        throw new IllegalArgumentException(message);
      }
      if (!lpType.equals(wfType)) {
        String message =
            String.format(
                "invalid %s input wrong type %s, expected %s, got %s instead",
                inputType, inputName, wfType, lpType);
        throw new IllegalArgumentException(message);
      }
    }
  }



  static Builder builder() {
    return new AutoValue_SdkLaunchPlan.Builder()
        .fixedInputs(Collections.emptyMap())
        .defaultInputs(Collections.emptyMap())
        .workflowInputTypeMap(Collections.emptyMap())
        .maxParallelism(Optional.empty());
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder name(String name);

    abstract Builder workflowProject(String workflowProject);

    abstract Builder workflowDomain(String workflowDomain);

    abstract Builder workflowName(String workflowName);

    abstract Builder workflowVersion(String workflowVersion);

    abstract Builder fixedInputs(Map<String, Literal> fixedInputs);

    abstract Builder defaultInputs(Map<String, Parameter> defaultInputs);

    abstract Builder cronSchedule(SdkCronSchedule cronSchedule);

    abstract Builder workflowInputTypeMap(Map<String, LiteralType> workflowInputTypeMap);

    abstract Builder maxParallelism(Optional<Integer> maxParallelism);

    abstract SdkLaunchPlan build();
  }
}
