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
package org.flyte.flytekit;

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Literal;

@SuppressWarnings("PreferJavaTimeOverload")
public class SdkLaunchPlan {

  private final String name;
  private final String workflowProject;
  private final String workflowDomain;
  private final String workflowName;
  private final String workflowVersion;
  private final Map<String, Literal> fixedInputs;

  private SdkLaunchPlan(
      String name,
      String workflowProject,
      String workflowDomain,
      String workflowName,
      String workflowVersion,
      Map<String, Literal> fixedInputs) {
    this.name = requireNonNull(name, "name");
    this.workflowProject = workflowProject;
    this.workflowDomain = workflowDomain;
    this.workflowName = requireNonNull(workflowName, "workflowName");
    this.workflowVersion = workflowVersion;
    this.fixedInputs = requireNonNull(fixedInputs, "fixedInputs");
  }

  /**
   * Creates a launch plan for specified {@link SdkLaunchPlan} with default naming and no inputs.
   * The default launch plan name is {@link SdkWorkflow#getName()}. New name and inputs could be
   * added by calling {@link #withName(String)} and {@link #withFixedInput(String, long)} and
   * friends respectively.
   *
   * @param workflow Workflow to be reference by new {@link SdkLaunchPlan}.
   * @return the created {@link SdkLaunchPlan}.
   */
  public static SdkLaunchPlan of(SdkWorkflow workflow) {
    return new SdkLaunchPlan(
        /* name= */ workflow.getName(),
        /* workflowProject= */ null,
        /* workflowDomain= */ null,
        /* workflowName= */ workflow.getName(),
        /* workflowVersion= */ null,
        Collections.emptyMap());
  }

  public SdkLaunchPlan withName(String newName) {
    return new SdkLaunchPlan(
        /* name= */ requireNonNull(newName, "Launch plan name should not be null"),
        /* workflowProject= */ workflowProject,
        /* workflowDomain= */ workflowDomain,
        /* workflowName= */ workflowName,
        /* workflowVersion= */ workflowVersion,
        fixedInputs);
  }

  public SdkLaunchPlan withFixedInput(String inputName, long value) {
    return withFixedInputs0(singletonMap(inputName, Literals.ofInteger(value)));
  }

  public SdkLaunchPlan withFixedInput(String inputName, double value) {
    return withFixedInputs0(singletonMap(inputName, Literals.ofFloat(value)));
  }

  public SdkLaunchPlan withFixedInput(String inputName, String value) {
    return withFixedInputs0(singletonMap(inputName, Literals.ofString(value)));
  }

  public SdkLaunchPlan withFixedInput(String inputName, boolean value) {
    return withFixedInputs0(singletonMap(inputName, Literals.ofBoolean(value)));
  }

  public SdkLaunchPlan withFixedInput(String inputName, Instant value) {
    return withFixedInputs0(singletonMap(inputName, Literals.ofDatetime(value)));
  }

  public SdkLaunchPlan withFixedInput(String inputName, Duration value) {
    return withFixedInputs0(singletonMap(inputName, Literals.ofDuration(value)));
  }

  public <T> SdkLaunchPlan withFixedInputs(SdkType<T> type, T value) {
    return withFixedInputs0(type.toLiteralMap(value));
  }

  private SdkLaunchPlan withFixedInputs0(Map<String, Literal> newInputs) {
    // TODO: validate that the workflow's interface contains an input with the given name and that
    // the types matches
    Map<String, Literal> newCompleteInputs = new LinkedHashMap<>(this.fixedInputs);
    newInputs.forEach(
        (inputName, value) -> {
          Literal previous = newCompleteInputs.put(inputName, value);
          if (previous != null) {
            throw new IllegalArgumentException(
                String.format(
                    "Duplicate input [%s] on launch plan [%s], values: [%s] [%s]",
                    inputName, name, value, previous));
          }
        });

    return new SdkLaunchPlan(
        /* name= */ name,
        /* workflowProject= */ workflowProject,
        /* workflowDomain= */ workflowDomain,
        /* workflowName= */ workflowName,
        /* workflowVersion= */ workflowVersion,
        newCompleteInputs);
  }

  public String getName() {
    return this.name;
  }

  @Nullable
  public String getWorkflowProject() {
    return this.workflowProject;
  }

  @Nullable
  public String getWorkflowDomain() {
    return this.workflowDomain;
  }

  public String getWorkflowName() {
    return this.workflowName;
  }

  @Nullable
  public String getWorkflowVersion() {
    return this.workflowVersion;
  }

  Map<String, Literal> getFixedInputs() {
    Map<String, Literal> copy = new LinkedHashMap<>(fixedInputs);
    return Collections.unmodifiableMap(copy);
  }
}
