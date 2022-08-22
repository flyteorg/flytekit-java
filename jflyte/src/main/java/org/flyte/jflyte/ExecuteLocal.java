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

import static java.util.Collections.emptyMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.localengine.ExecutionContext;
import org.flyte.localengine.ExecutionListener;
import org.flyte.localengine.LocalEngine;
import org.flyte.localengine.NoopExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Unmatched;

/** Handler for "execute-local" command. */
@Command(name = "execute-local")
public class ExecuteLocal implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecuteLocal.class);

  @Option(
      names = {"--workflow"},
      required = true)
  private String workflowName;

  @Option(
      names = {"-cp", "--classpath"},
      description = "Directory with packaged jars",
      required = false)
  private String packageDir;

  @Unmatched private String[] inputArgs;

  @Override
  public Integer call() {
    Map<String, ClassLoader> modules = ExecuteLocalLoader.loadModules(packageDir);

    Map<String, String> env =
        ImmutableMap.of(
            "FLYTE_INTERNAL_DOMAIN", "development",
            "FLYTE_INTERNAL_VERSION", "test",
            "FLYTE_INTERNAL_PROJECT", "flytetester");

    Map<String, RunnableTask> runnableTasks = ExecuteLocalLoader.loadTasks(modules, env);
    Map<String, DynamicWorkflowTask> dynamicWorkflowTasks =
        emptyMap(); // TODO support dynamic tasks
    Map<String, WorkflowTemplate> workflowTemplates =
        ExecuteLocalLoader.loadWorkflows(modules, env);

    WorkflowTemplate workflow =
        Preconditions.checkNotNull(
            workflowTemplates.get(workflowName), "workflow not found [%s]", workflowName);

    String synopsis = getCustomSynopsis();
    List<String> inputArgsList =
        inputArgs == null ? Collections.emptyList() : Arrays.asList(inputArgs);
    Map<String, Literal> inputs =
        getArgsParser().parseInputs(synopsis, workflow.interface_().inputs(), inputArgsList);

    try {
      // TODO, use logging listener here
      ExecutionListener listener = NoopExecutionListener.create();

      Map<String, Literal> outputs =
          new LocalEngine(
                  ExecutionContext.builder()
                      .runnableTasks(runnableTasks)
                      .dynamicWorkflowTasks(dynamicWorkflowTasks)
                      .workflowTemplates(workflowTemplates)
                      .executionListener(listener)
                      .build())
              .compileAndExecute(workflow, inputs);
      LOG.info("Outputs: " + StringUtil.serializeLiteralMap(outputs));

      return 0;
    } catch (Throwable e) {
      return handleException(e);
    }
  }

  protected ExecuteLocalArgsParser getArgsParser() {
    return new ExecuteLocalArgsParser();
  }

  protected int handleException(Throwable e) {
    LOG.error("Unhandled exception", e);

    return -1;
  }

  protected String getCustomSynopsis() {
    StringBuilder sb = new StringBuilder();

    sb.append("jflyte execute-local --workflow=").append(workflowName);

    if (packageDir != null) {
      sb.append(" ").append("--classpath=").append(packageDir);
    }

    return sb.toString();
  }
}
