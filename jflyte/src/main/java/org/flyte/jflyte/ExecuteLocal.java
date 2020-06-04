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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowTemplate;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Unmatched;

/** Handler for "execute-local" command. */
@Command(name = "execute-local")
public class ExecuteLocal implements Callable<Integer> {
  @Option(
      names = {"--workflow"},
      required = true)
  private String workflowName;

  @Option(
      names = {"-cp", "--classpath"},
      description = "Directory with packaged jars",
      required = true)
  private String packageDir;

  @Unmatched private String[] inputArgs;

  @Override
  public Integer call() {
    ClassLoader packageClassLoader = ClassLoaders.forDirectory(packageDir);

    // before we run anything, switch class loader, because we will be touching user classes;
    // setting it in thread context will give us access to the right class loader
    ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(packageClassLoader);

    try {
      Map<String, String> env =
          ImmutableMap.of(
              "JFLYTE_DOMAIN", "development",
              "JFLYTE_VERSION", "test",
              "JFLYTE_PROJECT", "flytetester");

      Map<String, RunnableTask> tasks = Modules.loadTasks(env);
      Map<String, WorkflowTemplate> workflows = Modules.loadWorkflows(env);

      WorkflowTemplate workflow =
          Preconditions.checkNotNull(
              workflows.get(workflowName), "workflow not found [%s]", workflowName);

      CommandLine.Model.CommandSpec spec = CommandLine.Model.CommandSpec.create();
      spec.usageMessage()
          .customSynopsis(
              String.format(
                  "jflyte execute-local --workflow=%s --classpath=%s", workflowName, packageDir));

      Map<String, Literal> inputs = parseInputs(spec, workflow.interface_().inputs(), inputArgs);

      LocalRunner.compileAndExecute(workflow, tasks, inputs);
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextClassLoader);
    }

    return 0;
  }

  static Map<String, Literal> parseInputs(
      CommandLine.Model.CommandSpec spec, Map<String, Variable> variableMap, String[] inputs) {
    for (Map.Entry<String, Variable> entry : variableMap.entrySet()) {
      Variable variable = entry.getValue();

      // FIXME we assume for now that there are only simple types, because everything else isn't
      // implemented, we should improve error message once we support other cases
      SimpleType simpleType = variable.literalType().simpleType();

      CommandLine.Model.OptionSpec.Builder builder =
          CommandLine.Model.OptionSpec.builder("--" + entry.getKey())
              .converters(new LiteralTypeConverter(simpleType))
              .required(true);

      if (variable.description() != null) {
        builder.description(variable.description());
      }

      spec.addOption(builder.build());
    }

    CommandLine.ParseResult result =
        new CommandLine(spec).parseArgs(inputs != null ? inputs : new String[0]);

    return variableMap.keySet().stream()
        .map(name -> Maps.immutableEntry(name, result.matchedOptionValue(name, (Literal) null)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
