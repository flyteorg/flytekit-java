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

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Handler for "execute-local" command. */
@Command(name = "execute-local")
public class ExecuteLocal implements Callable<Integer> {
  @Option(
      names = {"--workflow"},
      required = true)
  private String workflow;

  @Option(
      names = {"-cp", "--classpath"},
      description = "Directory with packaged jars",
      required = true)
  private String packageDir;

  // TODO once we support workflow inputs, parse them from options, supporting primitive types
  // should be enough

  @Override
  public Integer call() {
    ClassLoader packageClassLoader = ClassLoaders.forDirectory(packageDir);

    // before we run anything, switch class loader, because we will be touching user classes;
    // setting it in thread context will give us access to the right class loader
    ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(packageClassLoader);

    try {
      LocalRunner.executeWorkflow(workflow);
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextClassLoader);
    }

    return 0;
  }
}
