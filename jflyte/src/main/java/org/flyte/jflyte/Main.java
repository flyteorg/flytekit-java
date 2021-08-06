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
import org.slf4j.bridge.SLF4JBridgeHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Application entry point. */
@Command(subcommands = {Main.JFlyte.class})
public class Main implements Callable<Integer> {

  @Override
  public Integer call() {
    new CommandLine(this).usage(System.err);
    return 1;
  }

  /** "jflyte" entry point. */
  @Command(
      name = "jflyte",
      subcommands = {Execute.class, ExecuteLocal.class, Register.class, Serialize.class})
  static class JFlyte implements Callable<Integer> {

    @Override
    public Integer call() {
      new CommandLine(this).usage(System.err);
      return 1;
    }
  }

  /** "register" entry point. */
  @Command(
      name = "register",
      subcommands = {RegisterWorkflows.class})
  static class Register implements Callable<Integer> {
    @Override
    public Integer call() {
      new CommandLine(this).usage(System.err);
      return 1;
    }
  }

  /** "serialize" entry point. */
  @Command(
      name = "serialize",
      subcommands = {SerializeWorkflows.class})
  static class Serialize implements Callable<Integer> {
    @Override
    public Integer call() {
      new CommandLine(this).usage(System.err);
      return 1;
    }
  }

  /**
   * Runs the application.
   *
   * @param args command-line arguments
   */
  public static void main(String... args) {
    // http://www.slf4j.org/api/org/slf4j/bridge/SLF4JBridgeHandler.html
    setUpSLF4JBridge();

    System.exit(new CommandLine(new Main()).execute(args));
  }

  private static void setUpSLF4JBridge() {
    // Optionally remove existing handlers attached to j.u.l root logger
    SLF4JBridgeHandler.removeHandlersForRootLogger(); // (since SLF4J 1.6.5)

    // add SLF4JBridgeHandler to j.u.l's root logger, should be done once during
    // the initialization phase of your application
    SLF4JBridgeHandler.install();
  }
}
