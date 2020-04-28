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

import flyteidl.core.Literals;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Registrars;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.TaskIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Handler for "execute" command. */
@Command(name = "execute")
public class Execute implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(Execute.class);
  private static final String OUTPUTS_PB = "outputs.pb";

  @Option(
      names = {"--task"},
      required = true)
  private String task;

  @Option(
      names = {"--inputs"},
      required = true)
  private String inputs;

  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"--outputPrefix"},
      required = true)
  private String outputPrefix;

  @Option(
      names = {"--indexFileLocation"},
      required = true)
  private String indexFileLocation;

  @Override
  public Integer call() {
    execute();
    return 0;
  }

  public void execute() {
    Config config = Config.load();
    ClassLoader pluginClassLoader = ClassLoaders.forDirectory(config.pluginDir());
    List<String> stagedFiles = readStagedFiles(pluginClassLoader, indexFileLocation);

    ClassLoader packageClassLoader = loadPackage(stagedFiles, pluginClassLoader);

    FileSystem inputFs =
        FileSystemRegistrar.getFileSystem(URI.create(inputs).getScheme(), pluginClassLoader);
    FileSystem outputFs =
        FileSystemRegistrar.getFileSystem(URI.create(outputPrefix).getScheme(), pluginClassLoader);
    Map<String, Literal> input = getInput(inputFs, inputs);
    RunnableTask runnableTask = getTask(task, packageClassLoader);

    Map<String, Literal> outputs = runnableTask.run(input);

    writeOutputs(outputFs, outputPrefix, outputs);
  }

  private static void writeOutputs(
      FileSystem fs, String outputPrefix, Map<String, Literal> outputs) {
    String outputUri;
    if (outputPrefix.endsWith("/")) {
      outputUri = outputPrefix + OUTPUTS_PB;
    } else {
      outputUri = outputPrefix + "/" + OUTPUTS_PB;
    }

    try (WritableByteChannel channel = fs.writer(outputUri)) {
      Literals.LiteralMap proto = ProtoUtil.serializeLiteralMap(outputs);

      proto.writeTo(Channels.newOutputStream(channel));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Map<String, Literal> getInput(FileSystem fs, String uri) {
    try (ReadableByteChannel channel = fs.reader(uri)) {
      Literals.LiteralMap proto = Literals.LiteralMap.parseFrom(Channels.newInputStream(channel));

      return ProtoUtil.deserialize(proto);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static RunnableTask getTask(String name, ClassLoader packageClassLoader) {
    // be careful not to pass extra
    Map<String, String> env =
        System.getenv().entrySet().stream()
            .filter(x -> x.getKey().startsWith("JFLYTE_"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<TaskIdentifier, RunnableTask> tasks =
        Registrars.loadAll(RunnableTaskRegistrar.class, packageClassLoader, env);

    for (Map.Entry<TaskIdentifier, RunnableTask> entry : tasks.entrySet()) {
      if (entry.getKey().name().equals(name)) {
        return entry.getValue();
      }
    }

    throw new IllegalArgumentException("Task not found: " + name);
  }

  private static List<String> readStagedFiles(
      ClassLoader pluginClassLoader, String indexFileLocation) {
    FileSystem fs =
        FileSystemRegistrar.getFileSystem(
            URI.create(indexFileLocation).getScheme(), pluginClassLoader);
    List<String> files = new ArrayList<>();

    try (ReadableByteChannel reader = fs.reader(indexFileLocation)) {
      Scanner scanner = new Scanner(Channels.newInputStream(reader), "UTF-8");

      while (scanner.hasNext()) {
        String next = scanner.next();

        LOG.info("Read staged file {}", next);

        if (!next.isEmpty()) {
          files.add(next);
        }
      }

      return files;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static ClassLoader loadPackage(List<String> stagedFiles, ClassLoader pluginClassLoader) {
    try {
      Path tmp = Files.createTempDirectory("tasks");

      // FIXME we assume all files use the same filesystem
      // TODO do in parallel

      String scheme = URI.create(stagedFiles.get(0)).getScheme();
      FileSystem fileSystem = FileSystemRegistrar.getFileSystem(scheme, pluginClassLoader);

      for (String stagedFile : stagedFiles) {
        try (ReadableByteChannel reader = fileSystem.reader(stagedFile)) {
          String name = stagedFile.substring(stagedFile.lastIndexOf("/") + 1);
          Path path = tmp.resolve(name);

          if (path.toFile().exists()) {
            // file already exists, but we have checksums, so we should be ok
            LOG.warn("Duplicate entry in --stagedFiles: [{}]", stagedFile);
            continue;
          }

          LOG.info("Copied {} to {}", stagedFile, path);

          Files.copy(Channels.newInputStream(reader), path);
        }
      }

      return ClassLoaders.forDirectory(tmp.toFile().getAbsolutePath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
