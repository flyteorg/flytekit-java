/*
 * Copyright 2021-2023 Flyte Authors.
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
package org.flyte.jflyte.utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.jflyte.api.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PackageLoader.class);

  public static ClassLoader load(
      Map<String, FileSystem> fileSystems,
      TaskTemplate taskTemplate,
      ExecutorService executorService) {
    JFlyteCustom custom = JFlyteCustom.deserializeFromStruct(taskTemplate.custom());

    return loadPackage(fileSystems, custom.artifacts(), executorService);
  }

  private static ClassLoader loadPackage(
      Map<String, FileSystem> fileSystems,
      List<Artifact> artifacts,
      ExecutorService executorService) {
    Path tmp = createTempDirectory();

    List<CompletionStage<Void>> stages =
        artifacts.stream()
            .filter(distinct())
            .map(artifact -> handleArtifact(fileSystems, artifact, tmp, executorService))
            .collect(Collectors.toList());
    CompletableFutures.getAll(stages);

    return ClassLoaders.forDirectory(tmp.toFile());
  }

  private static Path createTempDirectory() {
    try {
      return Files.createTempDirectory("tasks");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Predicate<Artifact> distinct() {
    Map<String, Boolean> seen = new HashMap<>();
    return artifact -> seen.putIfAbsent(artifact.name(), Boolean.TRUE) == null;
  }

  private static CompletableFuture<Void> handleArtifact(
      Map<String, FileSystem> fileSystems,
      Artifact artifact,
      Path tmp,
      ExecutorService executorService) {
    return CompletableFuture.runAsync(
        () -> handleArtifact(fileSystems, artifact, tmp), executorService);
  }

  private static void handleArtifact(
      Map<String, FileSystem> fileSystems, Artifact artifact, Path tmp) {
    Path path = tmp.resolve(artifact.name());
    FileSystem fileSystem = FileSystemLoader.getFileSystem(fileSystems, artifact.location());

    try (ReadableByteChannel reader = fileSystem.reader(artifact.location())) {
      LOG.debug("Copied {} to {}", artifact.location(), path);

      Files.copy(Channels.newInputStream(reader), path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
