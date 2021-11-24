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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.jflyte.api.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PackageLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PackageLoader.class);

  static ClassLoader load(Map<String, FileSystem> fileSystems, TaskTemplate taskTemplate) {
    JFlyteCustom custom = JFlyteCustom.deserializeFromStruct(taskTemplate.custom());

    return loadPackage(fileSystems, custom.artifacts());
  }

  private static ClassLoader loadPackage(
      Map<String, FileSystem> fileSystems, List<Artifact> artifacts) {
    try {
      Path tmp = Files.createTempDirectory("tasks");

      // TODO do in parallel

      for (Artifact artifact : artifacts) {
        FileSystem fileSystem = FileSystemLoader.getFileSystem(fileSystems, artifact.location());

        try (ReadableByteChannel reader = fileSystem.reader(artifact.location())) {
          Path path = tmp.resolve(artifact.name());

          if (path.toFile().exists()) {
            // file already exists, but we have checksums, so we should be ok
            LOG.warn("Duplicate entry in artifacts: [{}]", artifact);
            continue;
          }

          LOG.info("Copied {} to {}", artifact.location(), path);

          Files.copy(Channels.newInputStream(reader), path);
        }
      }

      return ClassLoaders.forDirectory(tmp.toFile());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
