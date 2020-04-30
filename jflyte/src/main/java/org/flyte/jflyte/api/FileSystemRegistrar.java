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
package org.flyte.jflyte.api;

import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A registrar that creates {@link FileSystem} instances. */
public abstract class FileSystemRegistrar {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemRegistrar.class);

  public abstract Iterable<FileSystem> load(ClassLoader classLoader);

  public static FileSystem getFileSystem(String scheme, ClassLoader classLoader) {
    ServiceLoader<FileSystemRegistrar> loader =
        ServiceLoader.load(FileSystemRegistrar.class, classLoader);

    LOG.debug("Discovering FileSystemRegistrar");

    for (FileSystemRegistrar registrar : loader) {
      for (FileSystem fileSystem : registrar.load(classLoader)) {
        LOG.debug("Discovered FileSystem [{}]", fileSystem.getClass().getName());

        if (scheme.equals(fileSystem.getScheme())) {
          return fileSystem;
        }
      }
    }

    return null;
  }
}
