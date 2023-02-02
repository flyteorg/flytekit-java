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

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.FileSystemRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load the available FileSystems.
 */
class FileSystemLoader {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemLoader.class);

  static Map<String, FileSystem> loadFileSystems(Collection<ClassLoader> modules) {
    return modules.stream()
        .flatMap(module -> ClassLoaders.withClassLoader(module, () -> loadFileSystems().stream()))
        .collect(Collectors.toMap(FileSystem::getScheme, x -> x));
  }

  static FileSystem getFileSystem(Map<String, FileSystem> fileSystems, String uri) {
    return getFileSystem(fileSystems, URI.create(uri));
  }

  static FileSystem getFileSystem(Map<String, FileSystem> fileSystems, URI uri) {
    String scheme = uri.getScheme();
    FileSystem fileSystem = fileSystems.get(scheme);

    return Verify.verifyNotNull(fileSystem, "Can't find FileSystem for [%s]", scheme);
  }

  static List<FileSystem> loadFileSystems() {
    ServiceLoader<FileSystemRegistrar> loader = ServiceLoader.load(FileSystemRegistrar.class);

    LOG.debug("Discovering FileSystemRegistrar");

    List<FileSystem> fileSystems = new ArrayList<>();

    Map<String, String> env = ImmutableMap.copyOf(System.getenv());

    for (FileSystemRegistrar registrar : loader) {
      for (FileSystem fileSystem : registrar.load(env)) {
        LOG.debug(String.format("Discovered FileSystem [%s]", fileSystem.getClass().getName()));

        fileSystems.add(fileSystem);
      }
    }

    return fileSystems;
  }
}
