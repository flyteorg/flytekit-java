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

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import javax.annotation.Nullable;
import org.flyte.jflyte.api.Manifest;

class InMemoryFileSystem implements org.flyte.jflyte.api.FileSystem {

  final FileSystem fileSystem;

  InMemoryFileSystem(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @Override
  public String getScheme() {
    return "mem";
  }

  @Override
  public ReadableByteChannel reader(String uri) {
    return seekableByteChannel(uri);
  }

  @Override
  public WritableByteChannel writer(String uri) {
    return seekableByteChannel(uri, CREATE_NEW, WRITE);
  }

  private SeekableByteChannel seekableByteChannel(String uri, OpenOption... options) {
    try {
      return Files.newByteChannel(fileSystem.getPath(uri), options);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nullable
  @Override
  public Manifest getManifest(String uri) {
    return null;
  }
}
