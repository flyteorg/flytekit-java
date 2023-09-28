/*
 * Copyright 2020-2021 Flyte Authors.
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

import com.google.errorprone.annotations.MustBeClosed;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;

/**
 * File system interface for jflyte.
 *
 * <p>Defines APIs for file system agnostic code.
 */
public interface FileSystem {

  String getScheme();

  @MustBeClosed
  ReadableByteChannel reader(String uri);

  @MustBeClosed
  WritableByteChannel writer(String uri);

  /**
   * Returns Manifest for given resource, or null if it doesn't exist.
   *
   * @param uri uri
   * @return manifest
   */
  @Nullable
  Manifest getManifest(String uri);
}
