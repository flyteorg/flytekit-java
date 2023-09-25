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
package org.flyte.jflyte.utils;

import com.google.auto.value.AutoValue;
import org.flyte.jflyte.api.FileSystem;

/** Represents artifact to stage to {@link FileSystem}. */
@AutoValue
abstract class Artifact {

  /**
   * Get the artifact location.
   *
   * @return the artifact location.
   */
  abstract String location();

  /**
   * Get the artifact name.
   *
   * @return the artifact name.
   */
  abstract String name();

  /**
   * Get the artifact size.
   *
   * @return the artifact size.
   */
  abstract long size();

  /**
   * Create a new artifact.
   *
   * @param location artifact location.
   * @param name name location.
   * @param size size location.
   * @return A new artifact.
   */
  static Artifact create(String location, String name, long size) {
    return new AutoValue_Artifact(location, name, size);
  }
}
