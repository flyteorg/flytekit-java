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
package org.flyte.api.v1;

/** Encapsulation of fields that uniquely identifies a Flyte resource. */
public interface Identifier extends PartialIdentifier {

  /**
   * Name of the domain the resource belongs to. A domain can be considered as a subset within a
   * specific project.
   */
  @Override
  String domain();

  /** Name of the project the resource belongs to. */
  @Override
  String project();

  /** Specific version of the resource. */
  @Override
  String version();

  // TODO: add resourceType and name from src/main/proto/flyteidl/core/identifier.proto
}
