/*
 * Copyright 2020-2023 Flyte Authors
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

import com.google.auto.value.AutoValue;

/**
 * Encapsulation of fields that identifies a Flyte resource. A Flyte resource can be a task,
 * workflow or launch plan. A resource can internally have multiple versions and is uniquely
 * identified by project, domain, and name.
 */
@AutoValue
public abstract class NamedEntityIdentifier {

  /**
   * Name of the domain the resource belongs to. A domain can be considered as a subset within a
   * specific project.
   */
  public abstract String domain();

  /** Name of the project the resource belongs to. */
  public abstract String project();

  /**
   * User provided value for the resource. The combination of project + domain + name uniquely
   * identifies the resource. [Optional] - used in certain contexts - like 'List API', 'Launch
   * plans'
   */
  public abstract String name();

  public static Builder builder() {
    return new AutoValue_NamedEntityIdentifier.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder domain(String domain);

    public abstract Builder project(String project);

    public abstract Builder name(String name);

    public abstract NamedEntityIdentifier build();
  }
}
