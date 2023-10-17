/*
 * Copyright 2020-2022 Flyte Authors.
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
package org.flyte.integrationtests.structs;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class BQReference {
  @AutoValue
  public abstract static class Nested {
    public abstract String project();

    public abstract String dataset();

    public abstract String tableName();
  }

  public abstract String project();

  public abstract String dataset();

  public abstract String tableName();

  // this is only to test nested nested auto-value
  public abstract Nested nested();

  public static BQReference create(String project, String dataset, String tableName) {
    return new AutoValue_BQReference(
        project, dataset, tableName, new AutoValue_BQReference_Nested(project, dataset, tableName));
  }
}
