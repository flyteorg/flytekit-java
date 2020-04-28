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
package org.flyte.flytekit;

import com.google.auto.value.AutoValue;
import java.util.Map;

@AutoValue
public abstract class SdkConfig {

  public abstract String project();

  public abstract String domain();

  public abstract String version();

  public static Builder builder() {
    return new AutoValue_SdkConfig.Builder();
  }

  public static SdkConfig load(Map<String, String> env) {
    return SdkConfig.builder()
        .domain(env.get("JFLYTE_DOMAIN"))
        .project(env.get("JFLYTE_PROJECT"))
        .version(env.get("JFLYTE_VERSION"))
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder project(String project);

    public abstract Builder domain(String domain);

    public abstract Builder version(String version);

    public abstract SdkConfig build();
  }
}
