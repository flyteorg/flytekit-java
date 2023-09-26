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

import static org.flyte.jflyte.utils.MoreCollectors.toUnmodifiableList;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.flyte.api.v1.Struct;

@AutoValue
public abstract class JFlyteCustom {

  abstract List<Artifact> artifacts();

  public Struct serializeToStruct() {
    Struct jflyte =
        Struct.of(
            ImmutableMap.of(
                "artifacts",
                Struct.Value.ofListValue(
                    artifacts().stream()
                        .map(JFlyteCustom::serializeArtifactToStruct)
                        .map(Struct.Value::ofStructValue)
                        .collect(toUnmodifiableList()))));

    return Struct.of(ImmutableMap.of("jflyte", Struct.Value.ofStructValue(jflyte)));
  }

  public static JFlyteCustom deserializeFromStruct(Struct struct) {
    Struct.Value jflyteValue = struct.fields().get("jflyte");

    Preconditions.checkArgument(
        jflyteValue != null, "Can't find 'jflyte' field among %s", struct.fields().keySet());

    Preconditions.checkArgument(
        jflyteValue.kind() == Struct.Value.Kind.STRUCT_VALUE,
        "'jflyte' expected to be STRUCT_VALUE, but got %s",
        jflyteValue.kind());

    Struct.Value artifactsValue = jflyteValue.structValue().fields().get("artifacts");

    Preconditions.checkArgument(
        artifactsValue != null,
        "Can't find 'artifacts' field among %s",
        jflyteValue.structValue().fields().keySet());

    Preconditions.checkArgument(
        artifactsValue.kind() == Struct.Value.Kind.LIST_VALUE,
        "'artifacts' expected to be LIST_VALUE, but got %s",
        artifactsValue.kind());

    List<Artifact> artifacts =
        artifactsValue.listValue().stream()
            .map(JFlyteCustom::deserializeArtifact)
            .collect(toUnmodifiableList());

    return JFlyteCustom.builder().artifacts(artifacts).build();
  }

  private static Struct serializeArtifactToStruct(Artifact artifact) {
    // we don't add size because we aren't sure if we want to expose it
    return Struct.of(
        ImmutableMap.of(
            "name", Struct.Value.ofStringValue(artifact.name()),
            "location", Struct.Value.ofStringValue(artifact.location())));
  }

  private static Artifact deserializeArtifact(Struct.Value structValue) {
    Preconditions.checkArgument(
        structValue.kind() == Struct.Value.Kind.STRUCT_VALUE,
        "'structValue' expected to be STRUCT_VALUE, but got %s",
        structValue.kind());

    Struct struct = structValue.structValue();

    return Artifact.create(
        /*location=*/ struct.fields().get("location").stringValue(),
        /*name=*/ struct.fields().get("name").stringValue(),
        // we don't expose size yet, and we aren't sure if we want to
        0);
  }

  static Builder builder() {
    return new AutoValue_JFlyteCustom.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder artifacts(List<Artifact> artifacts);

    abstract JFlyteCustom build();
  }
}
