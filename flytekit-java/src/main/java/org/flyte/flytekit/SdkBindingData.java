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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

@AutoValue
public abstract class SdkBindingData {

  abstract BindingData idl();

  public static SdkBindingData create(BindingData idl) {
    return new AutoValue_SdkBindingData(idl);
  }

  public static SdkBindingData ofInteger(long value) {
    return ofPrimitive(Primitive.ofInteger(value));
  }

  public static SdkBindingData ofFloat(double value) {
    return ofPrimitive(Primitive.ofFloat(value));
  }

  public static SdkBindingData ofString(String value) {
    return ofPrimitive(Primitive.ofString(value));
  }

  public static SdkBindingData ofBoolean(boolean value) {
    return ofPrimitive(Primitive.ofBoolean(value));
  }

  public static SdkBindingData ofDatetime(int year, int month, int day) {
    Instant instant = LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);
    return ofDatetime(instant);
  }

  public static SdkBindingData ofDatetime(Instant value) {
    return ofPrimitive(Primitive.ofDatetime(value));
  }

  public static SdkBindingData ofDuration(Duration value) {
    return ofPrimitive(Primitive.ofDuration(value));
  }

  public static SdkBindingData ofOutputReference(String nodeId, String nodeVar) {
    BindingData idl = BindingData.of(OutputReference.builder().nodeId(nodeId).var(nodeVar).build());

    return create(idl);
  }

  public static SdkBindingData ofPrimitive(Primitive primitive) {
    return ofScalar(Scalar.of(primitive));
  }

  public static SdkBindingData ofScalar(Scalar scalar) {
    return create(BindingData.of(scalar));
  }
}
