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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Timestamp;

public class SdkBindingData {

  private final BindingData idl;

  public SdkBindingData(BindingData idl) {
    this.idl = idl;
  }

  public static SdkBindingData ofInteger(long value) {
    return ofScalar(Scalar.of(Primitive.of(value)));
  }

  public static SdkBindingData ofDouble(double value) {
    return ofScalar(Scalar.of(Primitive.of(value)));
  }

  public static SdkBindingData ofString(String value) {
    return ofScalar(Scalar.of(Primitive.of(value)));
  }

  public static SdkBindingData ofBoolean(boolean value) {
    return ofScalar(Scalar.of(Primitive.of(value)));
  }

  public static SdkBindingData ofDatetime(int year, int month, int day) {
    Instant instant = LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);

    return ofDatetime(Timestamp.create(instant.getEpochSecond(), 0));
  }

  public static SdkBindingData ofDatetime(Timestamp value) {
    return ofScalar(Scalar.of(Primitive.of(value)));
  }

  public static SdkBindingData ofDuration(Duration value) {
    return ofScalar(Scalar.of(Primitive.of(value)));
  }

  public static SdkBindingData ofOutputReference(String nodeId, String nodeVar) {
    BindingData idl = BindingData.of(OutputReference.create(nodeId, nodeVar));

    return new SdkBindingData(idl);
  }

  public static SdkBindingData ofScalar(Scalar scalar) {
    return new SdkBindingData(BindingData.of(scalar));
  }

  public BindingData toIdl() {
    return idl;
  }
}
