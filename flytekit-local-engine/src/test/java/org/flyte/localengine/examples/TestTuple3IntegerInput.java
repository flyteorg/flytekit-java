/*
 * Copyright 2020-2023 Flyte Authors.
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
package org.flyte.localengine.examples;

import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;

@AutoValue
public abstract class TestTuple3IntegerInput {
  public abstract SdkBindingData<Long> a();

  public abstract SdkBindingData<Long> b();

  public abstract SdkBindingData<Long> c();

  public static TestTuple3IntegerInput create(
      SdkBindingData<Long> a, SdkBindingData<Long> b, SdkBindingData<Long> c) {
    return new AutoValue_TestTuple3IntegerInput(a, b, c);
  }
}
