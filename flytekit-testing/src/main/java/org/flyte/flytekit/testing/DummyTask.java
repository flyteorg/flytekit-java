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
package org.flyte.flytekit.testing;

import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.flytekit.SdkRemoteTask;
import org.flyte.flytekit.SdkType;

public class DummyTask extends SdkRemoteTask<Map<String, Literal>, Map<String, Literal>> {

  private final String name;

  private final SdkType<Map<String, Literal>> inputs;

  private final SdkType<Map<String, Literal>> outputs;

  public DummyTask(
      String name, SdkType<Map<String, Literal>> inputs, SdkType<Map<String, Literal>> outputs) {
    this.name = name;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  @Override
  public String domain() {
    return "";
  }

  @Override
  public String project() {
    return "";
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public SdkType<Map<String, Literal>> inputs() {
    return this.inputs;
  }

  @Override
  public SdkType<Map<String, Literal>> outputs() {
    return this.outputs;
  }
}
