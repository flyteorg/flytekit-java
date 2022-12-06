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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.SdkTypes;

/** Example Flyte task that takes a name as the input and outputs a simple greeting message. */
@AutoService(SdkRunnableTask.class)
public class GreetTask extends SdkRunnableTask<String, String> {
  public GreetTask() {
    super(
        SdkTypes.ofPrimitive("name", String.class), SdkTypes.ofPrimitive("greeting", String.class));
  }

  /**
   * Binds input data to this task.
   *
   * @param name the input name
   * @return a transformed instance of this class with input data
   */
  public static SdkTransform of(SdkBindingData name) {
    return new GreetTask().withInput("name", name);
  }

  /**
   * Defines task behavior. This task takes a name as the input, compose a welcome message, and
   * returns it.
   *
   * @param name the name of the person to be greeted
   * @return the welcome message
   */
  @Override
  public String run(String name) {
    return String.format("Welcome, %s!", name);
  }
}
