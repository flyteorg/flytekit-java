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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.flyte.flytekit.SdkContainerTask;
import org.flyte.flytekit.SdkTypes;

/** Hello container in Flyte. */
@AutoService(SdkContainerTask.class)
public class HelloContainerTask extends SdkContainerTask<Void, Void> {

  public HelloContainerTask() {
    super(SdkTypes.nulls(), SdkTypes.nulls());
  }

  @Override
  public String getImage() {
    return "alpine";
  }

  @Override
  public List<String> getArgs() {
    return Arrays.asList("-c", "echo Hello $NAME");
  }

  @Override
  public List<String> getCommand() {
    return singletonList("sh");
  }

  @Override
  public Map<String, String> getEnv() {
    return singletonMap("NAME", "Joe");
  }
}
