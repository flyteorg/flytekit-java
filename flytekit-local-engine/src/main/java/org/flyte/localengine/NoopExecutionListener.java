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
package org.flyte.localengine;

import java.util.Map;
import org.flyte.api.v1.Literal;

public class NoopExecutionListener implements ExecutionListener {

  private NoopExecutionListener() {}

  public static ExecutionListener create() {
    return new NoopExecutionListener();
  }

  @Override
  public void pending(ExecutionNode node) {}

  @Override
  public void retrying(ExecutionNode node, Map<String, Literal> inputs, Throwable e, int attempt) {}

  @Override
  public void error(ExecutionNode node, Map<String, Literal> inputs, Throwable e) {}

  @Override
  public void starting(ExecutionNode node, Map<String, Literal> inputs) {}

  @Override
  public void completed(
      ExecutionNode node, Map<String, Literal> inputs, Map<String, Literal> outputs) {}
}
