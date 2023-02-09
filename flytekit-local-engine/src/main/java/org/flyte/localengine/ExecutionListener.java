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

/** Registers nodes depending on their state, i.e. pending, retrying, error, stating or completed */
public interface ExecutionListener {

  /** Registers the execution node as pending. */
  void pending(ExecutionNode node);

  /** Registers the execution node as retrying. */
  void retrying(ExecutionNode node, Map<String, Literal> inputs, Throwable e, int attempt);

  /** Registers the execution node as error. */
  void error(ExecutionNode node, Map<String, Literal> inputs, Throwable e);

  /** Registers the execution node as starting. */
  void starting(ExecutionNode node, Map<String, Literal> inputs);

  /** Registers the execution node as completed. */
  void completed(ExecutionNode node, Map<String, Literal> inputs, Map<String, Literal> outputs);
}
