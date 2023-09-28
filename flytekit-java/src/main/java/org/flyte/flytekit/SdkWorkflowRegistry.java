/*
 * Copyright 2020-2023 Flyte Authors
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

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** Used to discover {@link SdkWorkflow}s. */
public abstract class SdkWorkflowRegistry {

  /**
   * Returns the {@link SdkWorkflow} discovered.
   *
   * @return the workflow list.
   */
  public abstract List<SdkWorkflow<?, ?>> getWorkflows();

  static List<SdkWorkflow<?, ?>> loadAll() {
    return loadAll(ServiceLoader.load(SdkWorkflowRegistry.class));
  }

  static List<SdkWorkflow<?, ?>> loadAll(Iterable<SdkWorkflowRegistry> loader) {
    List<SdkWorkflow<?, ?>> workflows = new ArrayList<>();

    for (SdkWorkflowRegistry registry : loader) {
      workflows.addAll(registry.getWorkflows());
    }

    return unmodifiableList(workflows);
  }
}
