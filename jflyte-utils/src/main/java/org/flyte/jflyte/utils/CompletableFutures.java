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
package org.flyte.jflyte.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

/** Utility to wait for a list of completion stages. */
class CompletableFutures {

  static <T> List<T> getAll(List<CompletionStage<T>> stages) {
    List<T> result = new ArrayList<>(stages.size());

    for (int i = 0; i < stages.size(); ++i) {
      try {
        result.add(stages.get(i).toCompletableFuture().get());
      } catch (InterruptedException | ExecutionException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        for (int j = i; j < stages.size(); ++j) {
          stages.get(j).toCompletableFuture().cancel(true);
        }

        throw new RuntimeException(e);
      }
    }

    return result;
  }
}
