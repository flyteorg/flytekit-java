package org.flyte.jflyte;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

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
