/*
 * Copyright 2022-2023 Flyte Authors.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class CompletableFuturesTest {

  @Test
  void testGetAll() {
    List<CompletionStage<String>> stages = new ArrayList<>();
    stages.add(CompletableFuture.completedFuture("foo"));
    stages.add(CompletableFuture.completedFuture("bar"));

    assertThat(CompletableFutures.getAll(stages), contains("foo", "bar"));
  }

  @Test
  void testGetAllCancelled() {
    RuntimeException expectedException = new RuntimeException();
    CompletableFuture<String> failedFuture =
        spy(
            CompletableFuture.supplyAsync(
                () -> {
                  throw expectedException;
                }));
    CompletableFuture<String> shouldBeCancelledFuture =
        spy(CompletableFuture.supplyAsync(() -> "foo"));
    List<CompletionStage<String>> stages = new ArrayList<>();
    stages.add(failedFuture);
    stages.add(shouldBeCancelledFuture);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> CompletableFutures.getAll(stages));
    assertThat(exception.getCause().getCause(), is(expectedException));
    verify(failedFuture).cancel(true);
    verify(shouldBeCancelledFuture).cancel(true);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testGetAllInterruptedAndCancelled() throws ExecutionException, InterruptedException {
    InterruptedException expectedException = new InterruptedException();
    CompletableFuture<String> failedFuture = mock(CompletableFuture.class);
    CompletionStage<String> failedStage = mock(CompletionStage.class);
    when(failedFuture.get()).thenThrow(expectedException);
    when(failedStage.toCompletableFuture()).thenReturn(failedFuture);

    CompletableFuture<String> shouldBeCancelledFuture =
        spy(CompletableFuture.supplyAsync(() -> "foo"));
    List<CompletionStage<String>> stages = new ArrayList<>();
    stages.add(failedStage);
    stages.add(shouldBeCancelledFuture);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> CompletableFutures.getAll(stages));
    assertThat(exception.getCause(), is(expectedException));
    verify(failedFuture).cancel(true);
    verify(shouldBeCancelledFuture).cancel(true);

    assertThat(Thread.currentThread().isInterrupted(), is(true));
  }
}
