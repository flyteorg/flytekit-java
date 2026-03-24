/*
 * Copyright 2020-2026 Flyte Authors.
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

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.Var;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tiny utility to retry operations with exponential backoff. */
@AutoValue
abstract class Retries {
  private static final Logger LOG = LoggerFactory.getLogger(Retries.class);

  public abstract int maxRetries();

  public abstract long maxDelayMilliseconds();

  public abstract long initialDelayMilliseconds();

  abstract Sleeper sleeper();

  abstract Predicate<Exception> retryableCheck();

  public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  /** Like Callable, but doesn't throw a checked exception. */
  interface Retryable<T> extends Callable<T> {
    @Override
    T call();
  }

  interface CheckedRunnable {
    void run() throws Exception;
  }

  /**
   * Retry a callable that only throws unchecked exceptions. Uses a sneaky-throw to delegate to
   * {@link #retryInternal} without catching and re-wrapping: the compiler infers {@code E} as
   * {@code RuntimeException}, so no checked exception is declared at call sites, while the original
   * exception propagates unchanged at runtime thanks to type erasure.
   *
   * @param retryable The retry function.
   * @return The result of the retry.
   * @param <T> The type T of the return resolved object.
   * @param <E> Inferred as RuntimeException at call sites; enables sneaky-throw.
   */
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T retry(Retryable<T> retryable) throws E {
    try {
      return retryInternal(retryable);
    } catch (Exception e) {
      throw (E) e;
    }
  }

  /**
   * Retry a runnable that may throw checked exceptions.
   *
   * @param runnable The retry function.
   */
  public void retryChecked(CheckedRunnable runnable) throws Exception {
    retryInternal(
        () -> {
          runnable.run();
          return null;
        });
  }

  private <T> T retryInternal(Callable<T> callable) throws Exception {
    @Var int attempt = 0;

    do {
      try {
        return callable.call();
      } catch (Exception e) {
        if (attempt < maxRetries() && retryableCheck().test(e)) {
          long delay =
              Math.min(maxDelayMilliseconds(), (1L << attempt) * initialDelayMilliseconds());
          LOG.warn("Retrying in " + delay + " ms", e);

          try {
            sleeper().sleep(delay);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }

          attempt++;
        } else {
          throw e;
        }
      }
    } while (true);
  }

  static Retries create(
      int maxRetries,
      long maxDelayMilliseconds,
      long initialDelayMilliseconds,
      Sleeper sleeper,
      Predicate<Exception> retryableCheck) {
    return new AutoValue_Retries(
        maxRetries, maxDelayMilliseconds, initialDelayMilliseconds, sleeper, retryableCheck);
  }
}
