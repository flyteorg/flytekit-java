/*
 * Copyright 2020 Spotify AB.
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
package org.flyte.jflyte;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.Var;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tiny utility to retry gRPC requests. */
@AutoValue
abstract class GrpcRetries {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcRetries.class);

  public abstract int maxRetries();

  public abstract long maxDelayMilliseconds();

  public abstract long initialDelayMilliseconds();

  abstract Sleeper sleeper();

  public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  /** Like Callable, but doesn't throw an exception. */
  interface Retryable<T> extends Callable<T> {
    @Override
    T call();
  }

  public <T> T retry(Retryable<T> retryable) {
    @Var int attempt = 0;

    do {
      try {
        return retryable.call();
      } catch (StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();

        boolean isRetryable =
            code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;

        if (attempt < maxRetries() && isRetryable) {
          long delay =
              Math.min(maxDelayMilliseconds(), (1 << attempt) * initialDelayMilliseconds());
          LOG.warn("Retrying in " + delay + " ms", e);

          try {
            sleeper().sleep(delay);
          } catch (InterruptedException interrupted) {
            throw e;
          }

          attempt++;
        } else {
          throw e;
        }
      }
    } while (true);
  }

  static GrpcRetries create() {
    return create(
        /* maxRetries= */ 10,
        /* maxDelayMilliseconds= */ 5_000L,
        /* initialDelayMilliseconds= */ 250L,
        /* sleeper= */ Thread::sleep);
  }

  static GrpcRetries create(
      int maxRetries, long maxDelayMilliseconds, long initialDelayMilliseconds, Sleeper sleeper) {
    return new AutoValue_GrpcRetries(
        maxRetries, maxDelayMilliseconds, initialDelayMilliseconds, sleeper);
  }
}
