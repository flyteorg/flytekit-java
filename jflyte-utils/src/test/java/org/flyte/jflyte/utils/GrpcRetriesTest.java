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
package org.flyte.jflyte.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class GrpcRetriesTest {

  @Test
  void testMaxAttempts() {
    AtomicLong attempts = new AtomicLong();
    GrpcRetries retries =
        GrpcRetries.create(
            /* maxRetries= */ 7,
            /* maxDelayMilliseconds= */ Long.MAX_VALUE,
            /* initialDelayMilliseconds= */ 10,
            GrpcRetriesTest::noopSleeper);

    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                retries.retry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
                    }));

    assertEquals(8, attempts.get());
    assertEquals(Status.DEADLINE_EXCEEDED, e.getStatus());
  }

  @ParameterizedTest
  @EnumSource(
      value = Code.class,
      names = {"DEADLINE_EXCEEDED", "UNAVAILABLE", "INTERNAL"})
  void testSuccessfulRetry(Code code) {
    AtomicLong attempts = new AtomicLong();
    GrpcRetries retries =
        GrpcRetries.create(
            /* maxRetries= */ 7,
            /* maxDelayMilliseconds= */ Long.MAX_VALUE,
            /* initialDelayMilliseconds= */ 10,
            GrpcRetriesTest::noopSleeper);

    int out =
        retries.retry(
            () -> {
              if (attempts.incrementAndGet() <= 5L) {
                throw new StatusRuntimeException(code.toStatus());
              } else {
                return 10;
              }
            });

    assertEquals(10, out);

    // 5 attempts fail, and the last one succeeds
    assertEquals(6, attempts.get());
  }

  @Test
  void testNonRetryable() {
    AtomicLong attempts = new AtomicLong();
    GrpcRetries retries =
        GrpcRetries.create(
            /* maxRetries= */ 7,
            /* maxDelayMilliseconds= */ Long.MAX_VALUE,
            /* initialDelayMilliseconds= */ 10,
            GrpcRetriesTest::noopSleeper);

    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                retries.retry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                    }));

    assertEquals(Status.INVALID_ARGUMENT, e.getStatus());
    assertEquals(1, attempts.get());
  }

  @Test
  void testOtherException() {
    AtomicLong attempts = new AtomicLong();
    GrpcRetries retries =
        GrpcRetries.create(
            /* maxRetries= */ 7,
            /* maxDelayMilliseconds= */ Long.MAX_VALUE,
            /* initialDelayMilliseconds= */ 10,
            GrpcRetriesTest::noopSleeper);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                retries.retry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new IllegalArgumentException("oops");
                    }));

    assertEquals("oops", e.getMessage());
    assertEquals(1, attempts.get());
  }

  static void noopSleeper(long delay) {}
}
