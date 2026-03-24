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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class RetriesTest {

  private static final Set<Code> GRPC_RETRYABLE_CODES =
      Stream.of(Code.UNAVAILABLE, Code.DEADLINE_EXCEEDED, Code.INTERNAL)
          .collect(Collectors.toSet());

  private static final Retries GRPC_RETRIES =
      Retries.create(
          /* maxRetries= */ 7,
          /* maxDelayMilliseconds= */ Long.MAX_VALUE,
          /* initialDelayMilliseconds= */ 10,
          RetriesTest::noopSleeper,
          e ->
              e instanceof StatusRuntimeException
                  && GRPC_RETRYABLE_CODES.contains(
                      ((StatusRuntimeException) e).getStatus().getCode()));

  private static final Retries IO_RETRIES =
      Retries.create(
          /* maxRetries= */ 7,
          /* maxDelayMilliseconds= */ Long.MAX_VALUE,
          /* initialDelayMilliseconds= */ 10,
          RetriesTest::noopSleeper,
          e -> e instanceof IOException);

  @Test
  void testGrpcMaxAttempts() {
    AtomicLong attempts = new AtomicLong();

    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                GRPC_RETRIES.retry(
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
  void testGrpcSuccessfulRetry(Code code) {
    AtomicLong attempts = new AtomicLong();

    int out =
        GRPC_RETRIES.retry(
            () -> {
              if (attempts.incrementAndGet() <= 5L) {
                throw new StatusRuntimeException(code.toStatus());
              } else {
                return 10;
              }
            });

    assertEquals(10, out);
    assertEquals(6, attempts.get());
  }

  @Test
  void testGrpcNonRetryable() {
    AtomicLong attempts = new AtomicLong();

    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                GRPC_RETRIES.retry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                    }));

    assertEquals(Status.INVALID_ARGUMENT, e.getStatus());
    assertEquals(1, attempts.get());
  }

  @Test
  void testGrpcOtherException() {
    AtomicLong attempts = new AtomicLong();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GRPC_RETRIES.retry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new IllegalArgumentException("oops");
                    }));

    assertEquals("oops", e.getMessage());
    assertEquals(1, attempts.get());
  }

  @Test
  void testIoMaxAttempts() {
    AtomicLong attempts = new AtomicLong();

    IOException e =
        assertThrows(
            IOException.class,
            () ->
                IO_RETRIES.retryChecked(
                    () -> {
                      attempts.incrementAndGet();
                      throw new IOException("ssl error");
                    }));

    assertEquals(8, attempts.get());
    assertEquals("ssl error", e.getMessage());
  }

  @Test
  void testIoSuccessfulRetryOnIOException() throws Exception {
    AtomicLong attempts = new AtomicLong();

    IO_RETRIES.retryChecked(
        () -> {
          if (attempts.incrementAndGet() <= 5L) {
            throw new IOException("transient");
          }
        });

    assertEquals(6, attempts.get());
  }

  @Test
  void testIoNonRetryable() {
    AtomicLong attempts = new AtomicLong();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            IO_RETRIES.retryChecked(
                () -> {
                  attempts.incrementAndGet();
                  throw new IllegalArgumentException("bad input");
                }));

    assertEquals(1, attempts.get());
  }

  static void noopSleeper(long delay) {}
}
