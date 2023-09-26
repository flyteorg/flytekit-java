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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class QuantityUtilTest {
  @ParameterizedTest
  @ValueSource(strings = {"4", "2.3", "+1", "2Ki", "4m", "5e-3", "3E6"})
  void shouldIdentifyValidQuantities(String quantity) {
    assertTrue(QuantityUtil.isValidQuantity(quantity));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "+-1", "not a number"})
  void shouldNotIdentifyInvalidQuantities(String quantity) {
    assertFalse(QuantityUtil.isValidQuantity(quantity));
  }

  @ParameterizedTest
  @CsvSource({"4,4", "2.3,3", "+1,1", "2Ki,2K", "4m,1", "5e-3,1", "3E6,3000000"})
  void shouldConvertQuantitiesToJava(String quantity, String expected) {
    assertEquals(expected, QuantityUtil.asJavaQuantity(quantity));
  }

  @Test
  void shouldThrowExceptionWhenConvertingInvalidQuantities() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> QuantityUtil.asJavaQuantity("not a quantity"));

    assertEquals("Couldn't convert to Java quantity: not a quantity", ex.getMessage());
  }
}
