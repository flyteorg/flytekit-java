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
package org.flyte.flytekit;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

class MoreCollectors {
  static <T> Collector<T, ?, List<T>> toUnmodifiableList() {
    return collectingAndThen(toList(), Collections::unmodifiableList);
  }

  static <K, V> Collector<Map.Entry<K, V>, ?, Map<K, V>> toUnmodifiableMap() {
    return collectingAndThen(
        toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::unmodifiableMap);
  }

  static <T, K, V> Collector<T, ?, Map<K, V>> toUnmodifiableMap(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends V> valueMapper) {
    return collectingAndThen(toMap(keyMapper, valueMapper), Collections::unmodifiableMap);
  }
}
