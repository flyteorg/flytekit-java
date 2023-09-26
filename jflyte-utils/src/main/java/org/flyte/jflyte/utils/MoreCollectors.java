/*
 * Copyright 2020-2023 Flyte Authors.
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

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

/** Custom stream java collectors. */
public class MoreCollectors {
  public static <T> Collector<T, ?, List<T>> toUnmodifiableList() {
    return collectingAndThen(toList(), Collections::unmodifiableList);
  }

  public static <K, V> Collector<Map.Entry<K, V>, ?, Map<K, V>> toUnmodifiableMap() {
    return collectingAndThen(
        toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::unmodifiableMap);
  }

  public static <T, K, V> Collector<T, ?, Map<K, V>> toUnmodifiableMap(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends V> valueMapper) {
    return collectingAndThen(toMap(keyMapper, valueMapper), Collections::unmodifiableMap);
  }

  public static <K, V1, V2> Map<K, V2> mapValues(Map<K, V1> map, Function<V1, V2> fn) {
    return map.entrySet().stream()
        .collect(toUnmodifiableMap(Map.Entry::getKey, x -> fn.apply(x.getValue())));
  }

  public static <K, V1, V2> Map<K, V2> mapValues(
      Map<K, V1> map, Function<V1, V2> fn, String keyTemplate, Function<K, Object[]> templateVars) {
    Map<K, V2> newValues = new LinkedHashMap<>(map.size());
    map.forEach(
        (k, v1) -> {
          try {
            V2 v2 = fn.apply(v1);
            newValues.put(k, v2);
          } catch (RuntimeException e) {
            throw new RuntimeException(String.format(keyTemplate, templateVars.apply(k)), e);
          }
        });
    return unmodifiableMap(newValues);
  }
}
