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
package org.flyte.localengine;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import java.util.LinkedHashMap;
import java.util.Map;

public class ImmutableMap {

  // NB only for tests, doesn't offer thread-safe publishing as in guava

  public static <K, V> Map<K, V> of() {
    return emptyMap();
  }

  public static <K, V> Map<K, V> of(K k1, V v1) {
    return singletonMap(k1, v1);
  }

  public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2) {
    Map<K, V> map = new LinkedHashMap<>();
    map.put(k1, v1);
    map.put(k2, v2);

    return unmodifiableMap(map);
  }

  public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
    Map<K, V> map = new LinkedHashMap<>();
    map.put(k1, v1);
    map.put(k2, v2);
    map.put(k3, v3);

    return unmodifiableMap(map);
  }
}
