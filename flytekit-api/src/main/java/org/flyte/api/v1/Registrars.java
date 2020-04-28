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
package org.flyte.api.v1;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class consists exclusively of static methods that operate on {@link Registrar}. */
public class Registrars {
  private static final Logger LOG = LoggerFactory.getLogger(Registrars.class);

  public static <K, V, T extends Registrar<K, V>> Map<K, V> loadAll(
      Class<T> registrarClass, ClassLoader classLoader, Map<String, String> env) {
    ServiceLoader<T> loader = ServiceLoader.load(registrarClass, classLoader);

    LOG.debug("Discovering " + registrarClass.getSimpleName());

    Map<K, V> items = new HashMap<>();

    for (T registrar : loader) {
      LOG.debug("Discovered [{}]", registrar.getClass().getName());

      for (Map.Entry<K, V> entry : registrar.load(classLoader, env).entrySet()) {
        V previous = items.put(entry.getKey(), entry.getValue());

        if (previous != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Discovered a duplicate item [%s] [%s] [%s]",
                  entry.getKey(), entry.getValue(), previous));
        }
      }
    }

    return items;
  }
}
