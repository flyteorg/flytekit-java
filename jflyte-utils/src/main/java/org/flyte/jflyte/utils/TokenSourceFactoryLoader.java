/*
 * Copyright 2021-2023 Flyte Authors.
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

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.flyte.jflyte.api.TokenSource;
import org.flyte.jflyte.api.TokenSourceFactory;
import org.flyte.jflyte.api.TokenSourceFactoryRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TokenSource loader. */
public class TokenSourceFactoryLoader {
  private static final Logger LOG = LoggerFactory.getLogger(TokenSourceFactoryLoader.class);

  public static TokenSource getTokenSource(Collection<ClassLoader> modules, String name) {
    return modules.stream()
        .flatMap(
            module -> ClassLoaders.withClassLoader(module, () -> loadTokenFactorySources(name)))
        .map(TokenSourceFactory::getTokenSource)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("auth-mode not supported: " + name));
  }

  private static Stream<TokenSourceFactory> loadTokenFactorySources(String name) {
    ServiceLoader<TokenSourceFactoryRegistrar> loader =
        ServiceLoader.load(TokenSourceFactoryRegistrar.class);

    LOG.debug("Discovering TokenSourceFactoryRegistrar");

    Map<String, String> env = ImmutableMap.copyOf(System.getenv());

    for (TokenSourceFactoryRegistrar registrar : loader) {
      for (TokenSourceFactory tokenSourceFactory : registrar.load(env)) {
        LOG.debug(
            String.format(
                "Discovered TokenSourceFactory for method=%s [%s]",
                tokenSourceFactory.getMethod(), tokenSourceFactory.getClass().getName()));

        if (name.equals(tokenSourceFactory.getMethod())) {
          return Stream.of(tokenSourceFactory);
        }
      }
    }
    return Stream.empty();
  }
}
