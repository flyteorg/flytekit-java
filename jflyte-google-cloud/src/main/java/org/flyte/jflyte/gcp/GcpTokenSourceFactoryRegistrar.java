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
package org.flyte.jflyte.gcp;

import static java.util.Collections.singletonList;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.flyte.jflyte.api.TokenSourceFactory;
import org.flyte.jflyte.api.TokenSourceFactoryRegistrar;

/** Registrar for {@link GcpTokenSourceFactory}. */
@AutoService(TokenSourceFactoryRegistrar.class)
public class GcpTokenSourceFactoryRegistrar extends TokenSourceFactoryRegistrar {
  static final String FLYTE_PLATFORM_URL = "FLYTE_PLATFORM_URL";

  private final Function<String, TokenSourceFactory> factoryFn;

  public GcpTokenSourceFactoryRegistrar() {
    this(GcpTokenSourceFactory::new);
  }

  @VisibleForTesting
  GcpTokenSourceFactoryRegistrar(Function<String, TokenSourceFactory> factoryFn) {
    this.factoryFn = Objects.requireNonNull(factoryFn);
  }

  @Override
  public Iterable<TokenSourceFactory> load(Map<String, String> env) {
    String platformUrl = env.get(FLYTE_PLATFORM_URL);
    Preconditions.checkArgument(
        !Strings.nullToEmpty(platformUrl).trim().isEmpty(),
        "FLYTE_PLATFORM_URL env var must be a non empty string: " + platformUrl);

    return singletonList(factoryFn.apply(platformUrl));
  }
}
