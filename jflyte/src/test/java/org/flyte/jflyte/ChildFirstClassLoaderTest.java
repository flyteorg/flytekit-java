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
package org.flyte.jflyte;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ChildFirstClassLoaderTest {

  @Test
  void testGetResourcesFromChildAndParent() throws Exception {
    try (URLClassLoader classLoader =
        new ChildFirstClassLoader(new URL[] {getClass().getResource("/")})) {
      List<URL> resources = Collections.list(classLoader.getResources(thisClassAsResourceName()));

      assertEquals(2, resources.size());
    }
  }

  @Test
  void testGetResourcesOnlyFromChild() throws IOException {
    try (URLClassLoader classLoader =
        new ChildFirstClassLoader(
            new URL[] {getClass().getResource("/" + thisPackageAsResourceName() + "/")})) {
      List<URL> resources =
          Collections.list(classLoader.getResources("org/slf4j/impl/StaticLoggerBinder.class"));

      assertEquals(1, resources.size());
    }
  }

  private String thisClassAsResourceName() {
    return getClass().getName().replace(".", "/") + ".class";
  }

  private String thisPackageAsResourceName() {
    return getClass().getPackage().getName().replace(".", "/");
  }
}
