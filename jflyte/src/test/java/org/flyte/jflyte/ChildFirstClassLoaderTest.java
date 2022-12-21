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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
        new ChildFirstClassLoader(new URL[] {urlVisibleToBothChildAndParent()})) {
      List<URL> resources = Collections.list(classLoader.getResources(thisClassAsResourceName()));

      assertEquals(2, resources.size());
    }
  }

  @Test
  void testGetResourcesOnlyFromChild() throws IOException {
    try (URLClassLoader classLoader =
        new ChildFirstClassLoader(new URL[] {urlVisibleToBothChildAndParent()})) {
      List<URL> resources =
          Collections.list(classLoader.getResources("META-INF/services/org.flyte.jflyte.Foo"));

      assertEquals(1, resources.size());
    }
  }

  @Test
  void testGetResourceFromParent() throws Exception {
    try (URLClassLoader classLoader =
        new ChildFirstClassLoader(new URL[] {urlVisibleToChildOnly()})) {
      URL resource = classLoader.getResource(thisClassAsResourceName());

      assertNotNull(resource);
    }
  }

  @Test
  void testGetResourceFromChild() throws IOException {
    try (URLClassLoader classLoader =
        new ChildFirstClassLoader(new URL[] {urlVisibleToChildOnly()})) {
      URL resource = classLoader.getResource("org/slf4j/impl/StaticLoggerBinder.class");

      assertNotNull(resource);
    }
  }

  @Test
  void testGetResourceNotFound() throws IOException {
    try (URLClassLoader classLoader =
        new ChildFirstClassLoader(new URL[] {urlVisibleToChildOnly()})) {
      URL resource = classLoader.getResource("META-INF/services/org.flyte.jflyte.Foo");

      assertNull(resource);
    }
  }

  private String thisClassAsResourceName() {
    return getClass().getName().replace(".", "/") + ".class";
  }

  private String thisPackageAsResourceName() {
    return getClass().getPackage().getName().replace(".", "/");
  }

  private URL urlVisibleToBothChildAndParent() {
    return getClass().getResource("/");
  }

  private URL urlVisibleToChildOnly() {
    return getClass().getResource("/" + thisPackageAsResourceName() + "/");
  }
}
