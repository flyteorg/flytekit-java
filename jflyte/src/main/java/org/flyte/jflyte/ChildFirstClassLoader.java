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
package org.flyte.jflyte;

import com.google.errorprone.annotations.Var;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * {@link URLClassLoader} that loads classes into child class loader, instead of parent.
 *
 * <p>This way we can keep parent class loader clean from unnecessary classes, and load conflicting
 * classes into multiple children class loaders.
 *
 * <p>All "api" classes must be loaded in parent class loader, because they act as common interface
 * between the code in parent class loader, and the code loaded in child class loaders, and we pass
 * instances of these classes around.
 */
public class ChildFirstClassLoader extends URLClassLoader {

  private static final String FLYTE_API_NAMESPACE = "org.flyte.api.v1.";
  private static final String JFLYTE_API_NAMESPACE = "org.flyte.jflyte.api.";

  ChildFirstClassLoader(URL[] urls) {
    super(urls, Thread.currentThread().getContextClassLoader());
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {

    @Var Class<?> loadedClass = findLoadedClass(name);

    if (loadedClass != null) {
      return loadedClass;
    }

    // we have to load these classes in parent class loader
    // it's base shared between all plugins and SDK code
    if (name.startsWith(JFLYTE_API_NAMESPACE) || name.startsWith(FLYTE_API_NAMESPACE)) {
      return getParent().loadClass(name);
    }

    try {
      loadedClass = findClass(name);
    } catch (ClassNotFoundException e) {
      loadedClass = getParent().loadClass(name);
    }

    if (resolve) {
      resolveClass(loadedClass);
    }

    return loadedClass;
  }
}
