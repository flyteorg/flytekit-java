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
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

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
class ChildFirstClassLoader extends URLClassLoader {

  // we have to load these classes in parent class loader
  // it's base shared between all plugins and user code
  private static final String[] PARENT_FIRST_PACKAGE_PREFIXES =
      new String[] {"org.flyte.api.v1.", "org.flyte.jflyte.api."};

  @SuppressWarnings("JdkObsolete")
  private static class CustomEnumeration implements Enumeration<URL> {

    private final Iterator<URL> iter;

    CustomEnumeration(Iterator<URL> iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasMoreElements() {
      return iter.hasNext();
    }

    @Override
    public URL nextElement() {
      return iter.next();
    }
  }

  ChildFirstClassLoader(URL[] urls) {
    super(urls, ChildFirstClassLoader.class.getClassLoader());
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    @Var Class<?> cls = findLoadedClass(name);

    if (cls == null) {
      for (String prefix : PARENT_FIRST_PACKAGE_PREFIXES) {
        if (name.startsWith(prefix)) {
          return super.loadClass(name, resolve);
        }
      }

      try {
        cls = findClass(name);
      } catch (ClassNotFoundException e) {
        cls = getParent().loadClass(name);
      }
    }

    if (resolve) {
      resolveClass(cls);
    }

    return cls;
  }

  @Override
  public URL getResource(String name) {
    URL resource = findResource(name);

    if (resource != null) {
      return resource;
    }

    return getParent().getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> childResources = findResources(name);

    List<URL> allResources = new ArrayList<>();

    while (childResources.hasMoreElements()) {
      allResources.add(childResources.nextElement());
    }

    Enumeration<URL> parentResources = getParent().getResources(name);

    while (parentResources.hasMoreElements()) {
      allResources.add(parentResources.nextElement());
    }

    return new CustomEnumeration(allResources.iterator());
  }
}
