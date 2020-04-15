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

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility to work with class loaders. */
class ClassLoaders {

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaders.class);

  public static ClassLoader forDirectory(String dir) {
    return AccessController.doPrivileged(
        (PrivilegedAction<ClassLoader>) () -> new URLClassLoader(getClassLoaderUrls(dir)));
  }

  public static URL[] getClassLoaderUrls(String dir) {
    Preconditions.checkNotNull(dir, "dir is null");

    File file = new File(dir);

    LOG.debug("Loading jars from [{}]", dir);

    if (!file.exists()) {
      throw new RuntimeException(
          String.format("Directory doesn't exist [%s]", file.getAbsolutePath()));
    }

    File[] files = file.listFiles();

    if (!file.isDirectory() || files == null) {
      throw new RuntimeException(String.format("Isn't directory [%s]", file.getAbsolutePath()));
    }

    return Stream.of(files)
        .map(
            x -> {
              try {
                URL url = x.toURI().toURL();
                LOG.debug("Discovered [{}]", url);

                return url;
              } catch (MalformedURLException e) {
                throw new RuntimeException(e);
              }
            })
        .toArray(URL[]::new);
  }
}
