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

import com.google.api.services.storage.StorageScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auto.service.AutoService;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.FileSystemRegistrar;

/** Registrar for {@link GcsFileSystem}. */
@AutoService(FileSystemRegistrar.class)
public class GcsFileSystemRegistrar extends FileSystemRegistrar {
  private static final Logger LOG = Logger.getLogger(GcsFileSystemRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  /** Experimental: enables service account impersonalization. */
  private static final String GCP_IMPERSONATE_SERVICE_ACCOUNT_KEY =
      "GCP_IMPERSONATE_SERVICE_ACCOUNT";

  @Override
  public Iterable<FileSystem> load(Map<String, String> env) {
    Credentials credentials = getCredentials(env);
    StorageOptions options =
        StorageOptions.getDefaultInstance().toBuilder().setCredentials(credentials).build();

    return Collections.singletonList(new GcsFileSystem(options.getService()));
  }

  private static Credentials getCredentials(Map<String, String> env) {
    String impersonateServiceAccount = env.get(GCP_IMPERSONATE_SERVICE_ACCOUNT_KEY);

    try {
      GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();

      if (impersonateServiceAccount != null) {
        LOG.info(String.format("Using impersonated credentials [%s]", impersonateServiceAccount));

        return ImpersonatedCredentials.create(
            sourceCredentials,
            impersonateServiceAccount,
            /* delegates= */ null,
            /* scopes= */ ImmutableList.of(StorageScopes.DEVSTORAGE_READ_WRITE),
            /* lifetime= */ 3600 /* 1 hour */);
      } else {
        return sourceCredentials;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
