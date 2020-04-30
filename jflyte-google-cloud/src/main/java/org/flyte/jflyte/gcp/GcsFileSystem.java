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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.MustBeClosed;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.Manifest;

/** Implementation of {@link FileSystem} for Google Cloud Storage. */
public class GcsFileSystem implements FileSystem {
  private static final String SCHEME = "gs";
  private static final Pattern GCS_URI =
      Pattern.compile("(?<SCHEME>[^:]+)://(?<BUCKET>[^/]+)(/(?<OBJECT>.*))?");

  private final Storage storage;

  public GcsFileSystem() {
    storage = StorageOptions.getDefaultInstance().getService();
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  @MustBeClosed
  public ReadableByteChannel reader(String uri) {
    Blob blob = storage.get(parseUri(uri));

    if (blob == null) {
      throw new IllegalArgumentException("Resource doesn't exist: " + uri);
    }

    return blob.reader();
  }

  @Override
  public WritableByteChannel writer(String uri) {
    return storage.writer(BlobInfo.newBuilder(parseUri(uri)).build());
  }

  @Nullable
  @Override
  public Manifest getManifest(String uri) {
    Blob blob = storage.get(parseUri(uri));

    if (blob == null) {
      return null;
    }

    return Manifest.create();
  }

  @VisibleForTesting // TODO write tests
  static BlobId parseUri(String str) {
    URI uri = URI.create(str);

    checkArgument(uri.getScheme().equalsIgnoreCase(SCHEME), "Invalid GCS URI scheme [%s]", uri);
    checkArgument(uri.getPort() == -1, "Invalid GCS URI port [%s]", uri);
    checkArgument(isNullOrEmpty(uri.getFragment()), "Invalid GCS URI fragment [%s]", uri);
    checkArgument(isNullOrEmpty(uri.getQuery()), "Invalid GCS URI query [%s]", uri);
    checkArgument(isNullOrEmpty(uri.getUserInfo()), "Invalid GCS URI userInfo [%s]", uri);

    Matcher m = GCS_URI.matcher(str);
    checkArgument(m.matches(), "Invalid GCS URI [%s]", uri);

    return BlobId.of(m.group("BUCKET"), m.group("OBJECT"));
  }
}
