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
package org.flyte.jflyte.gcp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GcsFileSystemTest {

  @Mock private Storage storage;

  private GcsFileSystem gcsFs;

  @BeforeEach
  void setUp() {
    gcsFs = new GcsFileSystem(() -> storage);
  }

  @Test
  void testGetScheme() {
    assertThat(gcsFs.getScheme(), equalTo("gs"));
  }

  @SuppressWarnings("MustBeClosedChecker")
  @Test
  void testReaderPropagatesCallToStorage() {
    Blob mockerReturn = mock(Blob.class);
    when(storage.get(any(BlobId.class))).thenReturn(mockerReturn);
    ReadChannel expectedChannel = mock(ReadChannel.class);
    when(mockerReturn.reader()).thenReturn(expectedChannel);

    ReadableByteChannel actualChannel = gcsFs.reader("gs://bucket/path/to/file");

    assertThat(actualChannel, sameInstance(expectedChannel));
    verify(storage).get(BlobId.of("bucket", "path/to/file"));
  }

  @SuppressWarnings("MustBeClosedChecker")
  @Test
  void testWriterPropagatesCallToStorage() {
    WriteChannel expectedChannel = mock(WriteChannel.class);
    when(storage.writer(any(BlobInfo.class))).thenReturn(expectedChannel);

    WritableByteChannel actualChannel = gcsFs.writer("gs://bucket/path/to/file");

    assertThat(actualChannel, sameInstance(expectedChannel));
    verify(storage).writer(BlobInfo.newBuilder("bucket", "path/to/file").build());
  }

  @Test
  void testGetManifestPropagatesToStorage() {
    WriteChannel expectedChannel = mock(WriteChannel.class);
    when(storage.writer(any(BlobInfo.class))).thenReturn(expectedChannel);

    WritableByteChannel actualChannel = gcsFs.writer("gs://bucket/path/to/file");

    assertThat(actualChannel, sameInstance(expectedChannel));
    verify(storage).writer(BlobInfo.newBuilder("bucket", "path/to/file").build());
  }

  @ParameterizedTest
  @CsvSource({
    "ftp://bucket/file,Invalid GCS URI scheme [ftp://bucket/file]",
    "gs://bucket:123/file,Invalid GCS URI port [gs://bucket:123/file]",
    "gs://bucket/file#section,Invalid GCS URI fragment [gs://bucket/file#section]",
    "gs://bucket/file?format=xml,Invalid GCS URI query [gs://bucket/file?format=xml]",
    "gs://user@bucket/file,Invalid GCS URI userInfo [gs://user@bucket/file]",
    "not an uri,Illegal character in path at index 3: not an uri",
  })
  void testParseUriRejectsInvalidUris(String uri, String expectedErrMsg) {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> GcsFileSystem.parseUri(uri));

    assertThat(exception.getMessage(), equalTo(expectedErrMsg));
  }

  @Test
  void testReaderReportsUriOnStorageExceptions() {
    doThrow(StorageException.class).when(storage).get(any(BlobId.class));

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> gcsFs.reader("gs://bucket/file"));

    assertThat(exception.getMessage(), equalTo("Couldn't read resource: gs://bucket/file"));
  }

  @Test
  void testWriterReportsUriOnStorageExceptions() {
    doThrow(StorageException.class).when(storage).writer(any(BlobInfo.class));

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> gcsFs.writer("gs://bucket/file"));

    assertThat(exception.getMessage(), equalTo("Couldn't write resource: gs://bucket/file"));
  }

  @Test
  void testGetManifestReportsUriOnStorageExceptions() {
    doThrow(StorageException.class).when(storage).get(any(BlobId.class));

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> gcsFs.getManifest("gs://bucket/file"));

    assertThat(
        exception.getMessage(), equalTo("Couldn't get manifest for resource: gs://bucket/file"));
  }
}
