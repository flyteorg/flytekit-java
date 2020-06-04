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
package org.flyte.jflyte.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ThreadLocalRandom;
import org.flyte.jflyte.api.Manifest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class S3FileSystemIT {

  @Container public LocalStackContainer localStack = new LocalStackContainer().withServices(S3);

  private AmazonS3 s3;

  @BeforeEach
  public void setUp() {
    s3 =
        AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(localStack.getEndpointConfiguration(S3))
            .withCredentials(localStack.getDefaultCredentialsProvider())
            .build();

    s3.createBucket("flyteorg");
  }

  @Test
  public void testWriteAndRead() throws IOException {
    S3FileSystem fileSystem = new S3FileSystem(s3);
    String uri = "s3://flyteorg/0z/9bea2470f7a802f23abf84dc64cd8982";

    byte[] inputBytes = new byte[42];
    ThreadLocalRandom.current().nextBytes(inputBytes);

    try (WritableByteChannel writer = fileSystem.writer(uri)) {
      ByteArrayInputStream input = new ByteArrayInputStream(inputBytes);
      IOUtils.copy(input, Channels.newOutputStream(writer));
    }

    byte[] outputBytes;
    try (ReadableByteChannel reader = fileSystem.reader(uri)) {
      outputBytes = IOUtils.toByteArray(Channels.newInputStream(reader));
    }

    Manifest manifest = fileSystem.getManifest(uri);

    assertArrayEquals(inputBytes, outputBytes);
    assertEquals(Manifest.create(), manifest);
  }

  @Test
  public void testFileNotExists() {
    S3FileSystem fileSystem = new S3FileSystem(s3);
    String uri = "s3://flyteorg/0z/9bea2470f7a802f23abf84dc64cd8982";

    Manifest manifest = fileSystem.getManifest(uri);

    assertNull(manifest);
  }
}
