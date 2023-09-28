/*
 * Copyright 2020-2021 Flyte Authors
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

import com.amazonaws.services.s3.AmazonS3;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

class S3WritableByteChannel implements WritableByteChannel {
  private final File file;
  private final String bucketName;
  private final String key;
  private final WritableByteChannel fileChannel;
  private final AmazonS3 s3;

  // TODO ideally, we should buffer into file only if it's nescessary, but for now we
  //  got with a simple option of always creating a file given that the code isn't
  //  performance-critical

  S3WritableByteChannel(
      AmazonS3 s3, String bucketName, String key, File file, WritableByteChannel fileChannel) {
    this.s3 = s3;
    this.bucketName = bucketName;
    this.key = key;
    this.file = file;
    this.fileChannel = fileChannel;
  }

  public static S3WritableByteChannel create(AmazonS3 s3, String bucketName, String key)
      throws IOException {
    String fileName = key.replaceAll("\\W+", "_");
    File file = File.createTempFile("s3-upload", fileName);
    file.deleteOnExit();

    WritableByteChannel fileChannel = Files.newByteChannel(file.toPath(), StandardOpenOption.WRITE);

    return new S3WritableByteChannel(
        s3,
        /* bucketName= */ bucketName,
        /* key= */ key,
        /* file= */ file,
        /* fileChannel= */ fileChannel);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return fileChannel.write(src);
  }

  @Override
  public boolean isOpen() {
    return fileChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();

    s3.putObject(/* bucketName= */ bucketName, /* key= */ key, file);
  }
}
