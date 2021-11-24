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
package org.flyte.jflyte.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectId;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.Manifest;

public class S3FileSystem implements FileSystem {
  private final AmazonS3 s3;

  private static final Logger LOG = Logger.getLogger(S3FileSystem.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  public S3FileSystem(AmazonS3 s3) {
    this.s3 = s3;
  }

  public static S3FileSystem create(Map<String, String> env) {
    String endpoint = env.get("FLYTE_AWS_ENDPOINT");
    String accessKeyId = env.get("FLYTE_AWS_ACCESS_KEY_ID");

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

    if (accessKeyId != null) {
      LOG.fine(String.format("Using FLYTE_AWS_ACCESS_KEY_ID [%s]", accessKeyId));

      String secretAccessKey = env.get("FLYTE_AWS_SECRET_ACCESS_KEY");
      BasicAWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

      builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
    }

    if (endpoint != null) {
      LOG.fine(String.format("Using FLYTE_AWS_ENDPOINT [%s]", endpoint));

      // assume it's minio from this point, it doesn't work without signer override
      ClientConfiguration clientConfiguration = new ClientConfiguration();
      clientConfiguration.setSignerOverride("AWSS3V4SignerType");

      builder.withClientConfiguration(clientConfiguration);
      builder.withPathStyleAccessEnabled(true);

      builder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.US_EAST_1.name()));
    } else {
      builder.withRegion(Regions.DEFAULT_REGION);
    }

    return new S3FileSystem(builder.build());
  }

  @Override
  public String getScheme() {
    return "s3";
  }

  @Override
  public ReadableByteChannel reader(String uri) {
    AmazonS3URI s3Uri = new AmazonS3URI(uri);
    S3ObjectId objectId = new S3ObjectId(s3Uri.getBucket(), s3Uri.getKey(), s3Uri.getVersionId());
    S3Object object = s3.getObject(new GetObjectRequest(objectId));

    return Channels.newChannel(object.getObjectContent());
  }

  @Override
  public WritableByteChannel writer(String uri) {
    AmazonS3URI s3Uri = new AmazonS3URI(uri);

    LOG.fine("bucket=" + s3Uri.getBucket() + " key=" + s3Uri.getKey());
    LOG.fine(s3.listBuckets() + "");

    try {
      return S3WritableByteChannel.create(
          s3, /* bucketName= */ s3Uri.getBucket(), /* key= */ s3Uri.getKey());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nullable
  @Override
  public Manifest getManifest(String uri) {
    AmazonS3URI s3Uri = new AmazonS3URI(uri);

    if (!s3.doesObjectExist(s3Uri.getBucket(), s3Uri.getKey())) {
      return null;
    }

    // TODO once we have fields in Manifest, populate them

    return Manifest.create();
  }
}
