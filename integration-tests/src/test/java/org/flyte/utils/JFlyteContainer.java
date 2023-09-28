/*
 * Copyright 2021-2022 Flyte Authors
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
package org.flyte.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

class JFlyteContainer extends GenericContainer<JFlyteContainer> {
  static final String IMAGE_NAME;
  static final Map<String, String> envVars =
      ImmutableMap.<String, String>builder()
          .put("FLYTE_PLATFORM_URL", "flyte:30081")
          .put("FLYTE_PLATFORM_INSECURE", "True")
          .put("FLYTE_AWS_ENDPOINT", "http://flyte:30084")
          .put("FLYTE_AWS_ACCESS_KEY_ID", "minio")
          .put("FLYTE_AWS_SECRET_ACCESS_KEY", "miniostorage")
          .put("FLYTE_STAGING_LOCATION", "s3://my-s3-bucket")
          .build();

  static {
    // jflyte/target/jib-image.json
    // example of contents
    // {"image":"ghcr.io/flyteorg/flytekit-java","imageId":"sha256:....","imageDigest":"sha256:...","tags":["0.3.19-SNAPSHOT","latest"],"imagePushed":false}

    try {
      ObjectMapper objectMapper = new ObjectMapper();
      File imageNameFile = new File("../jflyte/target/jib-image.json");
      JsonNode jsonNode = objectMapper.readTree(imageNameFile);
      IMAGE_NAME = jsonNode.get("image").asText() + ':' + jsonNode.get("tags").get(0).asText();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to get image name", e);
    }
  }

  JFlyteContainer(String[] cmd) {
    super(IMAGE_NAME);

    String workingDir = new File("../.").getAbsolutePath();

    withEnv(envVars);
    withWorkingDirectory(workingDir);
    withFileSystemBind(workingDir, workingDir);
    withCommand(cmd);
    waitingFor(new NotRunningWaitStrategy());

    withNetwork(FlyteSandboxNetwork.INSTANCE);

    withLogConsumer(new Slf4jLogConsumer(logger()));
  }
}
