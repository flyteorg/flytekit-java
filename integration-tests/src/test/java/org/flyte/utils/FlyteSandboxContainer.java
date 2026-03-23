/*
 * Copyright 2021-2022 Flyte Authors.
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

import com.github.dockerjava.api.DockerClient;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import org.apache.commons.compress.utils.IOUtils;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class FlyteSandboxContainer extends GenericContainer<FlyteSandboxContainer> {

  public static final String IMAGE_NAME =
      "ghcr.io/flyteorg/flyte-sandbox-bundled:sha-f3ab1b7480bad4072f7ecb695660fdf47032a6c4";

  public static final FlyteSandboxContainer INSTANCE =
      new FlyteSandboxContainer()
          // Note to the developer: to enable test container reuse, please do the following
          // echo testcontainers.reuse.enable=true > ~/.testcontainers.properties
          .withReuse(true);

  static {
    startContainer();
  }

  private static void startContainer() {
    INSTANCE.start();

    // Flyte sandbox-bundled uses k3s with containerd (not Docker-in-Docker),
    // so we use ctr to import the jflyte image into the inner containerd
    // otherwise, flytepropeller can't use the right version for pod execution

    DockerClient client = DockerClientFactory.instance().client();
    try (InputStream imageInputStream = client.saveImageCmd(JFlyteContainer.IMAGE_NAME).exec()) {

      try (OutputStream outputStream = Files.newOutputStream(Paths.get("target/jflyte.tar"))) {
        IOUtils.copy(imageInputStream, outputStream);
      }

      // for some reason, when running on Mac, the above copied file is not fully ready after the
      // stream being closed; sleeping a little bit could work around that
      Thread.sleep(1000);

      ExecResult execResult =
          INSTANCE.execInContainer(
              "ctr",
              "--address",
              "/run/k3s/containerd/containerd.sock",
              "--namespace",
              "k8s.io",
              "images",
              "import",
              "integration-tests/target/jflyte.tar");

      if (execResult.getExitCode() != 0) {
        throw new RuntimeException(execResult.getStderr() + " " + execResult.getStdout());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("failed to load jflyte image", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("failed to load jflyte image", e);
    }
  }

  FlyteSandboxContainer() {
    super(IMAGE_NAME);

    String workingDir = new File("../.").getAbsolutePath();

    withPrivilegedMode(true);

    withNetworkAliases("flyte");

    withWorkingDirectory(workingDir);
    withFileSystemBind(workingDir, workingDir, BindMode.READ_ONLY);

    withExposedPorts(
        30080, // envoy proxy (flyteadmin http + grpc)
        30002 // minio
        );

    withReuse(true);

    withNetwork(FlyteSandboxNetwork.INSTANCE);

    waitingFor(Wait.forHttp("/healthcheck").forPort(30080));
    withStartupTimeout(Duration.ofMinutes(5));
  }

  @Override
  public void start() {
    super.start();

    logger().info("Flyte is ready!");

    String consoleUri = String.format("http://%s:%d/console", getHost(), getMappedPort(30080));

    logger().info("Flyte UI is available at " + consoleUri);
  }
}
