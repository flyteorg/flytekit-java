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

  public static final String IMAGE_NAME = "ghcr.io/flyteorg/flyte-sandbox:v1.1.0";

  public static final FlyteSandboxContainer INSTANCE = new FlyteSandboxContainer();

  static {
    startContainer();
  }

  private static void startContainer() {
    INSTANCE.start();

    // Flyte sandbox uses Docker in Docker, we have to copy jflyte container into inner Docker
    // otherwise, flytepropeller can't use the right version for pod execution

    DockerClient client = DockerClientFactory.instance().client();
    try (InputStream imageInputStream = client.saveImageCmd(JFlyteContainer.IMAGE_NAME).exec()) {

      try (OutputStream outputStream = Files.newOutputStream(Paths.get("target/jflyte.tar.gz"))) {
        IOUtils.copy(imageInputStream, outputStream);
      }

      ExecResult execResult = INSTANCE.execInContainer("docker", "load", "-i", "integration-tests/target/jflyte.tar.gz");

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
        30081, // flyteadmin
        30082, // k8s dashboard
        30084, // minio
        30086 // k8s api
        );

    withReuse(true);

    withNetwork(FlyteSandboxNetwork.INSTANCE);

    waitingFor(Wait.forLogMessage(".*Flyte is ready!.*", 1));
    withStartupTimeout(Duration.ofMinutes(5));
  }

  @Override
  public void start() {
    super.start();

    logger().info("Flyte is ready!");

    String consoleUri =
        String.format("http://%s:%d/console", getContainerIpAddress(), getMappedPort(30081));
    String k8sUri = String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(30082));

    logger().info("Flyte UI is available at " + consoleUri);
    logger().info("K8s dashboard is available at " + k8sUri);
  }
}
