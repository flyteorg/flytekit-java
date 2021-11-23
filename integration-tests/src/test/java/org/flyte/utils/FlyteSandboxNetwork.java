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

import com.github.dockerjava.api.command.CreateNetworkCmd;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.rules.ExternalResource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.ResourceReaper;

// see https://github.com/testcontainers/testcontainers-java/issues/3081

class FlyteSandboxNetwork extends ExternalResource implements Network {
  private static final String NAME = "flyte-sandbox";
  private final AtomicBoolean initialized = new AtomicBoolean();
  private String id;

  static final Network INSTANCE = new FlyteSandboxNetwork();

  private FlyteSandboxNetwork() {}

  @Override
  public String getId() {
    if (initialized.compareAndSet(false, true)) {
      id = create();
    }

    return id;
  }

  private String create() {
    List<com.github.dockerjava.api.model.Network> existing =
        DockerClientFactory.instance().client().listNetworksCmd().withNameFilter(NAME).exec();

    if (!existing.isEmpty()) {
      return existing.get(0).getId();
    }

    CreateNetworkCmd createNetworkCmd = DockerClientFactory.instance().client().createNetworkCmd();

    createNetworkCmd.withName(NAME);
    createNetworkCmd.withCheckDuplicate(true);

    Map<String, String> dockerLabels = createNetworkCmd.getLabels();
    Map<String, String> allLabels = new HashMap<>();

    if (dockerLabels != null) {
      allLabels.putAll(dockerLabels);
    }

    allLabels.putAll(DockerClientFactory.DEFAULT_LABELS);

    return createNetworkCmd.withLabels(allLabels).exec().getId();
  }

  @Override
  protected void after() {
    close();
  }

  @Override
  public void close() {
    if (initialized.getAndSet(false)) {
      ResourceReaper.instance().removeNetworkById(NAME);
    }
  }
}
