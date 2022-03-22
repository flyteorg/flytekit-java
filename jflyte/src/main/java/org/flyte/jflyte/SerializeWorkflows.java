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
package org.flyte.jflyte;

import static org.flyte.jflyte.TokenSourceFactoryLoader.getTokenSource;

import com.google.errorprone.annotations.Var;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.flyte.jflyte.api.TokenSource;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Serializes all workflows on classpath. */
@Command(name = "workflows")
public class SerializeWorkflows implements Callable<Integer> {

  private static final String PROJECT_PLACEHOLDER = "{{ registration.project }}";
  private static final String DOMAIN_PLACEHOLDER = "{{ registration.version }}";
  private static final String VERSION_PLACEHOLDER = "{{ registration.domain }}";

  @Option(
      names = {"-cp", "--classpath"},
      description = "Directory with packaged jars",
      required = true)
  private String packageDir;

  @Nullable
  @Option(
      names = {"-am", "--auth-mode"},
      description = "Authentication method used to retrieve token",
      required = false)
  private String authMode;

  @Option(
      names = {"-f", "--folder"},
      description = "Output directory",
      required = true)
  private String folder;

  @Override
  public Integer call() {
    Config config = Config.load();
    Collection<ClassLoader> modules = ClassLoaders.forModuleDir(config.moduleDir()).values();

    TokenSource tokenSource = (authMode == null) ? null : getTokenSource(modules, authMode);

    try (FlyteAdminClient adminClient =
        FlyteAdminClient.create(config.platformUrl(), config.platformInsecure(), tokenSource)) {
      Supplier<ArtifactStager> stagerSupplier = () -> ArtifactStager.create(config, modules);
      ExecutionConfig executionConfig =
          ExecutionConfig.builder()
              .domain(DOMAIN_PLACEHOLDER)
              .version(VERSION_PLACEHOLDER)
              .project(PROJECT_PLACEHOLDER)
              .image(config.image())
              .build();

      ProjectClosure closure =
          ProjectClosure.loadAndStage(packageDir, executionConfig, stagerSupplier, adminClient);

      closure.serialize(fileWriter(folder));
    }

    return 0;
  }

  private static BiConsumer<String, ByteString> fileWriter(String folder) {
    return (filename, bytes) -> {
      @Var FileOutputStream fos = null;

      // ugly because spotbugs doesn't like try-with-resources
      try {
        fos = new FileOutputStream(new File(folder, filename));
        bytes.writeTo(fos);
        fos.flush();
      } catch (IOException e) {
        throw new UncheckedIOException("failed to write to " + folder + "/" + filename, e);
      } finally {
        if (fos != null) {
          try {
            fos.close();
          } catch (IOException e) {
            // ignore, any significant error should happen during flush
          }
        }
      }
    };
  }
}
