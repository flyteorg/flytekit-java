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

import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.jflyte.api.FileSystem;

/**
 * ProtocolBuffer file reader helper.
 */
class ProtoReader {
  private final FileSystem inputFs;

  ProtoReader(FileSystem inputFs) {
    this.inputFs = inputFs;
  }

  TaskTemplate getTaskTemplate(String uri) {
    try (ReadableByteChannel channel = inputFs.reader(uri)) {
      Tasks.TaskTemplate proto = Tasks.TaskTemplate.parseFrom(Channels.newInputStream(channel));

      return ProtoUtil.deserialize(proto);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  Map<String, Literal> getInput(String uri) {
    try (ReadableByteChannel channel = inputFs.reader(uri)) {
      Literals.LiteralMap proto = Literals.LiteralMap.parseFrom(Channels.newInputStream(channel));

      return ProtoUtil.deserialize(proto);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
