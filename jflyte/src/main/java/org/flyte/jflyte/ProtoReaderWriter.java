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
package org.flyte.jflyte;

import flyteidl.core.DynamicJob;
import flyteidl.core.Errors;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import org.flyte.api.v1.DynamicJobSpec;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.jflyte.api.FileSystem;

class ProtoReaderWriter {
  private static final String OUTPUTS_PB = "outputs.pb";
  private static final String FUTURES_PB = "futures.pb";
  private static final String ERROR_PB = "error.pb";

  private final String outputPrefix;
  private final FileSystem inputFs;
  private final FileSystem outputFs;

  ProtoReaderWriter(String outputPrefix, FileSystem inputFs, FileSystem outputFs) {
    this.outputPrefix = outputPrefix;
    this.inputFs = inputFs;
    this.outputFs = outputFs;
  }

  interface Writer {
    void write(OutputStream os) throws IOException;
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

  void writeOutputs(Map<String, Literal> outputs) {
    String outputUri = normalizeUri(outputPrefix, OUTPUTS_PB);

    writeTo(
        outputFs,
        outputUri,
        outputStream -> {
          Literals.LiteralMap proto = ProtoUtil.serialize(outputs);
          proto.writeTo(outputStream);
        });
  }

  void writeFutures(DynamicJobSpec dynamicJobSpec) {
    String outputUri = normalizeUri(outputPrefix, FUTURES_PB);

    writeTo(
        outputFs,
        outputUri,
        outputStream -> {
          DynamicJob.DynamicJobSpec proto = ProtoUtil.serialize(dynamicJobSpec);
          proto.writeTo(outputStream);
        });
  }

  void writeError(Errors.ContainerError containerError) {
    String outputUri = normalizeUri(outputPrefix, ERROR_PB);

    writeTo(
        outputFs,
        outputUri,
        outputStream -> {
          Errors.ErrorDocument errorDocument =
              Errors.ErrorDocument.newBuilder().setError(containerError).build();

          errorDocument.writeTo(outputStream);
        });
  }

  private static void writeTo(FileSystem fs, String uri, Writer writer) {
    try (WritableByteChannel channel = fs.writer(uri);
        OutputStream os = Channels.newOutputStream(channel)) {
      writer.write(os);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String normalizeUri(String prefix, String fileName) {
    String uri;
    if (prefix.endsWith("/")) {
      uri = prefix + fileName;
    } else {
      uri = prefix + "/" + fileName;
    }
    return uri;
  }
}
