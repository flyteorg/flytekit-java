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
package org.flyte.jflyte.utils;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import flyteidl.core.DynamicJob;
import flyteidl.core.Errors;
import flyteidl.core.Literals;
import flyteidl.core.Types;
import flyteidl.core.Workflow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.DynamicJobSpec;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.Manifest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ProtoWriterTest {
  @RegisterExtension final FileSystemExtension extension = new FileSystemExtension();

  @BeforeEach
  void setUp() throws IOException {
    Path stagedPath = extension.getFileSystem().getPath("/home/test/prefix");
    Files.createDirectories(stagedPath);
  }

  @Test
  void shouldWriteOutput() throws IOException {
    ProtoWriter protoWriter =
        new ProtoWriter("/home/test/prefix", new InMemoryFileSystem(extension.getFileSystem()));

    Map<String, Literal> outputs =
        singletonMap("foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("bar"))));
    protoWriter.writeOutputs(outputs);

    Path outputPath = extension.getFileSystem().getPath("/home/test/prefix/outputs.pb");
    Literals.LiteralMap actualOutputs =
        Literals.LiteralMap.parseFrom(Files.newInputStream(outputPath));
    assertThat(
        actualOutputs,
        equalTo(
            Literals.LiteralMap.newBuilder()
                .putLiterals(
                    "foo",
                    Literals.Literal.newBuilder()
                        .setScalar(
                            Literals.Scalar.newBuilder()
                                .setPrimitive(
                                    Literals.Primitive.newBuilder().setStringValue("bar").build())
                                .build())
                        .build())
                .build()));
  }

  @Test
  void shouldWriteContainerError() throws IOException {
    ProtoWriter protoWriter =
        new ProtoWriter("/home/test/prefix", new InMemoryFileSystem(extension.getFileSystem()));

    Errors.ContainerError containerError =
        Errors.ContainerError.newBuilder().setCode("Err").build();
    protoWriter.writeError(containerError);

    Path errPath = extension.getFileSystem().getPath("/home/test/prefix/error.pb");
    Errors.ErrorDocument actualError =
        Errors.ErrorDocument.parseFrom(Files.newInputStream(errPath));
    assertThat(
        actualError, equalTo(Errors.ErrorDocument.newBuilder().setError(containerError).build()));
  }

  @Test
  void shouldThrowExceptionWhenNotAbleToWrite() {
    ProtoWriter protoWriter = new ProtoWriter("/home/test/prefix", new BrokenFileSystem());
    Map<String, Literal> outputs =
        singletonMap("foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("bar"))));

    UncheckedIOException exception =
        assertThrows(UncheckedIOException.class, () -> protoWriter.writeOutputs(outputs));

    assertThat(exception.getMessage(), equalTo("cannot write"));
  }

  @Test
  void shouldWriteFutures() throws IOException {
    ProtoWriter protoWriter =
        new ProtoWriter("/home/test/prefix", new InMemoryFileSystem(extension.getFileSystem()));

    DynamicJobSpec spec =
        DynamicJobSpec.builder()
            .nodes(
                singletonList(
                    Node.builder()
                        .id("echo")
                        .inputs(
                            singletonList(
                                Binding.builder()
                                    .var_("in")
                                    .binding(
                                        BindingData.ofScalar(
                                            Scalar.ofPrimitive(Primitive.ofStringValue("ping"))))
                                    .build()))
                        .upstreamNodeIds(emptyList())
                        .build()))
            .outputs(
                singletonList(
                    Binding.builder()
                        .var_("out")
                        .binding(
                            BindingData.ofOutputReference(
                                OutputReference.builder().nodeId("echo").var("out").build()))
                        .build()))
            .subWorkflows(emptyMap())
            .tasks(emptyMap())
            .build();
    protoWriter.writeFutures(spec);

    Path futurePath = extension.getFileSystem().getPath("/home/test/prefix/futures.pb");
    DynamicJob.DynamicJobSpec actualError =
        DynamicJob.DynamicJobSpec.parseFrom(Files.newInputStream(futurePath));
    assertThat(
        actualError,
        equalTo(
            DynamicJob.DynamicJobSpec.newBuilder()
                .addNodes(
                    Workflow.Node.newBuilder()
                        .setId("echo")
                        .addInputs(
                            Literals.Binding.newBuilder()
                                .setVar("in")
                                .setBinding(
                                    Literals.BindingData.newBuilder()
                                        .setScalar(
                                            Literals.Scalar.newBuilder()
                                                .setPrimitive(
                                                    Literals.Primitive.newBuilder()
                                                        .setStringValue("ping")
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .addOutputs(
                    Literals.Binding.newBuilder()
                        .setVar("out")
                        .setBinding(
                            Literals.BindingData.newBuilder()
                                .setPromise(
                                    Types.OutputReference.newBuilder()
                                        .setNodeId("echo")
                                        .setVar("out")
                                        .build())
                                .build())
                        .build())
                .build()));
  }

  private static class BrokenFileSystem implements FileSystem {

    @Override
    public String getScheme() {
      return "file";
    }

    @Override
    public ReadableByteChannel reader(String uri) {
      throw new UncheckedIOException("cannot read", new IOException());
    }

    @Override
    public WritableByteChannel writer(String uri) {
      throw new UncheckedIOException("cannot write", new IOException());
    }

    @Nullable
    @Override
    public Manifest getManifest(String uri) {
      throw new UncheckedIOException("cannot read", new IOException());
    }
  }
}
