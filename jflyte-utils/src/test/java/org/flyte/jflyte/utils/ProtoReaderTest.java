/*
 * Copyright 2021-2023 Flyte Authors
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.protobuf.MessageLite;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ProtoReaderTest {
  @RegisterExtension final FileSystemExtension extension = new FileSystemExtension();

  @BeforeEach
  void setUp() throws IOException {
    Path stagedPath = extension.getFileSystem().getPath("/test");
    Files.createDirectories(stagedPath);
  }

  @Test
  void shouldReadTaskTemplate() throws IOException {
    Tasks.TaskTemplate template =
        Tasks.TaskTemplate.newBuilder()
            .setType("jflyte")
            .setContainer(
                Tasks.Container.newBuilder()
                    .setImage("image")
                    .addCommand("jflyte")
                    .addArgs("arg1")
                    .addArgs("arg2")
                    .build())
            .build();
    Path templatePath = extension.getFileSystem().getPath("/test/template");
    writeProto(template, templatePath);
    ProtoReader protoReader = new ProtoReader(new InMemoryFileSystem(extension.getFileSystem()));

    TaskTemplate actual = protoReader.getTaskTemplate("/test/template");

    assertThat(
        actual,
        equalTo(
            TaskTemplate.builder()
                .type("jflyte")
                .container(
                    Container.builder()
                        .image("image")
                        .command(singletonList("jflyte"))
                        .args(asList("arg1", "arg2"))
                        .env(emptyList())
                        .build())
                .interface_(TypedInterface.builder().inputs(emptyMap()).outputs(emptyMap()).build())
                .retries(RetryStrategy.builder().retries(0).build())
                .custom(Struct.of(emptyMap()))
                .discoverable(false)
                .cacheSerializable(false)
                .build()));
  }

  @Test
  void shouldReadInputs() throws IOException {
    Literals.LiteralMap input =
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
            .build();
    Path inputPath = extension.getFileSystem().getPath("/test/input.pb");
    writeProto(input, inputPath);
    ProtoReader protoReader = new ProtoReader(new InMemoryFileSystem(extension.getFileSystem()));

    Map<String, Literal> actual = protoReader.getInput("/test/input.pb");

    assertThat(
        actual,
        equalTo(
            singletonMap(
                "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("bar"))))));
  }

  private void writeProto(MessageLite proto, Path path) throws IOException {
    try (OutputStream outputStream = Files.newOutputStream(path)) {
      proto.writeTo(outputStream);
    }
  }
}
