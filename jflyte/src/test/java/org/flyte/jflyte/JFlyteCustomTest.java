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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.flyte.api.v1.Struct;
import org.junit.Test;

public class JFlyteCustomTest {

  @Test
  public void testSerializeCustomToStruct() {
    JFlyteCustom custom =
        JFlyteCustom.builder()
            .artifacts(
                ImmutableList.of(
                    Artifact.create("location0", "name0", 1337),
                    Artifact.create("location1", "name1", 1337)))
            .build();

    List<Struct.Value> expectedArtifacts =
        ImmutableList.of(
            Struct.Value.ofStructValue(
                Struct.of(
                    ImmutableMap.of(
                        "name", Struct.Value.ofStringValue("name0"),
                        "location", Struct.Value.ofStringValue("location0")))),
            Struct.Value.ofStructValue(
                Struct.of(
                    ImmutableMap.of(
                        "name", Struct.Value.ofStringValue("name1"),
                        "location", Struct.Value.ofStringValue("location1")))));

    Struct expected =
        Struct.of(
            ImmutableMap.of(
                "jflyte",
                Struct.Value.ofStructValue(
                    Struct.of(
                        ImmutableMap.of(
                            "artifacts", Struct.Value.ofListValue(expectedArtifacts))))));

    assertThat(custom.serializeToStruct(), equalTo(expected));
  }

  @Test
  public void testSerDe() {
    JFlyteCustom custom0 =
        JFlyteCustom.builder()
            .artifacts(
                ImmutableList.of(
                    Artifact.create("location1", "name1", 0),
                    Artifact.create("location2", "name2", 0)))
            .build();

    JFlyteCustom custom1 = JFlyteCustom.deserializeFromStruct(custom0.serializeToStruct());

    assertThat(custom1, equalTo(custom0));
  }
}
