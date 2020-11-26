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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class StringUtilTest {

  @Test
  void shouldSerializeLiteralMap() {
    Map<String, Literal> input = new HashMap<>();

    Literal integer = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(1337L)));
    Literal map = Literal.ofMap(singletonMap("b", integer));
    Literal list = Literal.ofCollection(singletonList(integer));

    input.put("string", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("string"))));
    input.put("integer", integer);
    input.put("float", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloat(2.0))));
    input.put("boolean", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBoolean(true))));
    input.put("datetime", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(Instant.ofEpochSecond(60L)))));
    input.put("duration", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(Duration.ofSeconds(61)))));
    input.put("list", list);
    input.put("map", map);
    input.put("listOfList", Literal.ofCollection(ImmutableList.of(list, integer)));
    input.put("mapOfMap", Literal.ofMap(ImmutableMap.of("a", map, "c", integer)));

    Map<String, String> expected = new HashMap<>();
    expected.put("string", "string");
    expected.put("integer", "1337");
    expected.put("float", "2.0");
    expected.put("boolean", "true");
    expected.put("datetime", "1970-01-01T00:01:00Z");
    expected.put("duration", "PT1M1S");
    expected.put("list", "[1337]");
    expected.put("listOfList", "[[1337], 1337]");
    expected.put("map", "{b=1337}");
    expected.put("mapOfMap", "{a={b=1337}, c=1337}");

    Map<String, String> output = StringUtil.serializeLiteralMap(input);

    Assert.assertEquals(expected, output);
  }
}
