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
package org.flyte.flytekit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Struct;
import org.junit.jupiter.api.Test;

public class SdkStructTest {

  @Test
  void shouldConvertToStruct() {
    List<Struct.Value> values = new ArrayList<>();
    values.add(Struct.Value.ofNumberValue(1.0));
    values.add(Struct.Value.ofNumberValue(2.0));
    values.add(Struct.Value.ofNumberValue(3.0));

    Map<String, Struct.Value> inner = new HashMap<>();
    inner.put("stringValue", Struct.Value.ofStringValue("string"));

    Map<String, Struct.Value> fields = new HashMap<>();
    fields.put("stringValue", Struct.Value.ofStringValue("string"));
    fields.put("boolValue", Struct.Value.ofBoolValue(true));
    fields.put("integerValue", Struct.Value.ofNumberValue(42));
    fields.put("listValue", Struct.Value.ofListValue(values));
    fields.put("nullValue", Struct.Value.ofNullValue());
    fields.put("structValue", Struct.Value.ofStructValue(Struct.of(inner)));

    Struct expected = Struct.of(fields);

    SdkStruct sdkStruct =
        SdkStruct.builder()
            .addStringField("stringValue", "string")
            .addBooleanField("boolValue", true)
            .addIntegerField("integerValue", 42L)
            .addIntegerField("nullValue", null)
            .addIntegerCollectionField("listValue", Arrays.asList(1L, 2L, 3L))
            .addStructField(
                "structValue", SdkStruct.builder().addStringField("stringValue", "string").build())
            .build();

    assertThat(sdkStruct.struct(), equalTo(expected));
  }
}
