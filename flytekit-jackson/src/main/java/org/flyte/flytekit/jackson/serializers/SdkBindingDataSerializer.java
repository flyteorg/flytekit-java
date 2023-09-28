/*
 * Copyright 2020-2023 Flyte Authors
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
package org.flyte.flytekit.jackson.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.flyte.flytekit.SdkBindingData;

public class SdkBindingDataSerializer extends StdSerializer<SdkBindingData<?>> {
  private static final long serialVersionUID = 0L;

  public SdkBindingDataSerializer() {
    super(SdkBindingData.class, true);
  }

  @Override
  public void serialize(
      SdkBindingData<?> sdkBindingData, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeObject(sdkBindingData.get());
  }
}
