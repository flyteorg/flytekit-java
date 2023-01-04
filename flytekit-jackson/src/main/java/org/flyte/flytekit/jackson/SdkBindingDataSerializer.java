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
package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Struct;
import org.flyte.flytekit.SdkBindingData;

class SdkBindingDataSerializer extends StdSerializer<SdkBindingData> {
  private static final long serialVersionUID = 0L;

  public SdkBindingDataSerializer() {
    super(SdkBindingData.class);
  }

  @Override
  public void serialize(SdkBindingData sdkBindingData, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeObject(sdkBindingData.get());
  }
}
