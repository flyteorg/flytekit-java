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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import org.flyte.flytekit.SdkBindingData;

class SdkTypeModule extends Module {
  // For now, we don't make module public, however, one day
  // when we are stable, we can open-up and allow customizations
  // then we can add factory method to JacksonSdkType to specify
  // custom ObjectMapper.

  @Override
  public String getModuleName() {
    return "SdkType";
  }

  @Override
  public Version version() {
    return Version.unknownVersion();
  }

  @Override
  public void setupModule(SetupContext context) {
    SimpleSerializers serializers = new SimpleSerializers();
    serializers.addSerializer(new LiteralSerializer());
    serializers.addSerializer(new SdkBindingDataSerializer());

    SimpleDeserializers deserializers = new SimpleDeserializers();
    deserializers.addDeserializer(SdkBindingData.class, new SdkBindingDataDeserializer());

    context.addSerializers(serializers);
    context.addDeserializers(new LiteralMapDeserializers());
    context.addDeserializers(deserializers);

    // append with lowest priority to use as fallback, if builtin annotations aren't present
    context.appendAnnotationIntrospector(new AutoValueAnnotationIntrospector());
  }
}
