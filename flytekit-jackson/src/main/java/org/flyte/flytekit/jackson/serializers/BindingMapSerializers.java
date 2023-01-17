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
package org.flyte.flytekit.jackson.serializers;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.Serializers;
import org.flyte.flytekit.jackson.JacksonBindingMap;

public class BindingMapSerializers extends Serializers.Base {

  @Override
  public JsonSerializer<?> findSerializer(
      SerializationConfig config, JavaType type, BeanDescription beanDesc) {
    if (type.getRawClass().equals(JacksonBindingMap.class)) {
      return new BindingMapSerializer();
    }

    return super.findSerializer(config, type, beanDesc);
  }
}
