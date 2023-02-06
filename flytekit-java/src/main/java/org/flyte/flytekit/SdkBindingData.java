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
package org.flyte.flytekit;

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.OutputReference;

/** Specifies either a simple value or a reference to another output. */
@AutoValue
public abstract class SdkBindingData<T> {

  abstract BindingData idl();

  abstract LiteralType type();

  @Nullable
  abstract T value();

  // TODO: it would be interesting to see if we can use java 9 modules to only expose this method
  //      to other modules in the sdk
  /**
   * Creates a {@code SdkBindingData} based on its components; however it is not meant to be used by
   * users directly, but users must use the higher level factory methods.
   *
   * @param idl the api class equivalent to this
   * @param type the SdkBindingData type
   * @param value when {@code idl} is not a {@link BindingData.Kind#PROMISE} then value contains the
   *     simple value of this class, must be null otherwise
   * @return A newly created SdkBindingData
   * @param <T> the java or scala type for the corresponding LiteralType, for example {@code
   *     Duration} for {@code LiteralType.ofSimpleType(SimpleType.DURATION)}
   */
  public static <T> SdkBindingData<T> create(BindingData idl, LiteralType type, @Nullable T value) {
    return new AutoValue_SdkBindingData<>(idl, type, value);
  }

  public static <T> SdkBindingData<List<T>> create(
      List<SdkBindingData<T>> bindingList, LiteralType type, @Nullable List<T> valueList) {
    if (valueList != null && bindingList.size() != valueList.size()) {
      throw new IllegalArgumentException("bindingList.size() != valueList.size()");
    }
    var idl = bindingList.stream().map(SdkBindingData::idl).collect(toUnmodifiableList());
    return create(BindingData.ofCollection(idl), type, valueList);
  }

  public static <T> SdkBindingData<Map<String, T>> create(
      Map<String, SdkBindingData<T>> bindingMap,
      LiteralType type,
      @Nullable Map<String, T> valueMap) {
    if (valueMap != null && bindingMap.size() != valueMap.size()) {
      throw new IllegalArgumentException("bindingMap.size() != valueMap.size()");
    }
    var idl =
        bindingMap.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().idl()))
            .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return create(BindingData.ofMap(idl), type, valueMap);
  }

  /**
   * Returns the simple value contained by this data.
   *
   * @return the value that this simple data holds
   * @throws IllegalArgumentException when this data is an output reference
   */
  public T get() {
    if (idl().kind() == BindingData.Kind.PROMISE) {
      OutputReference promise = idl().promise();
      throw new IllegalArgumentException(
          String.format(
              "Value only available at workflow execution time: promise of %s[%s]",
              promise.nodeId(), promise.var()));
    }

    return value();
  }
}
