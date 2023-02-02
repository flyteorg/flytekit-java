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
package org.flyte.api.v1;

import com.google.auto.value.AutoOneOf;
import java.util.List;
import java.util.Map;

/** Specifies either a simple value or a reference to another output. */
@AutoOneOf(BindingData.Kind.class)
public abstract class BindingData {
  public enum Kind {
    /** A simple scalar value. */
    SCALAR,

    /**
     * A collection of binding data. This allows nesting of binding data to any number of levels.
     */
    COLLECTION,

    /** References an output promised by another node. */
    PROMISE,

    /** A map of bindings. The key is always a string. */
    MAP
  }

  public abstract Kind kind();
  /** A simple scalar value. */
  public abstract Scalar scalar();
  /** A collection of binding data. This allows nesting of binding data to any number of levels. */
  public abstract List<BindingData> collection();
  /** References an output promised by another node. */
  public abstract OutputReference promise();
  /** A map of bindings. The key is always a string. */
  public abstract Map<String, BindingData> map();

  public static BindingData ofScalar(Scalar scalar) {
    return AutoOneOf_BindingData.scalar(scalar);
  }

  public static BindingData ofCollection(List<BindingData> collection) {
    return AutoOneOf_BindingData.collection(collection);
  }

  public static BindingData ofOutputReference(OutputReference outputReference) {
    return AutoOneOf_BindingData.promise(outputReference);
  }

  public static BindingData ofMap(Map<String, BindingData> map) {
    return AutoOneOf_BindingData.map(map);
  }
}
