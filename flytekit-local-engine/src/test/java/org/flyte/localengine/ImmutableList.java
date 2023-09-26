/*
 * Copyright 2020-2023 Flyte Authors.
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
package org.flyte.localengine;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.List;

public class ImmutableList {

  // NB only for tests, doesn't offer thread-safe publishing as in guava

  public static <T> List<T> of() {
    return emptyList();
  }

  public static <T> List<T> of(T v1) {
    return singletonList(v1);
  }

  public static <T> List<T> of(T v1, T v2) {
    List<T> list = new ArrayList<>();
    list.add(v1);
    list.add(v2);

    return unmodifiableList(list);
  }

  public static <T> List<T> of(T v1, T v2, T v3) {
    List<T> list = new ArrayList<>();
    list.add(v1);
    list.add(v2);
    list.add(v3);

    return unmodifiableList(list);
  }

  public static <T> List<T> of(T v1, T v2, T v3, T v4) {
    List<T> list = new ArrayList<>();
    list.add(v1);
    list.add(v2);
    list.add(v3);
    list.add(v4);

    return unmodifiableList(list);
  }

  public static <T> List<T> of(T v1, T v2, T v3, T v4, T v5) {
    List<T> list = new ArrayList<>();
    list.add(v1);
    list.add(v2);
    list.add(v3);
    list.add(v4);
    list.add(v5);

    return unmodifiableList(list);
  }

  public static <T> List<T> of(T v1, T v2, T v3, T v4, T v5, T v6) {
    List<T> list = new ArrayList<>();
    list.add(v1);
    list.add(v2);
    list.add(v3);
    list.add(v4);
    list.add(v5);
    list.add(v6);

    return unmodifiableList(list);
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {
    private Builder() {}

    private final List<T> items = new ArrayList<>();

    @CanIgnoreReturnValue
    public Builder<T> add(T item) {
      items.add(item);
      return this;
    }

    public List<T> build() {
      return unmodifiableList(items);
    }
  }
}
