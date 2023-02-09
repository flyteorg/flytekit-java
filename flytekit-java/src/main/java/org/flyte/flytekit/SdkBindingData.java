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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.flyte.flytekit.SdkLiteralTypes.collections;
import static org.flyte.flytekit.SdkLiteralTypes.maps;

import com.google.auto.value.AutoValue;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.OutputReference;

/** Specifies either a simple value or a reference to another output. */
public abstract class SdkBindingData<T> {

  abstract BindingData idl();

  public abstract SdkLiteralType<T> type();


  /**
   * Returns the simple value contained by this data.
   *
   * @return the value that this simple data holds
   * @throws IllegalArgumentException when this data is an output reference
   */
  public abstract T get();

  public abstract <NewT> SdkBindingData<NewT> as(
      SdkLiteralType<NewT> newType, Function<T, NewT> castFunction);

  // TODO: it would be interesting to see if we can use java 9 modules to only expose this method
  //      to other modules in the sdk
  /**
   * Creates a {@code SdkBindingData} for a literal value.
   *
   * @param type the {@link SdkLiteralType} type
   * @param value contains the simple value of this class
   * @return A newly created SdkBindingData
   * @param <T> the java or scala type for the corresponding LiteralType, for example {@code
   *     Duration} for {@code LiteralType.ofSimpleType(SimpleType.DURATION)}
   */
  public static <T> SdkBindingData<T> literal(SdkLiteralType<T> type, T value) {
    return LiteralSdkBindingData.create(type, value);
  }

  /**
   * Creates a {@code SdkBindingData} for a reference to (promise for) another output.
   *
   * @param type the {@link SdkLiteralType} type
   * @param nodeId which nodeId to reference
   * @param var variable name to reference on the node id
   * @return A newly created SdkBindingData
   * @param <T> the java or scala type for the corresponding LiteralType, for example {@code
   *     Duration} for {@code LiteralType.ofSimpleType(SimpleType.DURATION)}
   */
  public static <T> SdkBindingData<T> promise(SdkLiteralType<T> type, String nodeId, String var) {
    return PromiseSdkBindingData.create(type, nodeId, var);
  }

  /**
   * Creates a {@code SdkBindingData} for a collections of {@link SdkBindingData}.
   *
   * @param elementType the {@link SdkLiteralType} of the elements of the collection.
   * @param collection collections of {@link SdkBindingData}s
   * @return A newly created SdkBindingData
   * @param <T> the java or scala type for the corresponding LiteralType, for example {@code
   *     Duration} for {@code LiteralType.ofSimpleType(SimpleType.DURATION)}
   */
  public static <T> SdkBindingData<List<T>> bindingCollection(
      SdkLiteralType<T> elementType, List<SdkBindingData<T>> collection) {
    return SdkBindingCollection.create(elementType, collection);
  }

  /**
   * Creates a {@code SdkBindingData} for a map of {@link SdkBindingData}.
   *
   * @param valuesType the {@link SdkLiteralType} of the elements of the collection.
   * @param map map of {@link SdkBindingData}s
   * @return A newly created SdkBindingData
   * @param <T> the java or scala type for the corresponding LiteralType, for example {@code
   *     Duration} for {@code LiteralType.ofSimpleType(SimpleType.DURATION)}
   */
  public static <T> SdkBindingData<Map<String, T>> bindingMap(
      SdkLiteralType<T> valuesType, Map<String, SdkBindingData<T>> map) {
    return SdkBindingMap.create(valuesType, map);
  }

  @AutoValue
  abstract static class LiteralSdkBindingData<T> extends SdkBindingData<T> {
    abstract T value();

    private static <T> LiteralSdkBindingData<T> create(SdkLiteralType<T> type, T value) {
      return new AutoValue_SdkBindingData_LiteralSdkBindingData<>(type, value);
    }

    @Override
    BindingData idl() {
      return type().toBindingData(value());
    }

    @Override
    public T get() {
      return value();
    }

    @Override
    public <NewtT> SdkBindingData<NewtT> as(
        SdkLiteralType<NewtT> newType, Function<T, NewtT> castFunction) {
      return create(newType, castFunction.apply(value()));
    }

    @Override
    public final String toString() {
      return String.format("SdkBindingData{type=%s, value=%s}", type(), value());
    }
  }

  @AutoValue
  abstract static class PromiseSdkBindingData<T> extends SdkBindingData<T> {
    abstract String nodeId();

    abstract String var();

    private static <T> PromiseSdkBindingData<T> create(
        SdkLiteralType<T> type, String nodeId, String var) {
      return new AutoValue_SdkBindingData_PromiseSdkBindingData<>(type, nodeId, var);
    }

    @Override
    BindingData idl() {
      return BindingData.ofOutputReference(
          OutputReference.builder().nodeId(nodeId()).var(var()).build());
    }

    @Override
    public T get() {
      throw new IllegalArgumentException(
          String.format(
              "Value only available at workflow execution time: promise of %s[%s]",
              nodeId(), var()));
    }

    @Override
    public <NewT> SdkBindingData<NewT> as(
        SdkLiteralType<NewT> newType, Function<T, NewT> castFunction) {
      return create(newType, nodeId(), var());
    }

    @Override
    public final String toString() {
      return String.format("SdkBindingData{type=%s, nodeIs=%s, var=%s}", type(), nodeId(), var());
    }
  }

  @AutoValue
  abstract static class SdkBindingCollection<T> extends SdkBindingData<List<T>> {
    abstract List<SdkBindingData<T>> bindingCollection();

    private static <T> SdkBindingCollection<T> create(
        SdkLiteralType<T> elementType, List<SdkBindingData<T>> bindingCollection) {
      checkIncompatibleTypes(elementType, bindingCollection);
      return new AutoValue_SdkBindingData_SdkBindingCollection<>(
          collections(elementType), bindingCollection);
    }

    @Override
    BindingData idl() {
      return BindingData.ofCollection(
          bindingCollection().stream().map(SdkBindingData::idl).collect(toUnmodifiableList()));
    }

    @Override
    public List<T> get() {
      return bindingCollection().stream().map(SdkBindingData::get).collect(toUnmodifiableList());
    }

    @Override
    public <NewT> SdkBindingData<NewT> as(
        SdkLiteralType<NewT> newType, Function<List<T>, NewT> castFunction) {
      // TODO: mmm, looks like the as method is not as flexible
      //  I would like a function to apply to each element, but
      throw new RuntimeException("Not yet implemented");
    }

    @Override
    public final String toString() {
      return String.format("SdkBindingData{type=%s, collection=%s}", type(), bindingCollection());
    }
  }

  @AutoValue
  abstract static class SdkBindingMap<T> extends SdkBindingData<Map<String, T>> {
    abstract Map<String, SdkBindingData<T>> bindingMap();

    private static <T> SdkBindingMap<T> create(
        SdkLiteralType<T> valuesType, Map<String, SdkBindingData<T>> bindingMap) {
      checkIncompatibleTypes(valuesType, bindingMap.values());
      return new AutoValue_SdkBindingData_SdkBindingMap<>(maps(valuesType), bindingMap);
    }

    @Override
    BindingData idl() {
      return BindingData.ofMap(
          bindingMap().entrySet().stream()
              .collect(toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().idl())));
    }

    @Override
    public Map<String, T> get() {
      return bindingMap().entrySet().stream()
          .collect(toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().get()));
    }

    @Override
    public <NewT> SdkBindingData<NewT> as(
        SdkLiteralType<NewT> newType, Function<Map<String, T>, NewT> castFunction) {
      // TODO: mmm, looks like the as method is not as flexible
      //  I would like a function to apply to each element, but
      throw new RuntimeException("Not yet implemented");
    }

    @Override
    public final String toString() {
      return String.format("SdkBindingData{type=%s, map=%s}", type(), bindingMap());
    }
  }

  private static <T> void checkIncompatibleTypes(
      SdkLiteralType<T> elementType, Collection<SdkBindingData<T>> elements) {
    List<LiteralType> incompatibleTypes =
        elements.stream()
            .map(SdkBindingData::type)
            .filter(type -> !type.equals(elementType))
            .map(SdkLiteralType::getLiteralType)
            .distinct()
            .collect(toList());
    if (!incompatibleTypes.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Type mismatch: expected all elements of type %s but found some elements of type: %s",
              elementType.getLiteralType(), incompatibleTypes));
    }
  }
}
