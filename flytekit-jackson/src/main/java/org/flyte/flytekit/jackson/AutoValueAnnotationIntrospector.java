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
package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

class AutoValueAnnotationIntrospector extends NopAnnotationIntrospector {
  private static final long serialVersionUID = 1L;

  @Override
  public Version version() {
    return Version.unknownVersion();
  }

  // we can't check for AutoValue annotation because it gets erased in runtime
  // our best guess without checking classes is to confirm that class is abstract
  // and then try to find generated class

  @Override
  public JavaType refineDeserializationType(MapperConfig<?> config, Annotated a, JavaType baseType)
      throws JsonMappingException {
    Class<?> cls = baseType.getRawClass();

    if (!Modifier.isAbstract(cls.getModifiers())) {
      return super.refineDeserializationType(config, a, baseType);
    }

    Class<?> generatedClass = getGeneratedClass(cls);

    if (generatedClass != null) {
      return config.constructType(generatedClass);
    }

    return super.refineDeserializationType(config, a, baseType);
  }

  @Override
  public JsonCreator.Mode findCreatorAnnotation(MapperConfig<?> config, Annotated a) {
    Class<?> cls = a.getRawType();

    // disable single value constructor delegation for auto-value generated classes
    // see https://github.com/FasterXML/jackson-module-parameter-names/issues/21
    if (cls.getSimpleName().startsWith("AutoValue_")) {
      return JsonCreator.Mode.PROPERTIES;
    }

    return super.findCreatorAnnotation(config, a);
  }

  @Override
  public PropertyName findNameForSerialization(Annotated a) {
    if (!(a instanceof AnnotatedMethod)) {
      return super.findNameForSerialization(a);
    }

    AnnotatedMethod am = (AnnotatedMethod) a;
    Class<?> cls = am.getDeclaringClass();
    String name = am.getName();

    if (cls.getSimpleName().startsWith("AutoValue_")) {
      Class<?> baseClass = cls.getSuperclass();
      Method baseClassMethod;

      try {
        baseClassMethod = baseClass.getDeclaredMethod(name);
      } catch (NoSuchMethodException e) {
        return super.findNameForSerialization(a);
      }

      if (!isAutoValueGetter(baseClassMethod)) {
        return super.findNameForSerialization(a);
      }
    } else {
      if (!isAutoValueGetter(am.getMember())) {
        return super.findNameForSerialization(a);
      }
    }

    return PropertyName.USE_DEFAULT;
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> getGeneratedClass(Class<T> clazz) {
    String generatedClassName = getAutoValueGeneratedName(clazz.getName());

    Class<?> generatedClass;
    try {
      generatedClass = Class.forName(generatedClassName, true, clazz.getClassLoader());
    } catch (ClassNotFoundException e) {
      // the only way to check if class doesn't exist is to catch an exception
      return null;
    }

    if (!clazz.isAssignableFrom(generatedClass)) {
      throw new IllegalArgumentException(
          String.format("Generated class [%s] is not assignable to [%s]", generatedClass, clazz));
    }

    return (Class<T>) generatedClass;
  }

  private static boolean isAutoValueGetter(Method method) {
    Class<?> cls = method.getDeclaringClass();

    if (method.getParameterCount() != 0) {
      return false;
    }

    if (!Modifier.isAbstract(method.getModifiers())) {
      return false;
    }

    return getGeneratedClass(cls) != null;
  }

  private static String getAutoValueGeneratedName(String baseClass) {
    int lastDot = baseClass.lastIndexOf('.');

    if (lastDot != -1) {
      String packageName = baseClass.substring(0, lastDot);
      String baseName = baseClass.substring(lastDot + 1).replace('$', '_');

      return packageName + ".AutoValue_" + baseName;
    } else {
      return "AutoValue_" + baseClass;
    }
  }
}
