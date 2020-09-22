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
package org.flyte.jflyte;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import picocli.CommandLine;

class LiteralTypeConverter implements CommandLine.ITypeConverter<Literal> {

  private final SimpleType simpleType;

  private static final DateTimeFormatter ISO_DATE_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .optionalStart()
          .appendOffsetId()
          .optionalEnd()
          .toFormatter();

  private static final DateTimeFormatter ISO_DATE_FORMAT =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_DATE)
          .optionalStart()
          .appendOffsetId()
          .optionalEnd()
          .toFormatter();

  LiteralTypeConverter(SimpleType simpleType) {
    this.simpleType = simpleType;
  }

  @Override
  public Literal convert(String value) {
    switch (simpleType) {
      case FLOAT:
        try {
          return literalOfPrimitive(Primitive.ofFloatValue(Float.parseFloat(value)));
        } catch (RuntimeException e) {
          throw fail(value, Float.TYPE);
        }

      case INTEGER:
        try {
          return literalOfPrimitive(Primitive.ofIntegerValue(Long.parseLong(value)));
        } catch (RuntimeException e) {
          throw fail(value, Long.TYPE);
        }

      case DATETIME:
        try {
          return literalOfPrimitive(Primitive.ofDatetime(parseInstant(value)));
        } catch (RuntimeException e) {
          throw fail(value, LocalDateTime.class);
        }

      case DURATION:
        try {
          return literalOfPrimitive(Primitive.ofDuration(Duration.parse(value)));
        } catch (RuntimeException e) {
          throw fail(value, Duration.class);
        }

      case STRING:
        return literalOfPrimitive(Primitive.ofStringValue(value));

      case BOOLEAN:
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
          return literalOfPrimitive(Primitive.ofBooleanValue(Boolean.parseBoolean(value)));
        } else {
          throw fail(value, Boolean.TYPE);
        }

      case STRUCT:
        throw new IllegalArgumentException(
            "Struct types aren't supported for command line parameters");
    }

    throw new AssertionError("Unexpected SimpleType: " + simpleType);
  }

  private static Instant parseInstant(String value) {
    try {
      TemporalAccessor accessor =
          ISO_DATE_TIME_FORMAT.parseBest(value, ZonedDateTime::from, LocalDateTime::from);

      if (accessor instanceof ZonedDateTime) {
        return ((ZonedDateTime) accessor).toInstant();
      }

      return ((LocalDateTime) accessor).toInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException e) {
      TemporalAccessor accessor = ISO_DATE_FORMAT.parse(value);

      ZoneOffset offset =
          accessor.isSupported(ChronoField.OFFSET_SECONDS)
              ? accessor.query(ZoneOffset::from)
              : ZoneOffset.UTC;

      return accessor.query(LocalDate::from).atStartOfDay(offset).toInstant();
    }
  }

  private static Literal literalOfPrimitive(Primitive primitive) {
    return Literal.ofScalar(Scalar.ofPrimitive(primitive));
  }

  // approach taken from picocli to be consistent with their converters
  private static CommandLine.TypeConversionException fail(String value, Class<?> c) {
    String template = "'%s' is not a %s";

    return new CommandLine.TypeConversionException(
        String.format(template, value, c.getSimpleName()));
  }
}
