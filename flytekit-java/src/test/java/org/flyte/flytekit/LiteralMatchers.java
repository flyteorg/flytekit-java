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
package org.flyte.flytekit;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class LiteralMatchers {
  static Matcher<Literal> matchesLiteralInteger(Matcher<Long> matcher) {
    return matchesLiteralPrimitive(
        new InternalMatcher<>(Primitive::type, SimpleType.INTEGER, Primitive::integer, matcher));
  }

  static Matcher<Literal> matchesLiteralFloat(Matcher<Double> matcher) {
    return matchesLiteralPrimitive(
        new InternalMatcher<>(Primitive::type, SimpleType.FLOAT, Primitive::float_, matcher));
  }

  static Matcher<Literal> matchesLiteralString(Matcher<String> matcher) {
    return matchesLiteralPrimitive(
        new InternalMatcher<>(Primitive::type, SimpleType.STRING, Primitive::string, matcher));
  }

  static Matcher<Literal> matchesLiteralBoolean(Matcher<Boolean> matcher) {
    return matchesLiteralPrimitive(
        new InternalMatcher<>(Primitive::type, SimpleType.BOOLEAN, Primitive::boolean_, matcher));
  }

  static Matcher<Literal> matchesLiteralDatetime(Matcher<Instant> matcher) {
    return matchesLiteralPrimitive(
        new InternalMatcher<>(Primitive::type, SimpleType.DATETIME, Primitive::datetime, matcher));
  }

  static Matcher<Literal> matchesLiteralDuration(Matcher<Duration> matcher) {
    return matchesLiteralPrimitive(
        new InternalMatcher<>(Primitive::type, SimpleType.DURATION, Primitive::duration, matcher));
  }

  private static Matcher<Literal> matchesLiteralPrimitive(Matcher<Primitive> primitiveMatcher) {
    return new InternalMatcher<>(
        Literal::kind,
        Literal.Kind.SCALAR,
        Literal::scalar,
        new InternalMatcher<>(
            Scalar::kind, Scalar.Kind.PRIMITIVE, Scalar::primitive, primitiveMatcher));
  }

  private static class InternalMatcher<ElementT, ClassifierT extends Enum<ClassifierT>, ContentT>
      extends TypeSafeDiagnosingMatcher<ElementT> {

    private final Function<ElementT, ClassifierT> classificationFunction;
    private final ClassifierT classifier;
    private final Function<ElementT, ContentT> extractor;
    private final Matcher<? super ContentT> matcher;

    public InternalMatcher(
        Function<ElementT, ClassifierT> classificationFunction,
        ClassifierT classifierValue,
        Function<ElementT, ContentT> extractor,
        Matcher<? super ContentT> matcher) {
      this.classificationFunction = classificationFunction;
      this.classifier = classifierValue;
      this.extractor = extractor;
      this.matcher = matcher;
    }

    @Override
    protected boolean matchesSafely(ElementT item, Description mismatchDescription) {
      if (classificationFunction.apply(item) != classifier) {
        mismatchDescription.appendValue(item).appendText(" is not a " + classifier);
        return false;
      }
      return matcher.matches(extractor.apply(item));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(classifier + " matching ").appendDescriptionOf(matcher);
    }
  }
}
