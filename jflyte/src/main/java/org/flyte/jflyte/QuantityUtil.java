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
package org.flyte.jflyte;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QuantityUtil {
  private static final String DIGITS = "(\\d+(\\.(\\d+)?)?|\\.(\\d+))";
  private static final String POSITIVE_NUMBER = "(?<number>([+])?" + DIGITS + ")";
  private static final String SIGNED_NUMBER = "([+-])?" + DIGITS;
  private static final String DECIMAL_EXPONENT = "(?<decimalexp>[eE]" + SIGNED_NUMBER + ")";
  private static final String BINARY_SI = "(?<binarysi>Ki|Mi|Gi|Ti|Pi|Ei)";
  private static final String DECIMAL_SI = "(?<decimalsi>m||k|M|G|T|P|E)";
  private static final String SUFFIX =
      "(?<suffix>" + BINARY_SI + "|" + DECIMAL_SI + "|" + DECIMAL_EXPONENT + ")";
  private static final Pattern QUANTITY_PATTERN =
      Pattern.compile("^" + POSITIVE_NUMBER + SUFFIX + "$");

  static boolean isValidQuantity(String value) {
    return QUANTITY_PATTERN.matcher(value).matches();
  }

  static String asJavaQuantity(String value) {
    Matcher matcher = QUANTITY_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Couldn't convert to Java quantity: " + value);
    }

    double number = Double.parseDouble(matcher.group("number"));
    String binarySi = matcher.group("binarysi");
    String decimalSi = matcher.group("decimalsi");
    String decimalExp = matcher.group("decimalexp");
    if (binarySi != null) {
      return javaQuantityFromBinarySi(number, binarySi);
    } else if (decimalSi != null) {
      return javaQuantityFromDecimalSi(number, decimalSi);
    } else { // if (decimalexp != null) {
      return javaQuantityFromDecimalExp(number, decimalExp);
    }
  }

  private static String javaQuantityFromBinarySi(double origNumber, String binarySiUnit) {
    return roundUp(origNumber) + binarySiUnit.substring(0, 1);
  }

  private static String javaQuantityFromDecimalSi(double origNumber, String decimalSiUnit) {
    // approximate decimal units to binary units to make it simple
    if (decimalSiUnit.equals("m")) {
      return String.valueOf(roundUp(origNumber / 1000));
    } else {
      return roundUp(origNumber) + decimalSiUnit;
    }
  }

  private static String javaQuantityFromDecimalExp(double origNumber, String decimalExp) {
    double exp = Double.parseDouble(decimalExp.replaceAll("[eE]", ""));
    return String.valueOf(roundUp(origNumber * Math.pow(10, exp)));
  }

  private static long roundUp(double origNumber) {
    return (long) Math.ceil(origNumber);
  }
}
