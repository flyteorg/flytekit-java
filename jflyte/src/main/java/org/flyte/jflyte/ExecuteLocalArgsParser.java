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

import static org.flyte.jflyte.MoreCollectors.toUnmodifiableMap;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;
import picocli.CommandLine;

/** Parser for command line arguments for execute-local. */
public class ExecuteLocalArgsParser {

  public Map<String, Literal> parseInputs(
      String synopsis, Map<String, Variable> variableMap, List<String> inputArgs) {
    CommandLine.Model.CommandSpec spec = CommandLine.Model.CommandSpec.create();
    spec.usageMessage().customSynopsis(synopsis);

    variableMap.forEach((name, variable) -> spec.addOption(getOption(name, variable)));

    CommandLine.ParseResult result =
        new CommandLine(spec).parseArgs(inputArgs.toArray(new String[0]));

    return variableMap.entrySet().stream()
        .map(
            kv -> {
              String name = kv.getKey();
              Literal defaultValue = getDefaultValueAsLiteral(name, kv.getValue());

              return Maps.immutableEntry(name, result.matchedOptionValue(name, defaultValue));
            })
        .collect(toUnmodifiableMap());
  }

  /**
   * Override to provide default values if necessary.
   *
   * @param name option name
   * @param variable variable
   * @return option spec
   */
  protected CommandLine.Model.OptionSpec getOption(String name, Variable variable) {
    String defaultValue = getDefaultValue(name);

    CommandLine.Model.OptionSpec.Builder builder =
        CommandLine.Model.OptionSpec.builder("--" + name)
            .converters(getLiteralTypeConverter(name, variable))
            .required(defaultValue == null);

    if (defaultValue != null) {
      builder.defaultValue(defaultValue);
    }

    if (variable.description() != null) {
      builder.description(variable.description());
    }

    return builder.build();
  }

  private LiteralTypeConverter getLiteralTypeConverter(String name, Variable variable) {
    LiteralType literalType = variable.literalType();
    switch (literalType.getKind()) {
      case SIMPLE_TYPE:
        return new LiteralTypeConverter(literalType.simpleType());

      case SCHEMA_TYPE:
      case COLLECTION_TYPE:
      case MAP_VALUE_TYPE:
      case BLOB_TYPE:
        // TODO: implements other types
        throw new IllegalArgumentException(
            String.format("Type of [%s] input parameter is not supported: %s", name, literalType));
    }
    throw new AssertionError("Unexpected LiteralType.Kind: " + literalType.getKind());
  }

  /**
   * Override to provide default values if necessary.
   *
   * @param name parameter name
   * @return literal
   */
  @Nullable
  protected String getDefaultValue(String name) {
    return null;
  }

  @Nullable
  private Literal getDefaultValueAsLiteral(String name, Variable variable) {
    String defaultValueString = getDefaultValue(name);
    if (defaultValueString == null) {
      return null;
    }

    return getLiteralTypeConverter(name, variable).convert(defaultValueString);
  }
}
