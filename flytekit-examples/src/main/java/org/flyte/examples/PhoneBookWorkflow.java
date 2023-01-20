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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class PhoneBookWorkflow extends SdkWorkflow<PhoneBookWorkflow.Output> {

  private static final List<String> NAMES = Arrays.asList("frodo", "bilbo");
  private static final Map<String, String> PHONE_BOOK = new HashMap<>();

  static {
    PHONE_BOOK.put("frodo", "123");
    PHONE_BOOK.put("bilbo", "456");
    PHONE_BOOK.put("gandalf", "789");
  }

  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<List<String>> phoneNumbers();

    /**
     * Wraps the constructor of the generated output value class.
     *
     * @param phoneNumbers the String literal output of {@link NodeMetadataExampleWorkflow}
     * @return output of NodeMetadataExampleWorkflow
     */
    public static PhoneBookWorkflow.Output create(SdkBindingData<List<String>> phoneNumbers) {
      return new AutoValue_PhoneBookWorkflow_Output(phoneNumbers);
    }
  }

  public PhoneBookWorkflow() {
    super(JacksonSdkType.of(PhoneBookWorkflow.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Map<String, String>> phoneBook = SdkBindingData.ofStringMap(PHONE_BOOK);

    SdkBindingData<List<String>> searchKeys = SdkBindingData.ofStringCollection(NAMES);

    SdkBindingData<List<String>> phoneNumbers =
        builder
            .apply(
                "search",
                new BatchLookUpTask()
                    .withInput("keyValues", phoneBook)
                    .withInput("searchKeys", searchKeys))
            .getOutputs()
            .values();

    builder.output("phoneNumbers", phoneNumbers);
  }
}
