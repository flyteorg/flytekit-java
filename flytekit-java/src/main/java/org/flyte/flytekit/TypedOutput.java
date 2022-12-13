package org.flyte.flytekit;

import java.util.Map;

public class TypedOutput {
  public Map<String, SdkBindingData> outputs;

  public TypedOutput(Map<String, SdkBindingData> outputs) {
    this.outputs = outputs;
  }
}
