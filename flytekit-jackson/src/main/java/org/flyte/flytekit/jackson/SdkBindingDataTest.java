package org.flyte.flytekit.jackson;

public class SdkBindingDataTest<T> {
  public T value;
  private String type;

  public SdkBindingDataTest(T value) {
    this.value = value;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
