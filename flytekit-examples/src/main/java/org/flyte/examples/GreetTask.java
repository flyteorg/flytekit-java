package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.jackson.JacksonSdkType;

/**
 * Example task that takes a name and outputs a simple greeting
 */
@AutoService(SdkRunnableTask.class)
public class GreetTask extends SdkRunnableTask<GreetTask.Input, GreetTask.Output> {
  public GreetTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  public static SdkTransform of(SdkBindingData name) {
    return new GreetTask().withInput("name", name);
  }

  @AutoValue
  public abstract static class Input {
    public abstract String name();
  }

  @AutoValue
  public abstract static class Output {
    public abstract String greeting();

    public static Output create(String greeting) {
      return new AutoValue_GreetTask_Output(greeting);
    }
  }

  @Override
  public Output run(Input input) {
    return Output.create(String.format("Welcome, %s!", input.name()));
  }
}
