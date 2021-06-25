package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.jackson.JacksonSdkType;

/**
 * Example task that takes a greeting message as input, appends "How are you?", and outputs the result
 */
@AutoService(SdkRunnableTask.class)
public class AddQuestionTask extends SdkRunnableTask<AddQuestionTask.Input, AddQuestionTask.Output> {
  public AddQuestionTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  public static SdkTransform of(SdkBindingData greeting) {
    return new AddQuestionTask().withInput("greeting", greeting);
  }

  @AutoValue
  public abstract static class Input {
    public abstract String greeting();
  }

  @AutoValue
  public abstract static class Output {
    public abstract String greeting();

    public static Output create(String greeting) {
      return new AutoValue_AddQuestionTask_Output(greeting);
    }
  }

  @Override
  public Output run(Input input) {
    return Output.create(String.format("%s How are you?", input.greeting()));
  }
}
