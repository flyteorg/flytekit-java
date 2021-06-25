package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.jackson.JacksonSdkType;

/** Example Flyte task that takes a name as the input and outputs a simple greeting message */
@AutoService(SdkRunnableTask.class)
public class GreetTask extends SdkRunnableTask<GreetTask.Input, GreetTask.Output> {
  public GreetTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  /**
   * Binds input data to this task
   *
   * @param name the input name
   * @return A transformed instance of this class with input data
   */
  public static SdkTransform of(SdkBindingData name) {
    return new GreetTask().withInput("name", name);
  }

  /**
   * Generate an immutable value class that represents {@link GreetTask}'s input, which is a String.
   */
  @AutoValue
  public abstract static class Input {
    public abstract String name();
  }

  /**
   * Generate an immutable value class that represents {@link GreetTask}'s output, which is a
   * String.
   */
  @AutoValue
  public abstract static class Output {
    public abstract String greeting();

    /**
     * Wraps the constructor of the generated output value class.
     *
     * @param greeting the String literal output of {@link GreetTask}
     * @return output of GreetTask
     */
    public static Output create(String greeting) {
      return new AutoValue_GreetTask_Output(greeting);
    }
  }

  /**
   * Defines task behavior. This task takes a name as the input, wraps it in a welcome message, and
   * outputs the message.
   *
   * @param input the name of the person to be greeted
   * @return the welcome message
   */
  @Override
  public Output run(Input input) {
    return Output.create(String.format("Welcome, %s!", input.name()));
  }
}
