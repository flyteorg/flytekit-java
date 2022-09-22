package org.flyte;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.util.stream.Stream;
import org.flyte.utils.FlyteSandboxClient;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SerializeJavaIT {
  private static final FlyteSandboxClient CLIENT = FlyteSandboxClient.create();
  private static final String CLASSPATH = "flytekit-examples/target/lib";
  @TempDir File tempDir;

  @Test
  @Disabled
  public void testSerializeWorkflows() {
    try {
      CLIENT.serializeWorkflows(CLASSPATH, tempDir.getAbsolutePath());

      boolean hasFibonacciWorkflow =
          Stream.of(tempDir.list())
              .anyMatch(x -> x.endsWith("_org.flyte.examples.FibonacciWorkflow_2.pb"));

      assertThat(hasFibonacciWorkflow, equalTo(true));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
