/*
 * Copyright 2020-2023 Flyte Authors.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SdkCronScheduleTest {

  @Test
  public void testFactoryMethods() {
    assertThat(SdkCronSchedule.hourly(), equalTo(SdkCronSchedule.of("hourly", null)));
    assertThat(SdkCronSchedule.daily(), equalTo(SdkCronSchedule.of("daily", null)));
    assertThat(SdkCronSchedule.weekly(), equalTo(SdkCronSchedule.of("weekly", null)));
    assertThat(SdkCronSchedule.monthly(), equalTo(SdkCronSchedule.of("monthly", null)));
    assertThat(SdkCronSchedule.yearly(), equalTo(SdkCronSchedule.of("yearly", null)));
  }

  @Test
  public void testCronExpression() {

    SdkCronSchedule cronScheduleExpression = SdkCronSchedule.of("* * * * *", Duration.ofHours(1));
    assertThat(cronScheduleExpression, notNullValue());
    assertThat(cronScheduleExpression.schedule(), equalTo("* * * * *"));
    assertThat(cronScheduleExpression.offset(), hasToString("PT1H"));

    SdkCronSchedule cronScheduleAlias = SdkCronSchedule.of("hourly", Duration.ofHours(6));
    assertThat(cronScheduleAlias, notNullValue());
    assertThat(cronScheduleAlias.schedule(), equalTo("hourly"));
    assertThat(cronScheduleAlias.offset(), hasToString("PT6H"));
  }

  @Test
  public void testNonValidCronExpression() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SdkCronSchedule.of("non valid cron", Duration.ofHours(1)));

    assertThrows(
        IllegalArgumentException.class,
        () -> SdkCronSchedule.of("*/60 * * * *", Duration.ofHours(1)));
  }
}
