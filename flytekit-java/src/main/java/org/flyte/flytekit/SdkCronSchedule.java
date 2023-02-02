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
package org.flyte.flytekit;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/** Create a Cron Schedule with optional offset that can be used to schedule a Launch Plan. */
@AutoValue
public abstract class SdkCronSchedule {
  private static final List<String> CRON_ALIAS =
      Collections.unmodifiableList(
          Arrays.asList(
              "hours",
              "days",
              "weeks",
              "months",
              "years",
              "hourly",
              "daily",
              "weekly",
              "monthly",
              "yearly",
              "annually",
              "@hourly",
              "@daily",
              "@weekly",
              "@monthly",
              "@yearly",
              "@annually"));

  private static final String REGEX_CRON_SCHEDULE =
      "(^(\\*|([0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])|"
          + "\\*\\/([0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])) "
          + "(\\*|([0-9]|1[0-9]|2[0-3])|\\*\\/([0-9]|1[0-9]|2[0-3])) "
          + "(\\*|([1-9]|1[0-9]|2[0-9]|3[0-1])|\\*\\/([1-9]|1[0-9]|2[0-9]|3[0-1])) "
          + "(\\*|([1-9]|1[0-2])|\\*\\/([1-9]|1[0-2])) (\\*|([0-6])|\\*\\/([0-6])))";

  /** Returns the schedule. */
  public abstract String schedule();

  /** Returns the offset duration. It could be null */
  @Nullable
  public abstract Duration offset();

  /**
   * Creates a {@link SdkCronSchedule} with input schedule and no offset.
   *
   * @param schedule A cron alias (days, months, etc.) or a cron expression '* * * * *'
   * @return the newly created {@link SdkCronSchedule}.
   */
  public static SdkCronSchedule of(String schedule) {
    return new AutoValue_SdkCronSchedule(schedule, null);
  }

  /**
   * Creates a {@link SdkCronSchedule} with input schedule and offset.
   *
   * @param schedule A cron alias (days, months, etc.) or a cron expression '* * * * *'
   * @param offset offset duration
   * @return the newly created {@link SdkCronSchedule}.
   */
  public static SdkCronSchedule of(String schedule, Duration offset) {
    if (!CRON_ALIAS.contains(schedule) && !schedule.matches(REGEX_CRON_SCHEDULE)) {
      throw new IllegalArgumentException(
          String.format("Schedule [%s] does not match a known pattern", schedule));
    }
    return new AutoValue_SdkCronSchedule(schedule, offset);
  }

  /** Returns a {@link SdkCronSchedule} with hourly alias and no offset. */
  public static SdkCronSchedule hourly() {
    return new AutoValue_SdkCronSchedule("hourly", null);
  }

  /** Returns a {@link SdkCronSchedule} with daily alias and no offset. */
  public static SdkCronSchedule daily() {
    return new AutoValue_SdkCronSchedule("daily", null);
  }

  /** Returns a {@link SdkCronSchedule} with weekly alias and no offset. */
  public static SdkCronSchedule weekly() {
    return new AutoValue_SdkCronSchedule("weekly", null);
  }

  /** Returns a {@link SdkCronSchedule} with monthly alias and no offset. */
  public static SdkCronSchedule monthly() {
    return new AutoValue_SdkCronSchedule("monthly", null);
  }

  /** Returns a {@link SdkCronSchedule} with yearly alias and no offset. */
  public static SdkCronSchedule yearly() {
    return new AutoValue_SdkCronSchedule("yearly", null);
  }
}
