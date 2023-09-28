/*
 * Copyright 2020-2021 Flyte Authors
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
package org.flyte.jflyte.aws;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.flyte.jflyte.api.FileSystem;
import org.flyte.jflyte.api.FileSystemRegistrar;

@AutoService(FileSystemRegistrar.class)
public class S3FileSystemRegistrar extends FileSystemRegistrar {

  @Override
  public Iterable<FileSystem> load(Map<String, String> env) {
    return Collections.singletonList(S3FileSystem.create(env));
  }
}
