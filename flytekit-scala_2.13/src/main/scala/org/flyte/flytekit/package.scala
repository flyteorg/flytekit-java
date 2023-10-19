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
package org.flyte

/** Contains subclasses for [[SdkBindingData]]. We are forced to define this
  * package here because [[SdkBindingData#idl()]] is package private (we don´t
  * want to expose it to users). Making it protected doesn't help either because
  * list or map needs to call this method of elements so that requires it to be
  * public.
  *
  * This is not ideal because we are splitting the flytekit package in two maven
  * modules. This would create problems when we decide to add java 9 style
  * modules.
  */
package object flytekit {}
