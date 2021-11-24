<!--
  Copyright 2021 Flyte Authors.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# flytekit-java

[![Lifecycle](https://img.shields.io/badge/lifecycle-alpha-a0c3d2.svg)](https://img.shields.io/badge/lifecycle-alpha-a0c3d2.svg)

Java/Scala library for easily authoring Flyte tasks and workflows.

Current development status:
- MVP features are developed
- Missing user documentation
- Project being tested, and collecting feedback
- No guarantees of API stability

To learn more about Flyte refer to:

 - [Flyte homepage](https://flyte.org)
 - [Flyte master repository](https://github.com/lyft/flyte)

## How to run examples

We don't publish artifacts yet, but you can build examples yourself. 

Create `.env.local` with:

```bash
FLYTE_PLATFORM_URL=flyte.local:81
FLYTE_STAGING_LOCATION=gs://yourbucket
FLYTE_PLATFORM_INSECURE=True
```

Package and run:

```bash
$ mvn package
$ scripts/jflyte register workflows \
  -d=development \
  -p=flytesnacks \
  -v=$(git describe --always) \
  -cp=flytekit-examples/target/lib
```
