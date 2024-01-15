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
test

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

## Build from source

It requires **Java 11 and Docker**

```bash
mvn clean verify

# Inspect dependency tree
mvn dependency:tree

# Inspect tooling dependency tree
mvn dependency:resolve-plugins

```

## How to run examples

You can build und run examples yourself. 

Create `.env.local` with:

```bash
FLYTE_PLATFORM_URL=localhost:30081
FLYTE_AWS_ENDPOINT=http://localhost:30084
FLYTE_AWS_ACCESS_KEY_ID=minio
FLYTE_AWS_SECRET_ACCESS_KEY=miniostorage
FLYTE_STAGING_LOCATION=s3://my-s3-bucket
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

**Note**: `scripts/jflyte` requires `jq` to run, in adition to `docker`

## Usage


### Maven

```
<dependency>
    <groupId>org.flyte</groupId>
    <artifactId>flytekit-java</artifactId>
    <version>0.3.15</version>
</dependency>
```

### SBT

Scala 2.12 and Scala 2.13 are supported.

```scala
libraryDependencies ++= Seq(
  "org.flyte" % "flytekit-java" % "0.3.15",
  "org.flyte" %% "flytekit-scala" % "0.3.15"
)
```

## Contributing 

Run `mvn spotless:apply` before committing. 

Also use `git commit --signoff "Commit message"` to comply with DCO. 

## Releasing

* Go to [Actions: Create flytekit-java release](https://github.com/flyteorg/flytekit-java/actions/workflows/release.yaml) and click "Run workflow"
* Wait until the workflow finishes; in the meanwhile prepare a release note
* Making sure the new release is visible in [Maven central](https://repo1.maven.org/maven2/org/flyte/flytekit-java/)
* Publish the release note associating with the latest tag created by the release workflow
