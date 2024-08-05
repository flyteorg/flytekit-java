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
package org.flyte.flytekitscala

import io.github.classgraph.{ClassGraph, ClassInfo, ClassInfoList, ScanResult}
import sbt.Keys._
import sbt._

import scala.collection.JavaConverters.*

object FlytekitScalaPlugin extends AutoPlugin {
  val autoImport = FlytekitJavaKeys
  import autoImport._

  private val MetaInfoServiceFileNames = Seq(
    "org.flyte.flytekit.SdkRunnableTask",
    "org.flyte.flytekit.SdkDynamicWorkflowTask",
    "org.flyte.flytekit.SdkPluginTask",
    "org.flyte.flytekit.SdkContainerTask",
    "org.flyte.flytekit.SdkWorkflow",
    "org.flyte.flytekit.SdkLaunchPlanRegistry"
  )

  override def trigger: PluginTrigger = noTrigger



  override lazy val projectSettings: Seq[Def.Setting[_]] = Seq(
    flyteVersion := "0.4.58",
    libraryDependencies ++=
      Seq(
        "org.flyte" % "flytekit-api" % flyteVersion.value,
        "org.flyte" %% "flytekit-scala" % flyteVersion.value,
        "org.flyte" % "flytekit-testing" % flyteVersion.value % Test
      ),
    // add flyte generated services after compilation as a jar resource
    // note that we first have to remove potentially duplicated META-INF/services
    // files to address a failure path like:
    //   $ sbt clean pack
    //   And then build project in IntelliJ (where generated files are copied to target/classes folder)
    //   $ sbt pack # this will result duplicated files and fail the build
    Compile / packageBin / mappings :=
      (Compile / packageBin / mappings).value
        .filterNot(v => MetaInfoServiceFileNames.contains(v._1.getName)) ++
        flyteGenerateServicesTask(Compile)
          .map(_.map(f => (f, s"META-INF/services/${f.getName}")))
          .value,
    // add flyte generated services after compilation as a test resource
    Test / resourceGenerators += flyteGenerateServicesTask(Test)
  )

  private def flyteGenerateServicesTask(configKey: ConfigKey) = Def.task {
    val log = (configKey / streams).value.log
    val classPath = (Runtime / fullClasspath).value.map(_.data.getAbsolutePath)
    val classGraph = new ClassGraph().overrideClasspath(classPath: _*)
    val result = classGraph.enableMethodInfo().scan()
    try {
      MetaInfoServiceFileNames
        .filter(fileName => result.getClassInfo(fileName) != null) // in case old version of flytekit-java
        .map { fileName =>
          val impls = getClassesImplementingOrExtending(result, fileName, log)
          impls.foreach(x => log.info(s"Discovered $fileName: $x"))
          val services = impls.mkString("\n")
          val file = (configKey / classDirectory).value / "META-INF" / "services" / fileName
          IO.write(file, services)
          file
        }
    } finally {
      result.close()
    }
  }

  private def getClassesImplementingOrExtending(
                                                 result: ScanResult,
                                                 className: String,
                                                 log: Logger
                                               ): List[String] = {
    val classesOrInterfaces =
      if (result.getClassInfo(className).isInterface) {
        result.getClassesImplementing(className)
      } else {
        result.getSubclasses(className)
      }

    warnAnonymousClasses(classesOrInterfaces, log)

    val subClasses =
      classesOrInterfaces
        .filter(x => !x.isAbstract && !x.isAnonymousInnerClass)

    failIfMissingDefaultConstructor(subClasses, log)

    val subInterfaces = classesOrInterfaces.getInterfaces.getNames.asScala.toList
    val subAbstractClasses = classesOrInterfaces.filter(_.isAbstract).getNames.asScala.toList

    val all = subClasses.getNames.asScala.toList ++
      subInterfaces.flatMap(getClassesImplementingOrExtending(result, _, log)) ++
      subAbstractClasses.flatMap(getClassesImplementingOrExtending(result, _, log))

    all.distinct
  }

  private def warnAnonymousClasses(
                                    classesOrInterfaces: ClassInfoList,
                                    log: Logger
                                  ): Unit = {
    classesOrInterfaces
      .filter(_.isAnonymousInnerClass)
      .forEach(
        x =>
          log.warn(
            s"Anonymous class ${x.getName} cannot be used to implement Flyte entities"
          )
      )
  }

  private def failIfMissingDefaultConstructor(classes: ClassInfoList, log: Logger): Unit = {
    val classesMissingDefaultConstructor = classes.filter(hasNoDefaultConstructor)

    if (!classesMissingDefaultConstructor.isEmpty) {
      classesMissingDefaultConstructor.forEach(
        x => log.error(s"Class ${x.getName} has no default constructor defined")
      )

      throw new MessageOnlyException(
        "One or more classes implementing Flyte entity have no default constructor defined"
      )
    }
  }

  private def hasNoDefaultConstructor(clazz: ClassInfo): Boolean =
    clazz.getDeclaredConstructorInfo.filter(_.getParameterInfo.isEmpty).isEmpty
}

object FlytekitJavaKeys {
  // don't override defaults for these settings unless you want to use unstable version
  lazy val flyteVersion = settingKey[String]("Flyte version")
}