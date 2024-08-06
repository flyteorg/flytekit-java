import org.flyte.flytekitscala.FlytekitScalaPlugin

ThisBuild / version := "0.4.60-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "flytekit-examples-scala_2.13",
  ).enablePlugins(FlytekitScalaPlugin)
