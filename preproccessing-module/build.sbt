ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "preprocessing-module",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql"  % "3.5.0",
      "org.apache.spark" %% "spark-mllib" % "3.5.0"
    )
  )

import sbtassembly.AssemblyPlugin.autoImport._

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("module-info.class") => MergeStrategy.discard
  case "arrow-git.properties" => MergeStrategy.first
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" =>
    MergeStrategy.concat
  case PathList("google", "protobuf", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", "logging", xs @ _*) => MergeStrategy.first
  case _ => MergeStrategy.first
}
