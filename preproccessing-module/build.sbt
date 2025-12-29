ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val sparkV = "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "preprocessing-module",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"  % sparkV % "provided",
      "org.apache.spark" %% "spark-sql"   % sparkV % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkV % "provided",

      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
      "org.apache.spark" %% "spark-token-provider-kafka-0-10" % sparkV,

      "org.apache.hbase" % "hbase-shaded-client" % "2.5.8"
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
