import sbtassembly.AssemblyPlugin.autoImport._

name := "vasts3-test-job"
version := "0.1"
scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .enablePlugins(AssemblyPlugin)
  .settings(
    scalaVersion := "2.12.18",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.1" % "provided",
      "com.typesafe" % "config" % "1.4.3"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    // Do not set a single mainClass here
    Compile / assembly / mainClass := None
  )