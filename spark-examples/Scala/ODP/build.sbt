name := "HDFSOperations"
version := "0.1"
scalaVersion := "2.12.17" // Match your Spark version

Compile / run / mainClass := Some("Job1HDFSRead")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.4",
  "org.apache.spark" %% "spark-sql"  % "3.3.4"
)